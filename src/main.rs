use axum::{
    extract::{
        Query,
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use qrcode::render::svg;
use qrcode::QrCode;
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::mpsc;

const INDEX_HTML: &str = include_str!("../static/index.html");
const STYLE_CSS: &str = include_str!("../static/style.css");
const APP_JS: &str = include_str!("../static/app.js");
const MAX_QR_BYTES: usize = 1500;

type PeerId = u64;
type PeerTx = mpsc::UnboundedSender<Message>;

#[derive(Clone)]
struct AppState {
    rooms: Arc<tokio::sync::Mutex<HashMap<String, Room>>>,
    next_id: Arc<AtomicU64>,
}

struct Room {
    last_text: String,
    peers: HashMap<PeerId, Peer>,
}

struct Peer {
    _role: Role,
    tx: PeerTx,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    #[allow(dead_code)]
    Host,
    Writer,
}

#[derive(Deserialize)]
struct PageQuery {
    #[serde(default)]
    role: String,
    #[serde(default)]
    room: String,
}

#[derive(Deserialize)]
struct WsQuery {
    room: String,
    #[serde(default)]
    role: String,
}

#[derive(Deserialize)]
struct QrQuery {
    data: String,
}

#[derive(serde::Serialize)]
struct OutMsg {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n: Option<usize>,
}

#[tokio::main]
async fn main() {
    let state = AppState {
        rooms: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        next_id: Arc::new(AtomicU64::new(1)),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/style.css", get(style_css))
        .route("/app.js", get(app_js))
        .route("/qr.svg", get(qr_svg))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8787);

    let addr = format!("0.0.0.0:{port}");
    println!("qr_bin listening on http://{addr}");
    if let Ok(ip) = local_ip_address::local_ip() {
        println!("LAN:   http://{ip}:{port}");
    }
    println!("Expose this port to the internet (reverse proxy / tunnel) so your phone can reach it.");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn index(Query(q): Query<PageQuery>) -> Html<&'static str> {
    // Same HTML for both roles; the page branches on ?role=writer.
    let _ = (&q.role, &q.room);
    Html(INDEX_HTML)
}

async fn style_css() -> Response {
    (
        [(header::CONTENT_TYPE, "text/css")],
        STYLE_CSS,
    )
        .into_response()
}

async fn app_js() -> Response {
    (
        [(header::CONTENT_TYPE, "text/javascript")],
        APP_JS,
    )
        .into_response()
}

async fn qr_svg(Query(q): Query<QrQuery>) -> Response {
    if q.data.is_empty() || q.data.len() > MAX_QR_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, "qr data too large").into_response();
    }
    let Ok(code) = QrCode::new(q.data.as_bytes()) else {
        return (StatusCode::BAD_REQUEST, "invalid qr data").into_response();
    };
    let svg = code
        .render::<svg::Color>()
        .min_dimensions(320, 320)
        .quiet_zone(true)
        .build();
    (
        [(header::CONTENT_TYPE, "image/svg+xml")],
        svg,
    )
        .into_response()
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<WsQuery>,
    State(state): State<AppState>,
) -> Response {
    if q.room.is_empty() || q.room.len() > 64 {
        return (StatusCode::BAD_REQUEST, "bad room").into_response();
    }
    let role = if q.role == "host" {
        Role::Host
    } else {
        Role::Writer
    };
    ws.on_upgrade(move |socket| handle_socket(socket, q.room, role, state))
}

async fn handle_socket(socket: WebSocket, room_id: String, role: Role, state: AppState) {
    let id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    // Register and grab a snapshot of the room text.
    let snapshot = {
        let mut rooms = state.rooms.lock().await;
        let room = rooms
            .entry(room_id.clone())
            .or_insert_with(|| Room {
                last_text: String::new(),
                peers: HashMap::new(),
            });
        room.peers.insert(
            id,
            Peer { _role: role, tx: tx.clone() },
        );
        room.last_text.clone()
    };

    broadcast_presence(&state, &room_id).await;

    // Deliver current text so a late joiner is up to date.
    if !snapshot.is_empty() {
let _ = tx.send(
        Message::Text(
            serde_json::to_string(&OutMsg {
                kind: "set",
                text: Some(snapshot),
                n: None,
            })
            .unwrap_or_default()
            .into(),
        ),
    );
    }

    // Reader: forward incoming text messages to every other peer in the room.
    // On disconnect, unregister and notify everyone else.
    let (mut sender, mut receiver) = socket.split();
    let pump_state = state.clone();
    let pump_room = room_id.clone();
    let exit_tx = tx.clone();
    drop(tx); // keep only the room's instance + the pump's clone
    let pump = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Text(text) => {
                    let forward = normalize_set(&text);
                    let mut rooms = pump_state.rooms.lock().await;
                    if let Some(room) = rooms.get_mut(&pump_room) {
                        if let Some(t) = extract_text(&forward) {
                            room.last_text = t;
                        }
                        for (peer_id, peer) in room.peers.iter() {
                            if *peer_id != id {
                                let _ = peer.tx.send(Message::Text(forward.clone().into()));
                            }
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        // Send our own close so the drain loop terminates.
        let _ = exit_tx.send(Message::Close(None));

        // Unregister.
        {
            let mut rooms = pump_state.rooms.lock().await;
            if let Some(room) = rooms.get_mut(&pump_room) {
                room.peers.remove(&id);
                if room.peers.is_empty() {
                    rooms.remove(&pump_room);
                }
            }
        }
        broadcast_presence(&pump_state, &pump_room).await;
    });

    // Writer: drain forwarded messages out to this peer's socket.
    while let Some(msg) = rx.recv().await {
        if sender.send(msg).await.is_err() {
            break;
        }
    }
    pump.await.ok();
}

/// Rebuild an incoming client payload as our canonical `{type,text}` JSON.
fn normalize_set(text: &str) -> String {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(text)
        && let Some(t) = v.get("text").and_then(|t| t.as_str())
    {
        return serde_json::to_string(&OutMsg {
            kind: "set",
            text: Some(t.to_string()),
            n: None,
        })
        .unwrap_or_default();
    }
    serde_json::to_string(&OutMsg {
        kind: "set",
        text: Some(text.to_string()),
        n: None,
    })
    .unwrap_or_default()
}

fn extract_text(json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get("text")?
        .as_str()
        .map(str::to_string)
}

/// Tell everyone in the room how many peers are connected.
async fn broadcast_presence(state: &AppState, room_id: &str) {
    let (n, senders): (usize, Vec<PeerTx>) = {
        let rooms = state.rooms.lock().await;
        match rooms.get(room_id) {
            Some(room) => (
                room.peers.len(),
                room.peers.values().map(|p| p.tx.clone()).collect(),
            ),
            None => return,
        }
    };
    let msg = serde_json::to_string(&OutMsg {
        kind: "peers",
        text: None,
        n: Some(n),
    })
    .unwrap_or_default();
    for tx in senders {
        let _ = tx.send(Message::Text(msg.clone().into()));
    }
}