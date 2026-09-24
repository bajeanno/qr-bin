(function () {
  "use strict";

  var params = new URLSearchParams(location.search);
  var role = params.get("role") === "writer" ? "writer" : "host";
  var room = params.get("room") || "";

  var el = {
    dot: document.getElementById("dot"),
    status: document.getElementById("statusText"),
    qr: document.getElementById("qrImg"),
    link: document.getElementById("linkText"),
    hint: document.getElementById("hint"),
    text: document.getElementById("text"),
    count: document.getElementById("count"),
  };
  var qrPanel = document.getElementById("qrPanel");

  function setStatus(text, on) {
    el.status.textContent = text;
    el.dot.className = "dot" + (on ? " on" : "");
  }

  var ws = null;

  function connect() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
    var proto = location.protocol === "https:" ? "wss://" : "ws://";
    ws = new WebSocket(proto + location.host + "/ws?room=" + encodeURIComponent(room) + "&role=" + role);
    ws.onopen = function () { setStatus("connected", true); };
    ws.onerror = function () { setStatus("reconnecting…", false); };
    ws.onclose = function () {
      setStatus("reconnecting…", false);
      setTimeout(connect, 2000);
    };
    ws.onmessage = function (ev) {
      var msg;
      try { msg = JSON.parse(ev.data); } catch (e) { return; }
      if (msg.type === "set" && typeof msg.text === "string") {
        if (el.text.value !== msg.text) {
          el.text.value = msg.text;
          updateCount();
        }
      } else if (msg.type === "peers" && typeof msg.n === "number") {
        if (role === "writer") {
          setStatus("connected (" + (msg.n - 1) + " viewer" + (msg.n - 1 === 1 ? "" : "s") + ")", true);
        } else {
          var others = msg.n - 1;
          setStatus(others ? others + " device" + (others === 1 ? "" : "s") + " connected" : "scan the code on your phone", true);
        }
      }
    };
  }

  // Debounced send of current text (both directions; last write wins).
  var timer = null;
  function push() {
    clearTimeout(timer);
    timer = setTimeout(function () {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "set", text: el.text.value }));
      }
    }, 150);
  }

  function updateCount() {
    var n = el.text.value.length;
    el.count.textContent = n + (n === 1 ? " character" : " characters");
  }

  el.text.addEventListener("input", function () {
    updateCount();
    push();
  });

  // --- host setup: build the QR code ---
  function uid() {
    try {
      if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID().replace(/-/g, "").slice(0, 20);
    } catch (e) {}
    return Math.random().toString(36).slice(2) + Date.now().toString(36);
  }

  if (role === "host") {
    // Generate room, point the QR at the writer URL on the same origin.
    room = uid();
    var base = location.pathname.replace(/\/+$/, "");
    var writerUrl = location.origin + base + "?role=writer&room=" + encodeURIComponent(room);
    var short = writerUrl.length > 120 ? writerUrl.slice(0, 120) + "…" : writerUrl;
    el.link.textContent = short;
    el.hint.textContent = "Scan this code with your phone to open the writer. Type on your phone and it appears here instantly.";
    el.qr.src = "/qr.svg?data=" + encodeURIComponent(writerUrl);
    el.qr.onerror = function () { el.hint.textContent = "Could not load QR image."; };
    el.text.placeholder = "Waiting for your phone to connect…";
  } else {
    // Writer: no QR needed.
    qrPanel.style.display = "none";
    el.text.placeholder = "Type here… it appears on the computer instantly.";
    document.title = "QR Bin — writer";
  }

  connect();
})();