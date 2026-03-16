"""WebSocket-based real-time collaboration server for issue_tracker.

Implements RFC 6455 WebSocket framing from scratch using only the Python
standard library (socket, threading, hashlib, base64, struct, json, uuid).

Architecture
------------
- One TCP socket server thread accepts connections.
- Each client connection runs in its own daemon thread.
- A single RGA CRDT instance holds the shared document state.
- All CRDT operations are broadcast to every connected client.
- Cursors are tracked per-client and broadcast on change.
- The HTTP root path (GET /) serves the embedded HTML/JS editor.
"""
from __future__ import annotations

import base64
import hashlib
import json
import socket
import struct
import threading
import uuid
from typing import Any

from .crdt import RGA, Atom

# ── WebSocket constants ───────────────────────────────────────────────────────

_WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_OP_TEXT  = 0x1
_OP_CLOSE = 0x8
_OP_PING  = 0x9
_OP_PONG  = 0xA

# ── WebSocket frame codec ─────────────────────────────────────────────────────


def _ws_accept(key: str) -> str:
    digest = hashlib.sha1((key + _WS_MAGIC).encode()).digest()
    return base64.b64encode(digest).decode()


def _parse_frame(data: bytes) -> tuple[int, bytes, int]:
    """Parse one WebSocket frame from *data*.

    Returns ``(opcode, payload, bytes_consumed)``.
    Returns ``(-1, b'', 0)`` when *data* is too short.
    """
    if len(data) < 2:
        return -1, b"", 0

    opcode      = data[0] & 0x0F
    masked      = bool(data[1] & 0x80)
    payload_len = data[1] & 0x7F
    offset      = 2

    if payload_len == 126:
        if len(data) < 4:
            return -1, b"", 0
        payload_len = struct.unpack(">H", data[2:4])[0]
        offset = 4
    elif payload_len == 127:
        if len(data) < 10:
            return -1, b"", 0
        payload_len = struct.unpack(">Q", data[2:10])[0]
        offset = 10

    mask_key = b""
    if masked:
        if len(data) < offset + 4:
            return -1, b"", 0
        mask_key = data[offset : offset + 4]
        offset += 4

    if len(data) < offset + payload_len:
        return -1, b"", 0

    payload = data[offset : offset + payload_len]
    if masked:
        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

    return opcode, payload, offset + payload_len


def _make_frame(opcode: int, payload: bytes) -> bytes:
    """Build an unmasked WebSocket frame (server→client)."""
    header = bytes([0x80 | opcode])
    length = len(payload)
    if length < 126:
        header += bytes([length])
    elif length < 65536:
        header += bytes([126]) + struct.pack(">H", length)
    else:
        header += bytes([127]) + struct.pack(">Q", length)
    return header + payload


# ── Embedded HTML/JS client ───────────────────────────────────────────────────

_HTML_CLIENT = r"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<title>リアルタイムコラボ編集 – Issue Tracker</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:monospace;background:#1e1e1e;color:#d4d4d4;height:100vh;display:flex;flex-direction:column}
header{padding:10px 16px;background:#252526;border-bottom:1px solid #3e3e42;display:flex;align-items:center;gap:14px}
header h1{font-size:15px;color:#cccccc}
#status{font-size:12px}
main{flex:1;display:flex;overflow:hidden}
#editor-pane{flex:1;display:flex;flex-direction:column}
#editor{flex:1;padding:14px;background:#1e1e1e;color:#d4d4d4;border:none;outline:none;resize:none;font-family:'Courier New',monospace;font-size:14px;line-height:1.6;tab-size:4}
#sidebar{width:210px;background:#252526;border-left:1px solid #3e3e42;padding:12px;overflow-y:auto}
#sidebar h2{font-size:11px;color:#858585;text-transform:uppercase;margin-bottom:8px;letter-spacing:.5px}
.cursor-entry{display:flex;align-items:flex-start;gap:8px;padding:4px 0;font-size:12px}
.cursor-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;margin-top:3px}
.cursor-label{color:#cccccc}
.cursor-info{color:#858585;font-size:11px}
footer{padding:4px 16px;background:#007acc;color:#fff;font-size:12px;display:flex;gap:16px}
</style>
</head>
<body>
<header>
  <h1>Issue Tracker – リアルタイムコラボ編集</h1>
  <span id="status" style="color:#f44747">接続中…</span>
</header>
<main>
  <div id="editor-pane">
    <textarea id="editor" placeholder="テキストを入力してください…" spellcheck="false"></textarea>
  </div>
  <div id="sidebar">
    <h2>接続中のユーザー</h2>
    <div id="cursors-list"></div>
  </div>
</main>
<footer>
  <span>WebSocket + CRDT リアルタイム同期</span>
  <span id="my-cursor">カーソル: 1行 1列</span>
</footer>
<script>
// ── RGA CRDT (mirrors issue_tracker/crdt.py) ─────────────────────────────────
class RGA {
  constructor(siteId) {
    this.siteId = siteId;
    this.seq = 0;
    this.atoms = [];          // [{site_id,seq,char,after_site,after_seq,deleted}]
  }

  _findAfterPos(afterSite, afterSeq) {
    if (afterSite === '' && afterSeq === 0) return 0;
    for (let i = 0; i < this.atoms.length; i++) {
      if (this.atoms[i].site_id === afterSite && this.atoms[i].seq === afterSeq)
        return i + 1;
    }
    return 0;
  }

  _doInsert(atom) {
    let pos = this._findAfterPos(atom.after_site, atom.after_seq);
    while (pos < this.atoms.length) {
      const e = this.atoms[pos];
      if (e.after_site !== atom.after_site || e.after_seq !== atom.after_seq) break;
      if (e.seq > atom.seq || (e.seq === atom.seq && e.site_id > atom.site_id)) pos++;
      else break;
    }
    this.atoms.splice(pos, 0, Object.assign({}, atom));
  }

  applyInsert(atom) {
    if (this.atoms.some(a => a.site_id === atom.site_id && a.seq === atom.seq))
      return false;
    this._doInsert(atom);
    return true;
  }

  applyDelete(uid) {
    for (const a of this.atoms) {
      if (a.site_id === uid[0] && a.seq === uid[1]) {
        if (!a.deleted) { a.deleted = true; return true; }
        return false;
      }
    }
    return false;
  }

  localInsert(pos, char) {
    let visPos = 0, afterSite = '', afterSeq = 0;
    for (const a of this.atoms) {
      if (a.deleted) continue;
      if (visPos === pos) break;
      afterSite = a.site_id; afterSeq = a.seq; visPos++;
    }
    this.seq++;
    const atom = {site_id:this.siteId, seq:this.seq, char,
                  after_site:afterSite, after_seq:afterSeq, deleted:false};
    this._doInsert(atom);
    return atom;
  }

  localDelete(pos) {
    let visPos = 0;
    for (const a of this.atoms) {
      if (a.deleted) continue;
      if (visPos === pos) { a.deleted = true; return [a.site_id, a.seq]; }
      visPos++;
    }
    return null;
  }

  getValue() { return this.atoms.filter(a => !a.deleted).map(a => a.char).join(''); }

  loadState(state) {
    this.atoms = state.map(s => Object.assign({}, s));
    for (const a of this.atoms)
      if (a.site_id === this.siteId && a.seq > this.seq) this.seq = a.seq;
  }

  // Visible index of an atom (call after applyInsert)
  visiblePosOf(siteId, seq) {
    let pos = 0;
    for (const a of this.atoms) {
      if (a.site_id === siteId && a.seq === seq) return pos;
      if (!a.deleted) pos++;
    }
    return -1;
  }

  // Visible index of an atom *before* applyDelete
  visiblePosOfUid(uid) {
    let pos = 0;
    for (const a of this.atoms) {
      if (a.site_id === uid[0] && a.seq === uid[1])
        return a.deleted ? -1 : pos;
      if (!a.deleted) pos++;
    }
    return -1;
  }
}

// ── Cursor colours ────────────────────────────────────────────────────────────
const COLORS = ['#e06c75','#98c379','#e5c07b','#61afef','#c678dd','#56b6c2','#d19a66'];
const clientColors = {};
let colorIdx = 0;
function getColor(id) {
  if (!clientColors[id]) clientColors[id] = COLORS[colorIdx++ % COLORS.length];
  return clientColors[id];
}

// ── State ─────────────────────────────────────────────────────────────────────
let myId = null, crdt = null, ws = null;
let remoteCursors = {};
let applyingRemote = false;
let lastText = '';

const editor   = document.getElementById('editor');
const statusEl = document.getElementById('status');
const cursList = document.getElementById('cursors-list');
const myCurEl  = document.getElementById('my-cursor');

// ── WebSocket connection ──────────────────────────────────────────────────────
function connect() {
  const url = `ws://${location.host}/`;
  ws = new WebSocket(url);
  ws.onopen  = () => { statusEl.textContent = '接続済み'; statusEl.style.color='#4ec9b0'; };
  ws.onclose = () => { statusEl.textContent = '切断 – 再接続中…'; statusEl.style.color='#f44747'; setTimeout(connect, 2000); };
  ws.onerror = () => { statusEl.textContent = '接続エラー'; };
  ws.onmessage = e => handleMsg(JSON.parse(e.data));
}
function send(msg) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg)); }

// ── Message handler ───────────────────────────────────────────────────────────
function handleMsg(msg) {
  switch (msg.type) {
    case 'init': {
      myId = msg.client_id;
      crdt = new RGA(myId);
      crdt.loadState(msg.state);
      applyingRemote = true;
      editor.value = lastText = crdt.getValue();
      applyingRemote = false;
      remoteCursors = {};
      for (const [cid, cur] of Object.entries(msg.cursors || {}))
        if (cid !== myId)
          remoteCursors[cid] = {pos: cur.pos||0, color: getColor(cid), name: cur.name||cid.slice(0,8)};
      renderCursors();
      break;
    }
    case 'insert': {
      if (!crdt) break;
      const sel = editor.selectionStart;
      crdt.applyInsert(msg.atom);
      const insVis = crdt.visiblePosOf(msg.atom.site_id, msg.atom.seq);
      applyingRemote = true;
      editor.value = lastText = crdt.getValue();
      const newSel = insVis <= sel ? sel + 1 : sel;
      editor.setSelectionRange(newSel, newSel);
      applyingRemote = false;
      break;
    }
    case 'delete': {
      if (!crdt) break;
      const sel = editor.selectionStart;
      const delVis = crdt.visiblePosOfUid(msg.uid);
      crdt.applyDelete(msg.uid);
      applyingRemote = true;
      editor.value = lastText = crdt.getValue();
      const newSel = (delVis !== -1 && delVis < sel) ? sel - 1 : sel;
      editor.setSelectionRange(newSel, newSel);
      applyingRemote = false;
      break;
    }
    case 'cursor': {
      if (msg.client_id !== myId)
        remoteCursors[msg.client_id] = {pos: msg.cursor.pos||0, color: getColor(msg.client_id), name: msg.cursor.name||msg.client_id.slice(0,8)};
      renderCursors();
      break;
    }
    case 'cursor_remove': {
      delete remoteCursors[msg.client_id];
      renderCursors();
      break;
    }
  }
}

// ── Cursor sidebar ────────────────────────────────────────────────────────────
function renderCursors() {
  cursList.innerHTML = '';
  for (const [cid, info] of Object.entries(remoteCursors)) {
    const el = document.createElement('div');
    el.className = 'cursor-entry';
    el.innerHTML =
      `<div class="cursor-dot" style="background:${info.color}"></div>` +
      `<div><div class="cursor-label">${info.name}</div>` +
      `<div class="cursor-info">位置: ${info.pos}</div></div>`;
    cursList.appendChild(el);
  }
}

// ── Input handling ────────────────────────────────────────────────────────────
editor.addEventListener('input', () => {
  if (applyingRemote || !crdt) return;
  const newText = editor.value;
  const ops = diffText(lastText, newText);
  lastText = newText;
  for (const op of ops) {
    if (op.type === 'insert') {
      const atom = crdt.localInsert(op.pos, op.char);
      send({type:'insert', atom});
    } else {
      const uid = crdt.localDelete(op.pos);
      if (uid) send({type:'delete', uid});
    }
  }
});

// Compute minimal diff between two strings.
function diffText(oldT, newT) {
  let s = 0;
  while (s < oldT.length && s < newT.length && oldT[s] === newT[s]) s++;
  let oe = oldT.length, ne = newT.length;
  while (oe > s && ne > s && oldT[oe-1] === newT[ne-1]) { oe--; ne--; }
  const ops = [];
  for (let i = oe - 1; i >= s; i--) ops.push({type:'delete', pos:i});
  for (let i = s; i < ne; i++)      ops.push({type:'insert', pos:i, char:newT[i]});
  return ops;
}

// ── Cursor tracking ───────────────────────────────────────────────────────────
function sendCursor() {
  const pos = editor.selectionStart;
  const text = editor.value.slice(0, pos);
  const lines = text.split('\n');
  myCurEl.textContent = `カーソル: ${lines.length}行 ${lines[lines.length-1].length+1}列`;
  send({type:'cursor', cursor:{pos}});
}
editor.addEventListener('keyup',   sendCursor);
editor.addEventListener('click',   sendCursor);
editor.addEventListener('mouseup', sendCursor);

connect();
</script>
</body>
</html>
"""


# ── Collaboration server ──────────────────────────────────────────────────────


class CollabServer:
    """TCP server that speaks WebSocket and keeps an RGA CRDT document.

    Clients connecting with a WebSocket upgrade are registered and receive
    the current document state plus real-time operation broadcasts.
    Plain HTTP ``GET /`` requests receive the embedded HTML editor.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
    ) -> None:
        self.host = host
        self.port = port
        self._crdt = RGA("server")
        self._clients: dict[str, socket.socket] = {}
        self._cursors: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    # ── public ────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Block and serve until KeyboardInterrupt."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen(64)
            print(f"コラボ編集サーバー起動: http://{self.host}:{self.port}/")
            print("Ctrl+C で停止")
            try:
                while True:
                    conn, addr = srv.accept()
                    t = threading.Thread(
                        target=self._handle_conn, args=(conn,), daemon=True
                    )
                    t.start()
            except KeyboardInterrupt:
                print("\n停止しました。")

    # ── internal ──────────────────────────────────────────────────────────

    def _broadcast(self, msg: dict[str, Any], exclude: str | None = None) -> None:
        frame = _make_frame(_OP_TEXT, json.dumps(msg).encode())
        dead: list[str] = []
        with self._lock:
            targets = list(self._clients.items())
        for cid, sock in targets:
            if cid == exclude:
                continue
            try:
                sock.sendall(frame)
            except OSError:
                dead.append(cid)
        for cid in dead:
            self._drop_client(cid)

    def _send_to(self, client_id: str, msg: dict[str, Any]) -> None:
        frame = _make_frame(_OP_TEXT, json.dumps(msg).encode())
        with self._lock:
            sock = self._clients.get(client_id)
        if sock:
            try:
                sock.sendall(frame)
            except OSError:
                self._drop_client(client_id)

    def _drop_client(self, client_id: str) -> None:
        with self._lock:
            self._clients.pop(client_id, None)
            self._cursors.pop(client_id, None)
        self._broadcast({"type": "cursor_remove", "client_id": client_id})

    # ── HTTP / WebSocket upgrade ──────────────────────────────────────────

    def _read_http_request(self, conn: socket.socket) -> bytes:
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk
        return buf

    def _handle_conn(self, conn: socket.socket) -> None:
        client_id = ""
        try:
            raw = self._read_http_request(conn)
            if not raw:
                conn.close()
                return

            header_part = raw.split(b"\r\n\r\n")[0].decode("utf-8", errors="replace")
            headers: dict[str, str] = {}
            lines = header_part.split("\r\n")
            for line in lines[1:]:
                if ":" in line:
                    k, _, v = line.partition(":")
                    headers[k.strip().lower()] = v.strip()

            if headers.get("upgrade", "").lower() == "websocket" and "sec-websocket-key" in headers:
                # WebSocket upgrade
                accept = _ws_accept(headers["sec-websocket-key"])
                conn.sendall(
                    (
                        "HTTP/1.1 101 Switching Protocols\r\n"
                        "Upgrade: websocket\r\n"
                        "Connection: Upgrade\r\n"
                        f"Sec-WebSocket-Accept: {accept}\r\n"
                        "\r\n"
                    ).encode()
                )
                client_id = str(uuid.uuid4())
                with self._lock:
                    self._clients[client_id] = conn
                self._on_ws_open(client_id)
                self._ws_loop(conn, client_id)
            else:
                # Plain HTTP – serve editor page
                body = _HTML_CLIENT.encode("utf-8")
                conn.sendall(
                    (
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: text/html; charset=utf-8\r\n"
                        f"Content-Length: {len(body)}\r\n"
                        "Connection: close\r\n"
                        "\r\n"
                    ).encode()
                    + body
                )
        except OSError:
            pass
        finally:
            if client_id:
                self._drop_client(client_id)
            try:
                conn.close()
            except OSError:
                pass

    def _on_ws_open(self, client_id: str) -> None:
        with self._lock:
            state = self._crdt.to_state()
            cursors = dict(self._cursors)
        self._send_to(client_id, {
            "type": "init",
            "client_id": client_id,
            "state": state,
            "cursors": cursors,
        })

    def _ws_loop(self, conn: socket.socket, client_id: str) -> None:
        buf = b""
        while True:
            try:
                chunk = conn.recv(4096)
            except OSError:
                break
            if not chunk:
                break
            buf += chunk
            while buf:
                opcode, payload, consumed = _parse_frame(buf)
                if consumed == 0:
                    break
                buf = buf[consumed:]
                if opcode == _OP_CLOSE:
                    return
                if opcode == _OP_PING:
                    try:
                        conn.sendall(_make_frame(_OP_PONG, payload))
                    except OSError:
                        return
                elif opcode == _OP_TEXT:
                    try:
                        msg = json.loads(payload.decode("utf-8"))
                        self._handle_msg(client_id, msg)
                    except (json.JSONDecodeError, KeyError, ValueError):
                        pass

    def _handle_msg(self, client_id: str, msg: dict[str, Any]) -> None:
        kind = msg.get("type")

        if kind == "insert":
            atom = Atom.from_dict(msg["atom"])
            with self._lock:
                applied = self._crdt.apply_insert(atom)
            if applied:
                self._broadcast({"type": "insert", "atom": atom.to_dict()}, exclude=client_id)

        elif kind == "delete":
            uid = (msg["uid"][0], msg["uid"][1])
            with self._lock:
                applied = self._crdt.apply_delete(uid)
            if applied:
                self._broadcast({"type": "delete", "uid": list(uid)}, exclude=client_id)

        elif kind == "cursor":
            cursor_info = msg.get("cursor", {})
            with self._lock:
                self._cursors[client_id] = cursor_info
            self._broadcast(
                {"type": "cursor", "client_id": client_id, "cursor": cursor_info},
                exclude=client_id,
            )
