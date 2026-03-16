"""Tests for the CRDT and WebSocket collaboration server."""
from __future__ import annotations

import base64
import json
import os
import socket
import struct
import threading
import time
import unittest

from issue_tracker.crdt import RGA, Atom
from issue_tracker.collab import CollabServer, _parse_frame, _make_frame


# ── CRDT unit tests ───────────────────────────────────────────────────────────


class TestRGA(unittest.TestCase):

    def test_insert_single(self) -> None:
        rga = RGA("s1")
        rga.local_insert(0, "a")
        self.assertEqual(rga.value(), "a")

    def test_insert_sequential(self) -> None:
        rga = RGA("s1")
        rga.local_insert(0, "H")
        rga.local_insert(1, "i")
        self.assertEqual(rga.value(), "Hi")

    def test_insert_at_middle(self) -> None:
        rga = RGA("s1")
        rga.local_insert(0, "a")
        rga.local_insert(1, "c")
        rga.local_insert(1, "b")
        self.assertEqual(rga.value(), "abc")

    def test_delete_first(self) -> None:
        rga = RGA("s1")
        rga.local_insert(0, "a")
        rga.local_insert(1, "b")
        rga.local_delete(0)
        self.assertEqual(rga.value(), "b")

    def test_delete_last(self) -> None:
        rga = RGA("s1")
        rga.local_insert(0, "a")
        rga.local_insert(1, "b")
        rga.local_delete(1)
        self.assertEqual(rga.value(), "a")

    def test_delete_out_of_range_returns_none(self) -> None:
        rga = RGA("s1")
        result = rga.local_delete(99)
        self.assertIsNone(result)

    def test_apply_insert_idempotent(self) -> None:
        rga = RGA("s1")
        atom = rga.local_insert(0, "x")
        applied = rga.apply_insert(atom)
        self.assertFalse(applied)
        self.assertEqual(rga.value(), "x")

    def test_apply_delete_idempotent(self) -> None:
        rga = RGA("s1")
        uid = rga.local_insert(0, "x").uid
        rga.apply_delete(uid)
        second = rga.apply_delete(uid)
        self.assertFalse(second)
        self.assertEqual(rga.value(), "")

    def test_concurrent_insert_convergence(self) -> None:
        """Two sites insert at position 0 simultaneously; both must converge."""
        s1 = RGA("alice")
        s2 = RGA("bob")

        a = s1.local_insert(0, "A")
        b = s2.local_insert(0, "B")

        s1.apply_insert(b)
        s2.apply_insert(a)

        self.assertEqual(s1.value(), s2.value())
        self.assertIn("A", s1.value())
        self.assertIn("B", s1.value())

    def test_concurrent_insert_at_different_positions(self) -> None:
        s1 = RGA("alice")
        s2 = RGA("bob")

        # Seed both with the same initial state
        seed = s1.local_insert(0, "X")
        s2.apply_insert(seed)

        a = s1.local_insert(1, "A")   # alice appends after X
        b = s2.local_insert(0, "B")   # bob inserts before X

        s1.apply_insert(b)
        s2.apply_insert(a)

        self.assertEqual(s1.value(), s2.value())

    def test_state_serialization_roundtrip(self) -> None:
        rga = RGA("s1")
        rga.local_insert(0, "h")
        rga.local_insert(1, "i")
        state = rga.to_state()

        rga2 = RGA("s2")
        rga2.from_state(state)
        self.assertEqual(rga2.value(), "hi")

    def test_atom_to_from_dict(self) -> None:
        atom = Atom(site_id="s1", seq=3, char="z",
                    after_site="s1", after_seq=2, deleted=False)
        d = atom.to_dict()
        atom2 = Atom.from_dict(d)
        self.assertEqual(atom.uid, atom2.uid)
        self.assertEqual(atom.char, atom2.char)
        self.assertEqual(atom.after_uid, atom2.after_uid)

    def test_large_document(self) -> None:
        rga = RGA("s1")
        text = "Hello, World!"
        for i, ch in enumerate(text):
            rga.local_insert(i, ch)
        self.assertEqual(rga.value(), text)

    def test_delete_in_middle(self) -> None:
        rga = RGA("s1")
        for i, ch in enumerate("abcde"):
            rga.local_insert(i, ch)
        rga.local_delete(2)  # remove 'c'
        self.assertEqual(rga.value(), "abde")


# ── WebSocket frame codec tests ───────────────────────────────────────────────


class TestFrameCodec(unittest.TestCase):

    def _client_frame(self, opcode: int, payload: bytes) -> bytes:
        """Build a masked client frame (as a browser would send)."""
        mask_key = os.urandom(4)
        masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
        length = len(payload)
        header = bytes([0x80 | opcode])
        if length < 126:
            header += bytes([0x80 | length])
        elif length < 65536:
            header += bytes([0x80 | 126]) + struct.pack(">H", length)
        else:
            header += bytes([0x80 | 127]) + struct.pack(">Q", length)
        return header + mask_key + masked

    def test_small_payload(self) -> None:
        frame = self._client_frame(1, b"hello")
        op, payload, consumed = _parse_frame(frame)
        self.assertEqual(op, 1)
        self.assertEqual(payload, b"hello")
        self.assertEqual(consumed, len(frame))

    def test_empty_payload(self) -> None:
        frame = self._client_frame(1, b"")
        op, payload, consumed = _parse_frame(frame)
        self.assertEqual(op, 1)
        self.assertEqual(payload, b"")

    def test_short_data_returns_minus_one(self) -> None:
        op, payload, consumed = _parse_frame(b"\x81")
        self.assertEqual(op, -1)
        self.assertEqual(consumed, 0)

    def test_server_frame_roundtrip(self) -> None:
        payload = b'{"type":"init"}'
        frame = _make_frame(1, payload)
        # Server frames are unmasked; parse them directly.
        op, decoded, consumed = _parse_frame(frame)
        self.assertEqual(op, 1)
        self.assertEqual(decoded, payload)
        self.assertEqual(consumed, len(frame))


# ── WebSocket server integration tests ───────────────────────────────────────


def _ws_connect(host: str, port: int) -> tuple[socket.socket, bytes]:
    """Open a WebSocket connection to the test server.

    Returns ``(socket, leftover_bytes)`` – bytes read past the HTTP headers
    that belong to the first WebSocket frame.
    """
    key = base64.b64encode(os.urandom(16)).decode()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    request = (
        f"GET / HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    sock.sendall(request.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = sock.recv(4096)
        if not chunk:
            break
        resp += chunk
    parts = resp.split(b"\r\n\r\n", 1)
    assert b"101 Switching Protocols" in parts[0], f"Unexpected response: {parts[0][:200]}"
    leftover = parts[1] if len(parts) > 1 else b""
    return sock, leftover


def _ws_send(sock: socket.socket, msg: dict) -> None:
    """Send a masked WebSocket text frame."""
    payload = json.dumps(msg).encode()
    mask_key = os.urandom(4)
    masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    length = len(payload)
    header = bytes([0x81])
    if length < 126:
        header += bytes([0x80 | length])
    elif length < 65536:
        header += bytes([0x80 | 126]) + struct.pack(">H", length)
    else:
        header += bytes([0x80 | 127]) + struct.pack(">Q", length)
    sock.sendall(header + mask_key + masked)


def _ws_recv(sock: socket.socket, timeout: float = 3.0, *, buf: bytes = b"") -> dict:
    """Receive one WebSocket text frame.

    *buf* may contain bytes already read from the socket (leftover from the
    HTTP handshake or a previous frame).
    """
    sock.settimeout(timeout)
    while True:
        if buf:
            op, payload, consumed = _parse_frame(buf)
            if consumed > 0:
                return json.loads(payload.decode("utf-8"))
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Socket closed before frame received")
        buf += chunk


def _find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestCollabServer(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.port = _find_free_port()
        cls.server = CollabServer(host="127.0.0.1", port=cls.port)
        cls.thread = threading.Thread(target=cls._run_server, daemon=True)
        cls.thread.start()
        # Give the server a moment to bind.
        time.sleep(0.15)

    @classmethod
    def _run_server(cls) -> None:
        try:
            cls.server.start()
        except Exception:
            pass

    def _client(self) -> tuple[socket.socket, bytes]:
        return _ws_connect("127.0.0.1", self.port)

    def test_init_message(self) -> None:
        sock, lo = self._client()
        try:
            msg = _ws_recv(sock, buf=lo)
            self.assertEqual(msg["type"], "init")
            self.assertIn("client_id", msg)
            self.assertIn("state", msg)
            self.assertIsInstance(msg["state"], list)
        finally:
            sock.close()

    def test_insert_broadcast(self) -> None:
        """Insert from one client must be broadcast to another."""
        c1, lo1 = self._client()
        c2, lo2 = self._client()
        try:
            init1 = _ws_recv(c1, buf=lo1)
            _ws_recv(c2, buf=lo2)  # consume init2

            atom = {
                "site_id": init1["client_id"],
                "seq": 1,
                "char": "X",
                "after_site": "",
                "after_seq": 0,
                "deleted": False,
            }
            _ws_send(c1, {"type": "insert", "atom": atom})

            msg = _ws_recv(c2)
            self.assertEqual(msg["type"], "insert")
            self.assertEqual(msg["atom"]["char"], "X")
        finally:
            c1.close()
            c2.close()

    def test_delete_broadcast(self) -> None:
        """Delete from one client must be broadcast to another."""
        c1, lo1 = self._client()
        c2, lo2 = self._client()
        try:
            init1 = _ws_recv(c1, buf=lo1)
            _ws_recv(c2, buf=lo2)  # consume init

            # First insert an atom so we have something to delete.
            atom = {
                "site_id": init1["client_id"],
                "seq": 2,
                "char": "Y",
                "after_site": "",
                "after_seq": 0,
                "deleted": False,
            }
            _ws_send(c1, {"type": "insert", "atom": atom})
            _ws_recv(c2)  # consume insert broadcast

            # Now delete it.
            _ws_send(c1, {"type": "delete", "uid": [init1["client_id"], 2]})
            msg = _ws_recv(c2)
            self.assertEqual(msg["type"], "delete")
        finally:
            c1.close()
            c2.close()

    def test_cursor_broadcast(self) -> None:
        """Cursor updates from one client must be forwarded to others."""
        c1, lo1 = self._client()
        c2, lo2 = self._client()
        try:
            _ws_recv(c1, buf=lo1)
            _ws_recv(c2, buf=lo2)

            _ws_send(c1, {"type": "cursor", "cursor": {"pos": 5, "line": 1, "col": 6}})
            msg = _ws_recv(c2)
            self.assertEqual(msg["type"], "cursor")
            self.assertEqual(msg["cursor"]["pos"], 5)
        finally:
            c1.close()
            c2.close()

    def test_new_client_gets_existing_state(self) -> None:
        """A client that joins after inserts receives the full CRDT state."""
        c1, lo1 = self._client()
        try:
            _ws_recv(c1, buf=lo1)
            atom = {
                "site_id": "seed-site",
                "seq": 99,
                "char": "Z",
                "after_site": "",
                "after_seq": 0,
                "deleted": False,
            }
            _ws_send(c1, {"type": "insert", "atom": atom})
            time.sleep(0.05)  # allow server to process
        finally:
            c1.close()

        c2, lo2 = self._client()
        try:
            init = _ws_recv(c2, buf=lo2)
            chars = [a["char"] for a in init["state"] if not a["deleted"]]
            self.assertIn("Z", chars)
        finally:
            c2.close()

    def test_http_serves_html(self) -> None:
        """Plain HTTP GET / must return the HTML editor page."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", self.port))
        sock.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        response = b""
        sock.settimeout(3.0)
        try:
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk
        except socket.timeout:
            pass
        finally:
            sock.close()
        self.assertIn(b"200 OK", response)
        self.assertIn(b"text/html", response)
        self.assertIn(b"WebSocket", response)


if __name__ == "__main__":
    unittest.main()
