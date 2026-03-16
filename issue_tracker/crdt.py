"""Replicated Growable Array (RGA) CRDT for collaborative text editing.

This module provides a conflict-free replicated data type for concurrent
text editing.  All operations commute and are idempotent, so replicas
converge to the same state regardless of operation delivery order.

No third-party dependencies – standard library only.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Atom:
    """A single character node in the RGA sequence."""

    site_id: str
    seq: int
    char: str
    after_site: str   # site_id of the predecessor atom ('' = document start)
    after_seq: int    # seq      of the predecessor atom (0  = document start)
    deleted: bool = False

    # ── convenience ──────────────────────────────────────────────────────

    @property
    def uid(self) -> tuple[str, int]:
        return (self.site_id, self.seq)

    @property
    def after_uid(self) -> tuple[str, int]:
        return (self.after_site, self.after_seq)

    def to_dict(self) -> dict[str, Any]:
        return {
            "site_id": self.site_id,
            "seq": self.seq,
            "char": self.char,
            "after_site": self.after_site,
            "after_seq": self.after_seq,
            "deleted": self.deleted,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Atom":
        return cls(
            site_id=d["site_id"],
            seq=d["seq"],
            char=d["char"],
            after_site=d["after_site"],
            after_seq=d["after_seq"],
            deleted=bool(d.get("deleted", False)),
        )


# Sentinel representing the position before all atoms.
_SENTINEL: tuple[str, int] = ("", 0)


class RGA:
    """Replicated Growable Array for collaborative text.

    Usage (single site)::

        rga = RGA("alice")
        atom = rga.local_insert(0, "H")
        rga.local_insert(1, "i")
        assert rga.value() == "Hi"
        rga.local_delete(0)
        assert rga.value() == "i"

    Usage (multi-site)::

        alice = RGA("alice")
        bob   = RGA("bob")
        a = alice.local_insert(0, "a")
        b = bob.local_insert(0, "b")
        alice.apply_insert(b)
        bob.apply_insert(a)
        assert alice.value() == bob.value()   # converged
    """

    def __init__(self, site_id: str) -> None:
        self.site_id = site_id
        self._seq: int = 0
        self._atoms: list[Atom] = []

    # ── public API ────────────────────────────────────────────────────────

    def local_insert(self, pos: int, char: str) -> Atom:
        """Insert *char* at visible position *pos* and return the new atom."""
        after_uid = _SENTINEL
        visible = 0
        for atom in self._atoms:
            if atom.deleted:
                continue
            if visible == pos:
                break
            after_uid = atom.uid
            visible += 1

        self._seq += 1
        new_atom = Atom(
            site_id=self.site_id,
            seq=self._seq,
            char=char,
            after_site=after_uid[0],
            after_seq=after_uid[1],
        )
        self._do_insert(new_atom)
        return new_atom

    def local_delete(self, pos: int) -> Optional[tuple[str, int]]:
        """Delete the character at visible position *pos*.

        Returns the (site_id, seq) uid of the deleted atom, or *None* if
        *pos* is out of range.
        """
        visible = 0
        for atom in self._atoms:
            if atom.deleted:
                continue
            if visible == pos:
                atom.deleted = True
                return atom.uid
            visible += 1
        return None

    def apply_insert(self, atom: Atom) -> bool:
        """Apply a remote insert operation.

        Returns *True* if the atom was new, *False* if it was a duplicate.
        """
        for existing in self._atoms:
            if existing.uid == atom.uid:
                return False
        self._do_insert(atom)
        return True

    def apply_delete(self, uid: tuple[str, int]) -> bool:
        """Apply a remote delete operation.

        Returns *True* if the atom was found and freshly deleted.
        """
        for atom in self._atoms:
            if atom.uid == uid:
                if not atom.deleted:
                    atom.deleted = True
                    return True
                return False
        return False

    def value(self) -> str:
        """Return the current visible text."""
        return "".join(a.char for a in self._atoms if not a.deleted)

    def to_state(self) -> list[dict[str, Any]]:
        """Serialise the full CRDT state (for sending to new clients)."""
        return [a.to_dict() for a in self._atoms]

    def from_state(self, state: list[dict[str, Any]]) -> None:
        """Load state from a serialised list (e.g. received from server)."""
        self._atoms = [Atom.from_dict(d) for d in state]
        # Re-sync the local sequence counter.
        for a in self._atoms:
            if a.site_id == self.site_id and a.seq > self._seq:
                self._seq = a.seq

    # ── internal helpers ──────────────────────────────────────────────────

    def _find_after_pos(self, after_uid: tuple[str, int]) -> int:
        """Return the index *after* the atom identified by *after_uid*."""
        if after_uid == _SENTINEL:
            return 0
        for i, atom in enumerate(self._atoms):
            if atom.uid == after_uid:
                return i + 1
        # Predecessor not found – fall back to the beginning.
        return 0

    def _do_insert(self, new_atom: Atom) -> None:
        """Insert *new_atom* in the correct position using RGA ordering."""
        pos = self._find_after_pos(new_atom.after_uid)

        # Resolve concurrent inserts at the same position:
        # atoms with a higher (seq, site_id) sort *earlier* (appear first).
        while pos < len(self._atoms):
            existing = self._atoms[pos]
            if existing.after_uid != new_atom.after_uid:
                break
            if (existing.seq, existing.site_id) > (new_atom.seq, new_atom.site_id):
                pos += 1
            else:
                break

        self._atoms.insert(pos, new_atom)
