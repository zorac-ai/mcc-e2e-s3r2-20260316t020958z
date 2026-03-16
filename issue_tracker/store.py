from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class IssueStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list(self, *, status: str | None = None) -> list[dict[str, Any]]:
        payload = self._load()
        items = payload["issues"]
        if status:
            items = [item for item in items if item["status"] == status]
        return items

    def add(self, title: str) -> dict[str, Any]:
        cleaned = title.strip()
        if not cleaned:
            raise ValueError("title must not be empty")
        payload = self._load()
        issue = {
            "id": payload["next_id"],
            "title": cleaned,
            "status": "open",
            "created_at": _now(),
            "closed_at": None,
        }
        payload["next_id"] += 1
        payload["issues"].append(issue)
        self._save(payload)
        return issue

    def close(self, issue_id: int) -> dict[str, Any]:
        payload = self._load()
        issue = self._require_issue(payload, issue_id)
        if issue["status"] == "closed":
            return issue
        issue["status"] = "closed"
        issue["closed_at"] = _now()
        self._save(payload)
        return issue

    # MCC-LIVE-E2E: store method anchor

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"next_id": 1, "issues": []}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _save(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def _require_issue(self, payload: dict[str, Any], issue_id: int) -> dict[str, Any]:
        for issue in payload["issues"]:
            if issue["id"] == issue_id:
                return issue
        raise KeyError(f"issue {issue_id} not found")
