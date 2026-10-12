"""랩(에이전틱 플로우 3.0) — 파드 공유 ResponseStore.

배포는 파드가 여러 개이고 고정 배정이 없어, 대화 기록을 파드 안 SQLite 에만 두면 후속 요청이 다른
파드에 떨어질 때 앞 대화를 잊는다. 응답 스냅숏을 Supabase 표(agentic3_responses)에도 쓰고, 읽을 때는
그 표를 먼저 본다. 로컬 SQLite 는 그대로 함께 쓴다 — 표가 없거나 닿지 않으면 종전 동작으로 돌아간다.

켜기: AGENTIC3_SHARED=1 + NEXT_PUBLIC_SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY. 대화마다 최신 한 행만 남기고,
개수 상한 대신 기간(TTL_DAYS)으로 지운다 — 로컬 상한 100건은 한 사람 기준이라 여러 유저가 쓰면 금방 찬다.
"""
import base64
import gzip
import json
import logging
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from gateway.platforms.api_server import ResponseStore

logger = logging.getLogger(__name__)
TABLE = "agentic3_responses"
TTL_DAYS = 7
READ_TIMEOUT = 3.0
WRITE_TIMEOUT = 2.0


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class SharedResponseStore(ResponseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._url = (os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or "").rstrip("/")
        self._key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
        self._remote = bool(self._url and self._key)
        self._conv: Dict[str, str] = {}  # 이 프로세스가 마지막으로 묶은 대화 → 응답(같은 값 다시 쓰기 생략)
        self._puts = 0
        if self._remote:
            try:  # 표가 없으면(SQL 미실행) 처음부터 로컬만 쓴다
                self._rest("GET", "?select=response_id&limit=1", timeout=READ_TIMEOUT)
            except Exception as exc:
                self._remote = False
                logger.warning("shared response store off (%s) — local SQLite only", exc)

    def _rest(self, method: str, query: str, body: Any = None, prefer: str = "", timeout: float = WRITE_TIMEOUT):
        headers = {"apikey": self._key, "Authorization": f"Bearer {self._key}", "Content-Type": "application/json"}
        if prefer:
            headers["Prefer"] = prefer
        req = urllib.request.Request(
            f"{self._url}/rest/v1/{TABLE}{query}", headers=headers, method=method,
            data=None if body is None else json.dumps(body).encode())
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        return json.loads(raw) if raw else []

    def _live(self) -> str:
        return "expires_at=gt." + _iso(datetime.now(timezone.utc))

    def get(self, response_id: str) -> Optional[Dict[str, Any]]:
        if self._remote:
            try:
                rows = self._rest("GET", f"?select=data&response_id=eq.{urllib.parse.quote(response_id)}&{self._live()}", timeout=READ_TIMEOUT)
                if rows:
                    return json.loads(gzip.decompress(base64.b64decode(rows[0]["data"])))
            except Exception as exc:
                logger.warning("shared response store read failed (%s) — local fallback", exc)
        return super().get(response_id)

    def put(self, response_id: str, data: Dict[str, Any]) -> None:
        super().put(response_id, data)
        if not self._remote:
            return
        try:
            raw = json.dumps(data, default=str).encode()
            now = datetime.now(timezone.utc)
            self._rest("POST", "?on_conflict=response_id", [{
                "response_id": response_id, "data": base64.b64encode(gzip.compress(raw, 6)).decode(),
                "data_bytes": len(raw), "updated_at": _iso(now), "expires_at": _iso(now + timedelta(days=TTL_DAYS)),
            }], prefer="resolution=merge-duplicates,return=minimal")
            self._puts += 1
            if self._puts % 200 == 0:  # 만료 행 정리
                self._rest("DELETE", "?expires_at=lt." + _iso(now))
        except Exception as exc:
            logger.warning("shared response store write failed (%s)", exc)

    def delete(self, response_id: str) -> bool:
        found = super().delete(response_id)
        if self._remote:
            try:
                rows = self._rest("DELETE", f"?response_id=eq.{urllib.parse.quote(response_id)}", prefer="return=representation")
                found = found or bool(rows)
            except Exception as exc:
                logger.warning("shared response store delete failed (%s)", exc)
        return found

    def get_conversation(self, name: str) -> Optional[str]:
        if self._remote:
            try:
                rows = self._rest(
                    "GET", f"?select=response_id&conversation=eq.{urllib.parse.quote(name)}&{self._live()}&order=updated_at.desc&limit=1",
                    timeout=READ_TIMEOUT)
                if rows:
                    return rows[0]["response_id"]
            except Exception as exc:
                logger.warning("shared response store lookup failed (%s) — local fallback", exc)
        return super().get_conversation(name)

    def set_conversation(self, name: str, response_id: str) -> None:
        super().set_conversation(name, response_id)
        if not self._remote or self._conv.get(name) == response_id:
            return
        try:
            quoted = urllib.parse.quote(name)
            self._rest("PATCH", f"?response_id=eq.{urllib.parse.quote(response_id)}", {"conversation": name}, prefer="return=minimal")
            # 대화마다 최신 한 행만 — 앞선 턴의 스냅숏은 누적 기록이 새 행에 다 들어 있어 필요 없다
            self._rest("DELETE", f"?conversation=eq.{quoted}&response_id=neq.{urllib.parse.quote(response_id)}")
            self._conv[name] = response_id
            if len(self._conv) > 5000:
                self._conv.clear()
        except Exception as exc:
            logger.warning("shared response store bind failed (%s)", exc)
