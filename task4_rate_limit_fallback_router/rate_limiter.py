"""Sliding-window token limiter stored in SQLite.

Each request is a row (tenant, ts, tokens) and usage is the sum over the last
60 seconds. Old rows are deleted on every check. BEGIN IMMEDIATE makes the
check-and-insert atomic, even with several processes sharing one DB file.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

SCHEMA = """
CREATE TABLE IF NOT EXISTS token_usage (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant  TEXT    NOT NULL,
    ts      REAL    NOT NULL,
    tokens  INTEGER NOT NULL CHECK (tokens >= 0)
);
CREATE INDEX IF NOT EXISTS ix_token_usage_tenant_ts ON token_usage (tenant, ts);
"""


@dataclass(frozen=True)
class Reservation:
    id: int
    tenant: str
    tokens: int


@dataclass(frozen=True)
class Denied:
    used: int
    limit: int
    requested: int
    retry_after_s: float


class SlidingWindowTokenLimiter:
    def __init__(self, db_path: str | Path, limit_tokens: int = 50_000, window_s: float = 60.0,
                 clock: Callable[[], float] = time.time) -> None:
        self.db_path = str(db_path)
        self.limit = limit_tokens
        self.window = window_s
        self.clock = clock
        self._local = threading.local()
        with self._conn() as c:
            c.executescript(SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=10000")
            self._local.conn = conn
        return conn

    def reserve_sync(self, tenant: str, tokens: int) -> Reservation | Denied:
        if tokens <= 0:
            raise ValueError("tokens must be positive")
        if tokens > self.limit:
            return Denied(used=0, limit=self.limit, requested=tokens, retry_after_s=float("inf"))

        c = self._conn()
        now = self.clock()
        cutoff = now - self.window
        c.execute("BEGIN IMMEDIATE")
        try:
            # evict rows that have left the window
            c.execute("DELETE FROM token_usage WHERE tenant = ? AND ts <= ?", (tenant, cutoff))
            used = c.execute("SELECT COALESCE(SUM(tokens), 0) FROM token_usage WHERE tenant = ?",
                             (tenant,)).fetchone()[0]
            if used + tokens > self.limit:
                retry = self._retry_after(c, tenant, used, tokens, now)
                c.execute("COMMIT")
                return Denied(used=used, limit=self.limit, requested=tokens, retry_after_s=retry)
            cur = c.execute("INSERT INTO token_usage (tenant, ts, tokens) VALUES (?, ?, ?)",
                            (tenant, now, tokens))
            c.execute("COMMIT")
            return Reservation(id=cur.lastrowid, tenant=tenant, tokens=tokens)
        except BaseException:
            c.execute("ROLLBACK")
            raise

    def _retry_after(self, c: sqlite3.Connection, tenant: str, used: int, tokens: int, now: float) -> float:
        need_to_free = used + tokens - self.limit
        freed = 0
        for ts, t in c.execute("SELECT ts, tokens FROM token_usage WHERE tenant = ? ORDER BY ts", (tenant,)):
            freed += t
            if freed >= need_to_free:
                return max(0.0, ts + self.window - now)
        return self.window

    def commit_sync(self, r: Reservation, actual_tokens: int) -> None:
        """Swap the estimate for the real usage. The timestamp stays the same."""
        self._conn().execute("UPDATE token_usage SET tokens = ? WHERE id = ?", (max(0, int(actual_tokens)), r.id))

    def release_sync(self, r: Reservation) -> None:
        self._conn().execute("DELETE FROM token_usage WHERE id = ?", (r.id,))

    def usage_sync(self, tenant: str) -> int:
        cutoff = self.clock() - self.window
        return self._conn().execute(
            "SELECT COALESCE(SUM(tokens), 0) FROM token_usage WHERE tenant = ? AND ts > ?",
            (tenant, cutoff)).fetchone()[0]

    def purge_expired_sync(self) -> int:
        """Cleans up rows for tenants that went quiet."""
        cur = self._conn().execute("DELETE FROM token_usage WHERE ts <= ?", (self.clock() - self.window,))
        return cur.rowcount

    async def reserve(self, tenant: str, tokens: int) -> Reservation | Denied:
        return await asyncio.to_thread(self.reserve_sync, tenant, tokens)

    async def commit(self, r: Reservation, actual_tokens: int) -> None:
        await asyncio.to_thread(self.commit_sync, r, actual_tokens)

    async def release(self, r: Reservation) -> None:
        await asyncio.to_thread(self.release_sync, r)

    async def usage(self, tenant: str) -> int:
        return await asyncio.to_thread(self.usage_sync, tenant)
