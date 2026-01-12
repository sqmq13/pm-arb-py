from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import orjson

from .clob_ws import build_subscribe_payload
from .config import Config

SUBSCRIBE_VARIANTS = ("A", "B", "C")


@dataclass(slots=True)
class DropCounter:
    total: int = 0

    def bump(self, count: int = 1) -> None:
        self.total += count


@dataclass(slots=True)
class ReconnectPolicy:
    max_reconnects: int
    backoff_seconds: float

    def can_reconnect(self, reconnects: int) -> bool:
        if self.max_reconnects <= 0:
            return False
        return reconnects < self.max_reconnects

    def backoff(self) -> float:
        return max(0.0, self.backoff_seconds)


def normalize_ws_keepalive(
    config: Config,
) -> tuple[float | None, float | None, float | None]:
    ping_interval = config.ws_ping_interval_seconds
    if ping_interval <= 0:
        ping_interval = None
    ping_timeout = config.ws_ping_timeout_seconds
    if ping_timeout <= 0:
        ping_timeout = None
    data_idle_reconnect = config.ws_data_idle_reconnect_seconds
    if data_idle_reconnect <= 0:
        data_idle_reconnect = None
    return ping_interval, ping_timeout, data_idle_reconnect


def split_subscribe_groups(
    token_ids: Sequence[str],
    max_tokens: int,
    max_bytes: int,
    variant: str,
) -> list[list[str]]:
    groups: list[list[str]] = []
    current: list[str] = []
    for token_id in token_ids:
        candidate = current + [token_id]
        payload = build_subscribe_payload(variant, candidate)
        payload_bytes = orjson.dumps(payload)
        if len(candidate) > max_tokens or len(payload_bytes) > max_bytes:
            if not current:
                raise ValueError("single subscribe payload exceeds limits")
            groups.append(current)
            current = [token_id]
        else:
            current = candidate
    if current:
        groups.append(current)
    return groups


def is_confirm_payload(payload_bytes: bytes) -> bool:
    if payload_bytes in (b"PONG", b"PING", b"[]", b""):
        return False
    return True
