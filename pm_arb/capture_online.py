from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import platform
import shutil
import socket
import struct
import time
from math import gcd
from collections import deque
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Iterable

import orjson
import websockets

from .capture import RunBootstrap, _append_ndjson, _write_json, bootstrap_run, monotonic_ns
from .capture_format import (
    FLAG_BINARY_PAYLOAD,
    FLAG_TEXT_PAYLOAD,
    append_record,
    frames_header_len,
    frames_header_struct,
    frames_magic,
    idx_entry_len,
)
from .capture_offline import quantile
from .clob_ws import build_subscribe_payload
from .config import Config
from .gamma import fetch_markets, parse_clob_token_ids, select_active_binary_markets

SUBSCRIBE_VARIANTS = ("A", "B", "C")

FATAL_LOW_DISK = "LOW_DISK"
FATAL_DROP = "DROP"
FATAL_LATENCY = "LATENCY_FATAL"
FATAL_BACKPRESSURE = "BACKPRESSURE_STALL"
FATAL_RECONNECT_STORM = "RECONNECT_STORM"
FATAL_VERIFY = "VERIFY_CORRUPTION"
FATAL_SUBSCRIBE_CONFIRM = "SUBSCRIBE_CONFIRM_FAIL"
FATAL_SHARD_ASSIGNMENT = "SHARD_ASSIGNMENT_INVALID"
FATAL_MONO_QUANTIZED = "MONO_TIME_QUANTIZED"
FATAL_INTERNAL = "INTERNAL_ASSERT"


@dataclass
class ShardStats:
    frames: int = 0
    bytes_written: int = 0
    write_durations_ns: deque[int] = field(default_factory=deque)
    ingest_latencies_ns: deque[int] = field(default_factory=deque)
    backpressure_ns: deque[int] = field(default_factory=deque)
    token_ids: set[str] = field(default_factory=set)
    msg_type_counts: dict[str, int] = field(default_factory=dict)
    decode_errors: int = 0

    def record(
        self,
        payload_len: int,
        header_len: int,
        idx_len: int,
        write_duration_ns: int,
        ingest_latency_ns: int,
        backpressure_ns: int,
        token_ids: Iterable[str],
        msg_type_counts: dict[str, int],
        max_samples: int,
    ) -> None:
        self.frames += 1
        self.bytes_written += payload_len + header_len + idx_len
        _append_sample(self.write_durations_ns, write_duration_ns, max_samples)
        _append_sample(self.ingest_latencies_ns, ingest_latency_ns, max_samples)
        _append_sample(self.backpressure_ns, backpressure_ns, max_samples)
        for token_id in token_ids:
            self.token_ids.add(token_id)
        for msg_type, count in msg_type_counts.items():
            self.msg_type_counts[msg_type] = self.msg_type_counts.get(msg_type, 0) + count


@dataclass
class ShardState:
    shard_id: int
    token_ids: list[str]
    groups: list[list[str]]
    frames_path: Path
    idx_path: Path
    frames_fh: Any
    idx_fh: Any
    ring: deque[bytes]
    stats: ShardStats = field(default_factory=ShardStats)
    last_seen: dict[str, int] = field(default_factory=dict)
    reconnects: int = 0
    confirm_failures: int = 0
    confirmed: bool = False
    confirm_token_ids: list[str] = field(default_factory=list)
    confirm_events_seen: int = 0
    confirm_deadline_mono_ns: int | None = None


@dataclass
class CaptureState:
    run: RunBootstrap
    config: Config
    shards: list[ShardState]
    pinned_tokens: list[str]
    backpressure_breach_count: int = 0
    fatal_event: asyncio.Event = field(default_factory=asyncio.Event)
    fatal_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    heartbeat_samples: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=10))


def _append_sample(samples: deque[int], value: int, max_samples: int) -> None:
    samples.append(value)
    while len(samples) > max_samples:
        samples.popleft()


def _quantile_from_samples(samples: Iterable[int], percentile: float) -> int:
    values = [value for value in samples if value > 0]
    if not values:
        return 0
    return quantile(values, percentile)


class ShardAssignmentError(RuntimeError):
    def __init__(
        self,
        message: str,
        errors: list[dict[str, Any]],
        shard_map: dict[int, list[str]],
        market_shards: dict[str, int],
    ) -> None:
        super().__init__(message)
        self.errors = errors
        self.shard_map = shard_map
        self.market_shards = market_shards


def _monotonic_precision_stats(sample_count: int) -> dict[str, Any]:
    if sample_count < 2:
        raise ValueError("sample_count must be >= 2")
    stamps = [monotonic_ns() for _ in range(sample_count)]
    deltas = [
        later - earlier
        for earlier, later in zip(stamps, stamps[1:])
        if later > earlier
    ]
    any_non_ms = any(stamp % 1_000_000 != 0 for stamp in stamps)
    stats: dict[str, Any] = {
        "sample_count": sample_count,
        "any_non_ms": any_non_ms,
    }
    if not deltas:
        stats["ok"] = False
        stats["reason"] = "no_positive_deltas"
        return stats
    delta_gcd = deltas[0]
    for delta in deltas[1:]:
        delta_gcd = gcd(delta_gcd, delta)
    stats.update(
        {
            "delta_gcd_ns": delta_gcd,
            "min_delta_ns": min(deltas),
            "max_delta_ns": max(deltas),
        }
    )
    stats["ok"] = any_non_ms and delta_gcd < 1_000_000
    if not stats["ok"]:
        stats["reason"] = "quantized"
    return stats


def _config_snapshot(config: Config) -> dict[str, Any]:
    snapshot = {}
    for field in fields(Config):
        snapshot[field.name] = getattr(config, field.name)
    return snapshot


def _read_git_commit() -> str:
    git_dir = Path.cwd() / ".git"
    head_path = git_dir / "HEAD"
    if not head_path.exists():
        return "unknown"
    head = head_path.read_text(encoding="utf-8").strip()
    if head.startswith("ref:"):
        ref = head.split(" ", 1)[1].strip()
        ref_path = git_dir / ref
        if ref_path.exists():
            return ref_path.read_text(encoding="utf-8").strip()
    return head or "unknown"


def _environment_metadata() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "hostname": socket.gethostname(),
    }


def _stable_hash(token_id: str) -> int:
    digest = hashlib.sha256(token_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "little", signed=False)


def _coerce_numeric(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _token_score_map(markets: list[dict[str, Any]]) -> dict[str, float]:
    scores: dict[str, float] = {}
    numeric_keys = ("volume", "volumeNum", "liquidity", "liquidityNum")
    for market in markets:
        score: float | None = None
        for key in numeric_keys:
            score = _coerce_numeric(market.get(key))
            if score is not None:
                break
        if score is None:
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        for token_id in token_ids:
            current = scores.get(token_id)
            if current is None or score > current:
                scores[token_id] = score
    return scores


def _select_confirm_tokens(
    shard_tokens: list[str],
    scores: dict[str, float],
    max_tokens: int,
) -> list[str]:
    if not shard_tokens:
        return []
    scored: list[tuple[float, str]] = []
    for token_id in shard_tokens:
        score = scores.get(token_id)
        if score is not None:
            scored.append((score, token_id))
    if scored:
        scored.sort(key=lambda item: (-item[0], item[1]))
        ordered = [token_id for _, token_id in scored]
    else:
        ordered = sorted(shard_tokens, key=lambda token_id: (_stable_hash(token_id), token_id))
    return ordered[: min(max_tokens, len(ordered))]


def assign_shards_by_market(
    markets: list[dict[str, Any]],
    shard_count: int,
) -> tuple[dict[int, list[str]], dict[str, int]]:
    if shard_count <= 0:
        raise ValueError("shard_count must be >= 1")
    shards: dict[int, list[str]] = {idx: [] for idx in range(shard_count)}
    shard_sets: dict[int, set[str]] = {idx: set() for idx in range(shard_count)}
    market_shards: dict[str, int] = {}
    token_shards: dict[str, int] = {}
    errors: list[dict[str, Any]] = []

    for market in markets:
        market_id = market.get("id")
        if market_id is None:
            errors.append({"error": "missing_market_id"})
            continue
        market_key = str(market_id)
        shard_id = _stable_hash(market_key) % shard_count
        prior_market_shard = market_shards.get(market_key)
        if prior_market_shard is not None and prior_market_shard != shard_id:
            errors.append(
                {
                    "error": "market_multi_shard",
                    "market_id": market_key,
                    "first_shard": prior_market_shard,
                    "second_shard": shard_id,
                }
            )
            continue
        market_shards[market_key] = shard_id

        token_ids = market.get("token_ids") or market.get("tokenIds")
        if not isinstance(token_ids, list) or len(token_ids) != 2:
            errors.append(
                {
                    "error": "market_tokens_invalid",
                    "market_id": market_key,
                }
            )
            continue
        for token_id in token_ids:
            token_key = str(token_id)
            prior_token_shard = token_shards.get(token_key)
            if prior_token_shard is not None and prior_token_shard != shard_id:
                errors.append(
                    {
                        "error": "token_multi_shard",
                        "token_id": token_key,
                        "first_shard": prior_token_shard,
                        "second_shard": shard_id,
                        "market_id": market_key,
                    }
                )
                continue
            token_shards[token_key] = shard_id
            if token_key not in shard_sets[shard_id]:
                shard_sets[shard_id].add(token_key)
                shards[shard_id].append(token_key)

    if errors:
        raise ShardAssignmentError("invalid shard assignment", errors, shards, market_shards)
    return shards, market_shards


def split_subscribe_groups(
    token_ids: list[str],
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


def _payload_bytes(raw: Any) -> tuple[bytes, int]:
    if isinstance(raw, (bytes, bytearray, memoryview)):
        return bytes(raw), FLAG_BINARY_PAYLOAD
    if isinstance(raw, str):
        return raw.encode("utf-8"), FLAG_TEXT_PAYLOAD
    return str(raw).encode("utf-8"), FLAG_TEXT_PAYLOAD


def _iter_items(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
    elif isinstance(payload, dict):
        yield payload


def _get_token_id(item: dict[str, Any]) -> str | None:
    token_id = item.get("asset_id")
    if token_id is None:
        token_id = item.get("token_id") or item.get("tokenId") or item.get("assetId")
    if token_id is None:
        return None
    return str(token_id)


def _get_msg_type(item: dict[str, Any]) -> str:
    msg_type = item.get("type")
    if isinstance(msg_type, str):
        return msg_type.lower()
    msg_type = item.get("event_type")
    if isinstance(msg_type, str):
        return msg_type.lower()
    if "bids" in item or "asks" in item:
        return "book"
    if "price_changes" in item:
        return "price_change"
    return "unknown"


def _extract_minimal_fields(payload: Any) -> tuple[list[tuple[str, str]], dict[str, int]]:
    token_pairs: list[tuple[str, str]] = []
    msg_type_counts: dict[str, int] = {}
    for item in _iter_items(payload):
        msg_type = _get_msg_type(item)
        msg_type_counts[msg_type] = msg_type_counts.get(msg_type, 0) + 1
        token_id = _get_token_id(item)
        if token_id is not None:
            token_pairs.append((token_id, msg_type))
        price_changes = item.get("price_changes")
        if isinstance(price_changes, list):
            for change in price_changes:
                if not isinstance(change, dict):
                    continue
                change_token_id = _get_token_id(change)
                if change_token_id is None:
                    continue
                token_pairs.append((change_token_id, msg_type))
    return token_pairs, msg_type_counts


def _load_pinned_markets(
    config: Config,
) -> tuple[list[dict[str, Any]], list[str], str, str | None, list[dict[str, Any]]]:
    markets = fetch_markets(
        config.gamma_base_url,
        config.rest_timeout,
        limit=config.gamma_limit,
        max_markets=config.capture_max_markets,
    )
    selected_markets = select_active_binary_markets(
        markets, max_markets=config.capture_max_markets
    )
    universe_mode = "active-binary"
    market_regex_effective: str | None = None
    selected: list[dict[str, Any]] = []
    tokens: set[str] = set()
    for market in selected_markets:
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        token_a, token_b = str(token_ids[0]), str(token_ids[1])
        selected.append({"id": market.get("id"), "token_ids": [token_a, token_b]})
        tokens.add(token_a)
        tokens.add(token_b)
    return selected, sorted(tokens), universe_mode, market_regex_effective, selected_markets


def _coverage_pct(
    token_ids: list[str],
    last_seen: dict[str, int],
    now_ns: int,
    window_ns: int | None,
) -> float:
    if not token_ids:
        return 100.0
    seen = 0
    for token_id in token_ids:
        ts = last_seen.get(token_id)
        if ts is None:
            continue
        if window_ns is None or now_ns - ts <= window_ns:
            seen += 1
    return 100.0 * seen / len(token_ids)


def _missing_tokens(
    token_ids: list[str],
    last_seen: dict[str, int],
    now_ns: int,
    window_ns: int | None,
) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    for token_id in token_ids:
        ts = last_seen.get(token_id)
        if ts is None:
            missing.append({"token_id": token_id, "last_seen_mono_ns": None})
        elif window_ns is not None and now_ns - ts > window_ns:
            missing.append({"token_id": token_id, "last_seen_mono_ns": ts})
    return missing


def _confirm_event_from_payload(payload_bytes: bytes) -> tuple[bool, Any | None]:
    if payload_bytes in (b"PONG", b"PING"):
        return False, []
    try:
        payload = orjson.loads(payload_bytes)
    except orjson.JSONDecodeError:
        return False, None
    if isinstance(payload, list) and not payload:
        return False, payload
    return True, payload


def _ring_header_sample(shard: ShardState, max_entries: int) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for entry in list(shard.ring)[-max_entries:]:
        if len(entry) < 8:
            continue
        magic = entry[:8]
        try:
            schema_version = 1 if magic == frames_magic(1) else 2 if magic == frames_magic(2) else None
            if schema_version is None:
                continue
            header_struct = frames_header_struct(schema_version)
            header_len = header_struct.size
            if len(entry) < header_len:
                continue
            header_bytes = entry[:header_len]
            if schema_version == 1:
                magic, schema_field, flags, rx_mono_ns, payload_len, payload_crc32 = (
                    header_struct.unpack(header_bytes)
                )
                rx_wall_ns_utc = 0
            else:
                (
                    magic,
                    schema_field,
                    flags,
                    rx_mono_ns,
                    rx_wall_ns_utc,
                    payload_len,
                    payload_crc32,
                ) = header_struct.unpack(header_bytes)
            if schema_field != schema_version:
                continue
        except struct.error:
            continue
        samples.append(
            {
                "raw_header_hex": header_bytes.hex(),
                "magic": magic.decode("ascii", "ignore"),
                "schema_version": schema_version,
                "flags": flags,
                "rx_mono_ns": rx_mono_ns,
                "rx_wall_ns_utc": rx_wall_ns_utc,
                "payload_len": payload_len,
                "payload_crc32": payload_crc32,
            }
        )
    return samples


def _cap_missing_tokens(
    missing: list[dict[str, Any]],
    *,
    max_tokens: int,
) -> dict[str, Any]:
    return {
        "count": len(missing),
        "sample": missing[:max_tokens],
    }


def _build_missing_tokens_dump(
    state: CaptureState,
    reason: str,
    now_ns: int,
    *,
    include_missing_list: bool,
    window_ns: int | None,
) -> dict[str, Any]:
    per_shard: dict[str, Any] = {}
    last_seen_global: dict[str, int] = {}
    for shard in state.shards:
        last_seen_global.update(shard.last_seen)
        deadline = shard.confirm_deadline_mono_ns
        per_shard[str(shard.shard_id)] = {
            "confirmed": shard.confirmed,
            "confirm_events_seen": shard.confirm_events_seen,
            "confirm_deadline_mono_ns": deadline,
            "confirm_deadline_exceeded": deadline is not None and now_ns > deadline,
            "reconnects": shard.reconnects,
            "confirm_token_ids": shard.confirm_token_ids,
            "recent_headers": _ring_header_sample(shard, max_entries=10),
        }
    missing_global = _missing_tokens(state.pinned_tokens, last_seen_global, now_ns, window_ns)
    if include_missing_list:
        global_missing = missing_global
    else:
        global_missing = _cap_missing_tokens(missing_global, max_tokens=200)
    return {
        "reason": reason,
        "global_missing": global_missing,
        "per_shard": per_shard,
    }


def _write_runlog(run_dir: Path, record: dict[str, Any]) -> None:
    _append_ndjson(run_dir / "runlog.ndjson", record)


def _write_metrics(path: Path, record: dict[str, Any]) -> None:
    _append_ndjson(path, record)


def _write_startup_fatal(
    run: RunBootstrap,
    reason: str,
    message: str,
    *,
    extra: dict[str, Any] | None = None,
) -> None:
    _write_runlog(
        run.run_dir,
        {
            "record_type": "fatal",
            "run_id": run.run_id,
            "fatal_reason": reason,
            "fatal_message": message,
        },
    )
    payload = {
        "fatal_reason": reason,
        "fatal_message": message,
        "run_id": run.run_id,
    }
    if extra:
        payload.update(extra)
    _write_json(run.run_dir / "fatal.json", payload)


def _all_shards_confirmed(state: CaptureState) -> bool:
    return all(shard.confirmed for shard in state.shards)


async def _check_backpressure_fatal(
    state: CaptureState,
    now_ns: int,
    global_backpressure_p99: int,
) -> None:
    threshold_ns = int(state.config.capture_backpressure_fatal_ms * 1_000_000)
    if threshold_ns <= 0:
        return
    if global_backpressure_p99 > threshold_ns:
        state.backpressure_breach_count += 1
    else:
        state.backpressure_breach_count = 0
    if state.backpressure_breach_count < 3:
        return
    missing = _build_missing_tokens_dump(
        state,
        FATAL_BACKPRESSURE,
        now_ns,
        include_missing_list=False,
        window_ns=None,
    )
    missing["backpressure_ns_p99"] = global_backpressure_p99
    await _trigger_fatal(
        state,
        FATAL_BACKPRESSURE,
        "backpressure p99 exceeded threshold",
        missing_tokens=missing,
    )


async def _trigger_fatal(
    state: CaptureState,
    reason: str,
    message: str,
    *,
    first_error: dict[str, Any] | None = None,
    missing_tokens: dict[str, Any] | None = None,
) -> None:
    async with state.fatal_lock:
        if state.fatal_event.is_set():
            return
        state.fatal_event.set()
        _write_runlog(
            state.run.run_dir,
            {
                "record_type": "fatal",
                "run_id": state.run.run_id,
                "fatal_reason": reason,
                "fatal_message": message,
            },
        )
        fatal_record: dict[str, Any] = {
            "fatal_reason": reason,
            "fatal_message": message,
            "run_id": state.run.run_id,
            "recent_heartbeats": list(state.heartbeat_samples),
        }
        if first_error:
            fatal_record.update(first_error)
        _write_json(state.run.run_dir / "fatal.json", fatal_record)
        if missing_tokens:
            _write_json(state.run.run_dir / "missing_tokens.json", missing_tokens)
        for shard in state.shards:
            dump_path = state.run.run_dir / f"last_frames_shard_{shard.shard_id:02d}.bin"
            dump_path.write_bytes(b"".join(shard.ring))


async def _heartbeat_loop(state: CaptureState) -> None:
    interval_ns = int(state.config.capture_heartbeat_interval_seconds * 1_000_000_000)
    run_dir = state.run.run_dir
    metrics_global = run_dir / "metrics" / "global.ndjson"
    metrics_shard_paths = {
        shard.shard_id: run_dir / "metrics" / f"shard_{shard.shard_id:02d}.ndjson"
        for shard in state.shards
    }
    next_tick = state.run.t0_mono_ns + interval_ns
    while not state.fatal_event.is_set():
        now_ns = monotonic_ns()
        if now_ns < next_tick:
            await asyncio.sleep((next_tick - now_ns) / 1_000_000_000)
            continue
        now_ns = monotonic_ns()
        hb_wall_ns_utc = time.time_ns()
        elapsed_ns = now_ns - state.run.t0_mono_ns
        _write_runlog(
            run_dir,
            {
                "record_type": "heartbeat",
                "run_id": state.run.run_id,
                "hb_wall_ns_utc": hb_wall_ns_utc,
                "hb_mono_ns": now_ns,
            },
        )

        global_frames = 0
        global_bytes = 0
        global_write_samples: list[int] = []
        global_ingest_samples: list[int] = []
        global_backpressure_samples: list[int] = []
        global_decode_errors = 0
        global_msg_type_counts: dict[str, int] = {}
        global_reconnects = 0
        global_confirm_failures = 0
        global_tokens_seen: set[str] = set()
        global_coverage_pct = 0.0

        for shard in state.shards:
            shard_stats = shard.stats
            global_frames += shard_stats.frames
            global_bytes += shard_stats.bytes_written
            global_decode_errors += shard_stats.decode_errors
            global_reconnects += shard.reconnects
            global_confirm_failures += shard.confirm_failures
            global_tokens_seen.update(shard_stats.token_ids)
            global_write_samples.extend(shard_stats.write_durations_ns)
            global_ingest_samples.extend(shard_stats.ingest_latencies_ns)
            global_backpressure_samples.extend(shard_stats.backpressure_ns)
            for key, value in shard_stats.msg_type_counts.items():
                global_msg_type_counts[key] = global_msg_type_counts.get(key, 0) + value

            shard_coverage_pct = _coverage_pct(
                shard.token_ids,
                shard.last_seen,
                now_ns,
                None,
            )
            shard_record = {
                "record_type": "heartbeat",
                "run_id": state.run.run_id,
                "shard_id": shard.shard_id,
                "hb_wall_ns_utc": hb_wall_ns_utc,
                "hb_mono_ns": now_ns,
                "elapsed_ns": elapsed_ns,
                "frames": shard_stats.frames,
                "bytes_written": shard_stats.bytes_written,
                "msgs_per_sec": shard_stats.frames
                / max(elapsed_ns / 1_000_000_000.0, 1e-9),
                "bytes_per_sec": shard_stats.bytes_written
                / max(elapsed_ns / 1_000_000_000.0, 1e-9),
                "write_ns_p50": _quantile_from_samples(shard_stats.write_durations_ns, 50),
                "write_ns_p95": _quantile_from_samples(shard_stats.write_durations_ns, 95),
                "write_ns_p99": _quantile_from_samples(shard_stats.write_durations_ns, 99),
                "ingest_ns_p50": _quantile_from_samples(shard_stats.ingest_latencies_ns, 50),
                "ingest_ns_p95": _quantile_from_samples(shard_stats.ingest_latencies_ns, 95),
                "ingest_ns_p99": _quantile_from_samples(shard_stats.ingest_latencies_ns, 99),
                "backpressure_ns_p50": _quantile_from_samples(
                    shard_stats.backpressure_ns, 50
                ),
                "backpressure_ns_p95": _quantile_from_samples(
                    shard_stats.backpressure_ns, 95
                ),
                "backpressure_ns_p99": _quantile_from_samples(
                    shard_stats.backpressure_ns, 99
                ),
                "coverage_pct": shard_coverage_pct,
                "token_ids_seen": len(shard_stats.token_ids),
                "token_ids_assigned": len(shard.token_ids),
                "reconnects": shard.reconnects,
                "confirm_failures": shard.confirm_failures,
                "confirmed": shard.confirmed,
                "confirm_events_seen": shard.confirm_events_seen,
                "decode_errors": shard_stats.decode_errors,
                "msg_type_counts": shard_stats.msg_type_counts,
            }
            _write_metrics(metrics_shard_paths[shard.shard_id], shard_record)

        if state.pinned_tokens:
            last_seen_global: dict[str, int] = {}
            for shard in state.shards:
                last_seen_global.update(shard.last_seen)
            global_coverage_pct = _coverage_pct(
                state.pinned_tokens,
                last_seen_global,
                now_ns,
                None,
            )

        global_record = {
            "record_type": "heartbeat",
            "run_id": state.run.run_id,
            "hb_wall_ns_utc": hb_wall_ns_utc,
            "hb_mono_ns": now_ns,
            "elapsed_ns": elapsed_ns,
            "frames": global_frames,
            "bytes_written": global_bytes,
            "msgs_per_sec": global_frames / max(elapsed_ns / 1_000_000_000.0, 1e-9),
            "bytes_per_sec": global_bytes / max(elapsed_ns / 1_000_000_000.0, 1e-9),
            "write_ns_p50": _quantile_from_samples(global_write_samples, 50),
            "write_ns_p95": _quantile_from_samples(global_write_samples, 95),
            "write_ns_p99": _quantile_from_samples(global_write_samples, 99),
            "ingest_ns_p50": _quantile_from_samples(global_ingest_samples, 50),
            "ingest_ns_p95": _quantile_from_samples(global_ingest_samples, 95),
            "ingest_ns_p99": _quantile_from_samples(global_ingest_samples, 99),
            "backpressure_ns_p50": _quantile_from_samples(global_backpressure_samples, 50),
            "backpressure_ns_p95": _quantile_from_samples(global_backpressure_samples, 95),
            "backpressure_ns_p99": _quantile_from_samples(global_backpressure_samples, 99),
            "coverage_pct": global_coverage_pct,
            "token_ids_seen": len(global_tokens_seen),
            "token_ids_assigned": len(state.pinned_tokens),
            "reconnects": global_reconnects,
            "confirm_failures": global_confirm_failures,
            "decode_errors": global_decode_errors,
            "msg_type_counts": global_msg_type_counts,
        }
        _write_metrics(metrics_global, global_record)
        state.heartbeat_samples.append(global_record)

        await _check_backpressure_fatal(
            state,
            now_ns,
            global_record["backpressure_ns_p99"],
        )
        if state.fatal_event.is_set():
            break

        if state.config.min_free_disk_gb is not None:
            usage = shutil.disk_usage(run_dir)
            free_gb = usage.free / (1024**3)
            if free_gb < state.config.min_free_disk_gb:
                await _trigger_fatal(
                    state,
                    FATAL_LOW_DISK,
                    f"free disk below {state.config.min_free_disk_gb} GB",
                )
                break

        deadline_exceeded = any(
            shard.confirm_deadline_mono_ns is not None
            and now_ns > shard.confirm_deadline_mono_ns
            and not shard.confirmed
            for shard in state.shards
        )
        if deadline_exceeded:
            missing = _build_missing_tokens_dump(
                state,
                FATAL_SUBSCRIBE_CONFIRM,
                now_ns,
                include_missing_list=False,
                window_ns=None,
            )
            await _trigger_fatal(
                state,
                FATAL_SUBSCRIBE_CONFIRM,
                "confirmation deadline exceeded",
                missing_tokens=missing,
            )
            break

        next_tick = now_ns + interval_ns


def _handle_payload(
    state: CaptureState,
    shard: ShardState,
    raw: Any,
    *,
    rx_mono_ns: int,
    rx_wall_ns_utc: int,
) -> None:
    payload_bytes, flags = _payload_bytes(raw)
    write_start_ns = monotonic_ns()
    record = append_record(
        shard.frames_fh,
        shard.idx_fh,
        payload_bytes,
        rx_mono_ns,
        rx_wall_ns_utc,
        flags=flags,
        schema_version=state.config.capture_frames_schema_version,
    )
    write_end_ns = monotonic_ns()
    backpressure_ns = max(0, write_start_ns - rx_mono_ns)
    header_struct = frames_header_struct(record.schema_version)
    if record.schema_version == 1:
        header_bytes = header_struct.pack(
            frames_magic(record.schema_version),
            record.schema_version,
            record.flags,
            record.rx_mono_ns,
            record.payload_len,
            record.payload_crc32,
        )
    else:
        header_bytes = header_struct.pack(
            frames_magic(record.schema_version),
            record.schema_version,
            record.flags,
            record.rx_mono_ns,
            record.rx_wall_ns_utc,
            record.payload_len,
            record.payload_crc32,
        )
    shard.ring.append(header_bytes + record.payload)

    confirm_event, payload = _confirm_event_from_payload(payload_bytes)
    if confirm_event:
        shard.confirm_events_seen += 1
        if not shard.confirmed and (
            shard.confirm_events_seen >= state.config.capture_confirm_min_events
        ):
            shard.confirmed = True
            _write_runlog(
                state.run.run_dir,
                {
                    "record_type": "subscribe_confirm_success",
                    "run_id": state.run.run_id,
                    "shard_id": shard.shard_id,
                    "confirm_events_seen": shard.confirm_events_seen,
                    "confirm_deadline_mono_ns": shard.confirm_deadline_mono_ns,
                },
            )

    if payload is None:
        shard.stats.decode_errors += 1
        shard.stats.record(
            record.payload_len,
            frames_header_len(record.schema_version),
            idx_entry_len(record.schema_version),
            write_end_ns - write_start_ns,
            write_end_ns - rx_mono_ns,
            backpressure_ns,
            [],
            {},
            state.config.capture_metrics_max_samples,
        )
        return

    token_pairs, msg_type_counts = _extract_minimal_fields(payload)
    token_ids = [token_id for token_id, _ in token_pairs]
    shard.stats.record(
        record.payload_len,
        frames_header_len(record.schema_version),
        idx_entry_len(record.schema_version),
        write_end_ns - write_start_ns,
        write_end_ns - rx_mono_ns,
        backpressure_ns,
        token_ids,
        msg_type_counts,
        state.config.capture_metrics_max_samples,
    )
    for token_id in token_ids:
        shard.last_seen[token_id] = rx_mono_ns


async def _run_shard(state: CaptureState, shard: ShardState) -> None:
    run_dir = state.run.run_dir
    if not shard.token_ids:
        await state.fatal_event.wait()
        return
    variant_index = 0
    while not state.fatal_event.is_set():
        variant = SUBSCRIBE_VARIANTS[variant_index % len(SUBSCRIBE_VARIANTS)]
        variant_index += 1
        try:
            async with websockets.connect(state.config.clob_ws_url) as ws:
                shard.confirmed = False
                shard.confirm_events_seen = 0
                shard.confirm_deadline_mono_ns = (
                    monotonic_ns()
                    + int(state.config.capture_confirm_timeout_seconds * 1_000_000_000)
                )

                async def _check_confirm_deadline(now_ns: int) -> None:
                    if shard.confirmed or shard.confirm_deadline_mono_ns is None:
                        return
                    if now_ns < shard.confirm_deadline_mono_ns:
                        return
                    shard.confirm_failures += 1
                    _write_runlog(
                        run_dir,
                        {
                            "record_type": "subscribe_confirm_fail",
                            "run_id": state.run.run_id,
                            "shard_id": shard.shard_id,
                            "variant": variant,
                            "confirm_events_seen": shard.confirm_events_seen,
                        },
                    )
                    if shard.confirm_failures >= state.config.capture_confirm_max_failures:
                        await _trigger_fatal(
                            state,
                            FATAL_SUBSCRIBE_CONFIRM,
                            "subscription confirmation failed",
                        )
                    raise TimeoutError("subscribe confirmation timeout")

                for group_index, token_group in enumerate(shard.groups):
                    payload = build_subscribe_payload(variant, token_group)
                    payload_bytes = orjson.dumps(payload)
                    await ws.send(payload_bytes.decode("utf-8"))
                    _write_runlog(
                        run_dir,
                        {
                            "record_type": "subscribe_attempt",
                            "run_id": state.run.run_id,
                            "shard_id": shard.shard_id,
                            "variant": variant,
                            "group_index": group_index,
                            "token_count": len(token_group),
                            "payload_bytes": len(payload_bytes),
                        },
                    )

                while not state.fatal_event.is_set():
                    await _check_confirm_deadline(monotonic_ns())
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        await _check_confirm_deadline(monotonic_ns())
                        continue

                    rx_mono_ns = monotonic_ns()
                    rx_wall_ns_utc = time.time_ns()
                    await _check_confirm_deadline(monotonic_ns())
                    _handle_payload(
                        state,
                        shard,
                        raw,
                        rx_mono_ns=rx_mono_ns,
                        rx_wall_ns_utc=rx_wall_ns_utc,
                    )
        except Exception as exc:
            shard.reconnects += 1
            _write_runlog(
                run_dir,
                {
                    "record_type": "reconnect",
                    "run_id": state.run.run_id,
                    "shard_id": shard.shard_id,
                    "reason": type(exc).__name__,
                    "reconnects": shard.reconnects,
                },
            )
            if shard.reconnects >= state.config.ws_reconnect_max:
                await _trigger_fatal(
                    state,
                    FATAL_RECONNECT_STORM,
                    "reconnect limit exceeded",
                )
                break
            await asyncio.sleep(state.config.ws_reconnect_backoff_seconds)


async def _capture_online_async(config: Config, run_id: str | None = None) -> int:
    if config.ws_shards <= 0:
        raise ValueError("ws_shards must be >= 1")
    markets, pinned_tokens, universe_mode, _, selected_markets = _load_pinned_markets(
        config
    )
    if not pinned_tokens:
        raise RuntimeError("no pinned tokens available for capture")
    assignment_errors: list[dict[str, Any]] | None = None
    market_shards: dict[str, int] = {}
    try:
        shard_map, market_shards = assign_shards_by_market(markets, config.ws_shards)
    except ShardAssignmentError as exc:
        shard_map = exc.shard_map
        market_shards = exc.market_shards
        assignment_errors = exc.errors
    scores = _token_score_map(selected_markets)
    confirm_token_map = {
        shard_id: _select_confirm_tokens(
            tokens, scores, config.capture_confirm_tokens_per_shard
        )
        for shard_id, tokens in shard_map.items()
    }
    shard_groups: dict[int, list[list[str]]] = {}
    for shard_id, tokens in shard_map.items():
        shard_groups[shard_id] = split_subscribe_groups(
            tokens,
            config.ws_subscribe_max_tokens,
            config.ws_subscribe_max_bytes,
            "A",
        )

    manifest_extra = {
        "capture_schema_version": config.capture_frames_schema_version,
        "payload_source": "text",
        "payload_encoder": "utf-8",
        "universe_mode": universe_mode,
        "monotonic_ns_source": "perf_counter_ns",
        "git_commit": _read_git_commit(),
        "environment": _environment_metadata(),
        "config": _config_snapshot(config),
        "pinned_tokens": pinned_tokens,
        "pinned_markets": markets,
        "market_shards": market_shards,
        "shards": {
            "count": config.ws_shards,
            "assignments": {
                str(shard_id): {"token_ids": tokens, "groups": shard_groups[shard_id]}
                for shard_id, tokens in shard_map.items()
            },
        },
        "subscribe_caps": {
            "max_tokens": config.ws_subscribe_max_tokens,
            "max_bytes": config.ws_subscribe_max_bytes,
        },
    }

    run = bootstrap_run(config, run_id=run_id, manifest_extra=manifest_extra)
    run_dir = run.run_dir
    print(f"capture run dir: {run_dir}")
    if assignment_errors:
        _write_startup_fatal(
            run,
            FATAL_SHARD_ASSIGNMENT,
            "invalid shard assignment",
            extra={"shard_assignment_errors": assignment_errors},
        )
        return 1

    precision = _monotonic_precision_stats(sample_count=200)
    _write_runlog(
        run_dir,
        {
            "record_type": "monotonic_precision",
            "run_id": run.run_id,
            **precision,
        },
    )
    if not precision.get("ok", False):
        _write_startup_fatal(
            run,
            FATAL_MONO_QUANTIZED,
            "monotonic clock precision insufficient",
            extra={"monotonic_precision": precision},
        )
        return 1
    shards: list[ShardState] = []
    for shard_id, token_ids in shard_map.items():
        frames_path = run_dir / "capture" / f"shard_{shard_id:02d}.frames"
        idx_path = run_dir / "capture" / f"shard_{shard_id:02d}.idx"
        frames_fh = frames_path.open("ab")
        idx_fh = idx_path.open("ab")
        ring = deque(maxlen=config.capture_ring_buffer_frames)
        shards.append(
            ShardState(
                shard_id=shard_id,
                token_ids=token_ids,
                groups=shard_groups[shard_id],
                frames_path=frames_path,
                idx_path=idx_path,
                frames_fh=frames_fh,
                idx_fh=idx_fh,
                ring=ring,
                confirm_token_ids=confirm_token_map.get(shard_id, []),
            )
        )

    state = CaptureState(run=run, config=config, shards=shards, pinned_tokens=pinned_tokens)
    _write_runlog(
        run_dir,
        {
            "record_type": "capture_start",
            "run_id": run.run_id,
            "shards": config.ws_shards,
            "token_count": len(pinned_tokens),
        },
    )

    tasks = [asyncio.create_task(_run_shard(state, shard)) for shard in shards]
    heartbeat_task = asyncio.create_task(_heartbeat_loop(state))
    tasks.append(heartbeat_task)

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    if not state.fatal_event.is_set():
        for task in done:
            exc = task.exception()
            if exc is not None:
                await _trigger_fatal(
                    state,
                    FATAL_INTERNAL,
                    f"task failed: {type(exc).__name__}",
                )
                break
    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    for shard in shards:
        shard.frames_fh.flush()
        shard.idx_fh.flush()
        os.fsync(shard.frames_fh.fileno())
        os.fsync(shard.idx_fh.fileno())
        shard.frames_fh.close()
        shard.idx_fh.close()

    return 1 if state.fatal_event.is_set() else 0


def run_capture_online(config: Config, run_id: str | None = None) -> int:
    try:
        return asyncio.run(_capture_online_async(config, run_id=run_id))
    except KeyboardInterrupt:
        return 0
