from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .config import Config

MANIFEST_VERSION = 1


class RunBootstrap:
    def __init__(self, run_id: str, run_dir: Path, t0_wall_ns_utc: int, t0_mono_ns: int):
        self.run_id = run_id
        self.run_dir = run_dir
        self.t0_wall_ns_utc = t0_wall_ns_utc
        self.t0_mono_ns = t0_mono_ns


def _default_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = uuid.uuid4().hex[:8]
    return f"{stamp}-{suffix}"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    path.write_text(data + "\n", encoding="utf-8")


def _append_ndjson(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=True, separators=(",", ":"))
    with path.open("ab") as handle:
        handle.write(line.encode("utf-8") + b"\n")


def bootstrap_run(
    config: Config,
    run_id: str | None = None,
    manifest_extra: dict | None = None,
) -> RunBootstrap:
    run_id = run_id or _default_run_id()
    run_dir = Path(config.data_dir) / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "capture").mkdir()
    (run_dir / "metrics").mkdir()

    t0_wall_ns_utc = time.time_ns()
    t0_mono_ns = time.monotonic_ns()
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "run_id": run_id,
        "t0_wall_ns_utc": t0_wall_ns_utc,
        "t0_mono_ns": t0_mono_ns,
    }
    if manifest_extra:
        manifest.update(manifest_extra)
    _write_json(run_dir / "manifest.json", manifest)

    run_start = {
        "record_type": "run_start",
        "run_id": run_id,
        "ts_wall_ns_utc": t0_wall_ns_utc,
        "ts_mono_ns": t0_mono_ns,
    }
    _append_ndjson(run_dir / "runlog.ndjson", run_start)
    return RunBootstrap(run_id, run_dir, t0_wall_ns_utc, t0_mono_ns)
