from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .capture_format import read_idx


@dataclass(frozen=True)
class InspectSummary:
    run_dir: Path
    payload: dict[str, Any]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    return records


def inspect_run(run_dir: Path) -> InspectSummary:
    run_dir = run_dir.resolve()
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(str(manifest_path))
    manifest = _load_json(manifest_path)

    capture_dir = run_dir / "capture"
    idx_paths = sorted(capture_dir.glob("*.idx"))
    shard_summaries: dict[str, Any] = {}
    total_records = 0
    total_frames_bytes = 0
    total_idx_bytes = 0
    for idx_path in idx_paths:
        shard_name = idx_path.stem
        frames_path = idx_path.with_suffix(".frames")
        idx_records = read_idx(idx_path)
        frames_bytes = frames_path.stat().st_size if frames_path.exists() else 0
        idx_bytes = idx_path.stat().st_size
        total_records += len(idx_records)
        total_frames_bytes += frames_bytes
        total_idx_bytes += idx_bytes
        first_rx = idx_records[0].rx_mono_ns if idx_records else None
        last_rx = idx_records[-1].rx_mono_ns if idx_records else None
        shard_summaries[shard_name] = {
            "frames_path": str(frames_path),
            "idx_path": str(idx_path),
            "records": len(idx_records),
            "frames_bytes": frames_bytes,
            "idx_bytes": idx_bytes,
            "rx_mono_ns_first": first_rx,
            "rx_mono_ns_last": last_rx,
        }

    metrics_dir = run_dir / "metrics"
    global_metrics_path = metrics_dir / "global.ndjson"
    global_records = _read_ndjson(global_metrics_path)
    global_last = global_records[-1] if global_records else None

    shard_metrics: dict[str, Any] = {}
    for metrics_path in sorted(metrics_dir.glob("shard_*.ndjson")):
        records = _read_ndjson(metrics_path)
        shard_metrics[metrics_path.stem] = {
            "count": len(records),
            "last": records[-1] if records else None,
        }

    summary = {
        "run_dir": str(run_dir),
        "run_id": manifest.get("run_id"),
        "manifest_version": manifest.get("manifest_version"),
        "capture_schema_version": manifest.get("capture_schema_version"),
        "payload_source": manifest.get("payload_source"),
        "shards": {
            "count": len(shard_summaries),
            "records": total_records,
            "frames_bytes": total_frames_bytes,
            "idx_bytes": total_idx_bytes,
            "by_shard": shard_summaries,
        },
        "metrics": {
            "global": {
                "path": str(global_metrics_path),
                "count": len(global_records),
                "last": global_last,
            },
            "shards": shard_metrics,
        },
    }
    return InspectSummary(run_dir, summary)
