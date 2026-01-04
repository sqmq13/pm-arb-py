# pm-arb-py - Phase 1 Capture and Scanner

Phase-1 data collection and measurement only. This repository does not place orders, sign transactions, manage wallets, or bypass restrictions. Geoblock checks are detection-only and may fail.

## Phase 1 North Star (Capture)

- Capture raw Polymarket CLOB WS frames (full L2 when present) for a large binary-market universe.
- Timestamp each received frame with `rx_mono_ns` (monotonic) and `rx_wall_ns_utc` (UTC wall clock), stored as ns integers.
- Provide wall-clock anchors in `manifest.json` (`t0_wall_ns_utc`, `t0_mono_ns`) and runlog heartbeats (`hb_wall_ns_utc`, `hb_mono_ns`).
- Keep capture records raw (no orderbook building, no normalization, no Decimal parsing).
- Emit reproducible run bundles with manifest + pinned token set + shard mapping.

The existing `scan`/`report` flow remains for Strategy-A measurement, but Phase 1 capture is the primary deliverable.

## Setup

Make-based workflow:
```bash
make setup
make test
```

If you do not have `make`:
```bash
uv venv .venv
uv pip install -r requirements.lock
uv pip install -e .
uv run python -m pytest
```

## CLI Usage

- `pm_arb scan` (online) / `pm_arb scan --offline`
- `pm_arb report`
- `pm_arb contract-test` (online/offline)
- `pm_arb discover`
- `pm_arb discover --universe active-binary` (show capture universe)
- `pm_arb capture` (online capture; requires network access)
- `pm_arb capture --offline` (fixtures-only capture)
- `pm_arb capture-verify` (verifies `.frames` and optional `.idx`; add `--summary-only` for totals-only JSON)
- `pm_arb capture-bench --offline` (bench fixtures; prints msgs/sec, bytes/sec, p99 write/ingest)
- `pm_arb capture-inspect --run-dir <path>` (summarize manifest/metrics/idx)
- `pm_arb capture-slice --run-dir <path> --start-mono-ns <ns> --end-mono-ns <ns>` (time window)
- `pm_arb capture-slice --run-dir <path> --start-offset <bytes> --end-offset <bytes>` (offset window)

All commands exit non-zero on failure.

Universe selection:
- Default capture pins the active-binary YES/NO universe (ignores regex filters).
- Opt-in filtered capture by setting `capture_use_market_filters=true`.
- Use `pm_arb discover --universe active-binary` to inspect the capture universe.

## Output Layout

Legacy scan/report outputs:
- NDJSON logs: `data/logs/YYYY-MM-DD/events.ndjson` (+ rotated)
- WS schema capture: `data/debug/ws_samples.jsonl` (first N messages)
- Reports: `data/reports/summary.json` and `data/reports/summary.csv`

Capture run bundle (all files under `data/runs/<run_id>/`):
- `manifest.json`
- `runlog.ndjson`
- `capture/shard_<NN>.frames`
- `capture/shard_<NN>.idx`
- `metrics/global.ndjson`
- `metrics/shard_<NN>.ndjson`
On fatal:
- `fatal.json`
- `missing_tokens.json` (if coverage/subscription issues)
- `last_frames_shard_<NN>.bin`

## Data Formats and Schema Versioning

Frames are binary and preserve WS frame boundaries (schema v2 default; v1 readable):
- Path: `data/runs/<run_id>/capture/shard_<NN>.frames`
- Layout (little-endian):
  - v2:
    - magic: `b"PMCAPv02"` (8 bytes)
    - schema_version: u16
    - flags: u16
    - rx_mono_ns: u64
    - rx_wall_ns_utc: u64
    - payload_len: u32
    - payload_crc32: u32
    - payload_bytes: payload_len
  - v1 (legacy):
    - magic: `b"PMCAPv01"` (8 bytes)
    - schema_version: u16
    - flags: u16
    - rx_mono_ns: u64
    - payload_len: u32
    - payload_crc32: u32
    - payload_bytes: payload_len

Index records align 1:1 with frames:
- Path: `data/runs/<run_id>/capture/shard_<NN>.idx`
- Layout (little-endian):
  - v2:
    - offset_frames: u64
    - rx_mono_ns: u64
    - rx_wall_ns_utc: u64
    - payload_len: u32
    - payload_crc32: u32
  - v1 (legacy):
    - offset_frames: u64
    - rx_mono_ns: u64
    - payload_len: u32
    - payload_crc32: u32

To force v1 writes for a run, set `capture_frames_schema_version=1` (CLI or env).

## Capture Dataset Contract

Capture run bundle (all files under `data/runs/<run_id>/`):
- `manifest.json` includes schema version, config snapshot, pinned universe, shard map, and environment metadata.
- `runlog.ndjson` contains run lifecycle and heartbeat records.
- `capture/shard_<NN>.frames` stores raw WS frame payloads with per-frame `rx_mono_ns` and `rx_wall_ns_utc`.
- `capture/shard_<NN>.idx` provides 1:1 offsets + timestamps for fast slicing.
- `metrics/*.ndjson` contain ingest/write latency metrics and per-shard counters.

Contract invariants:
- Payload bytes are written exactly as received (no normalization, no truncation).
- Both monotonic and wall-clock timestamps are stored per frame as ns integers.
- Schema v1 is always readable; new runs default to v2.
- Universe selection is explicit in `manifest.json` (`universe_mode`, `capture_use_market_filters`, `market_regex_effective`).

Verification and tools:
```bash
pm_arb capture-verify --run-dir data/runs/<run_id>
pm_arb capture-inspect --run-dir data/runs/<run_id>
pm_arb capture-slice --run-dir data/runs/<run_id> --start-mono-ns <ns> --end-mono-ns <ns>
```

## Phase 2 Readiness

Phase 2 replay depends on per-frame timestamps and raw payload bytes:
- `rx_mono_ns` for deterministic ordering and replay scheduling.
- `rx_wall_ns_utc` for aligning captures to external clocks.
- `payload_bytes` for L2 reconstruction without Phase 1 normalization.
- `manifest.json` shard assignments for deterministic replay partitioning.

See `docs/PHASE2_READY.md` for the exact fields and latency decomposition guidance.

## Verification and Debugging

Verify a run bundle:
```bash
pm_arb capture-verify --run-dir data/runs/<run_id>
```

Summarize run health without decoding payloads:
```bash
pm_arb capture-inspect --run-dir data/runs/<run_id>
```

Slice a window into a self-contained bundle:
```bash
pm_arb capture-slice --run-dir data/runs/<run_id> --start-offset <bytes> --end-offset <bytes>
```

## Bench Workflow

Run an offline ingest benchmark on fixtures:
```bash
pm_arb capture-bench --offline --fixtures-dir testdata/fixtures
```
Scale the workload by repeating fixtures:
```bash
pm_arb capture-bench --offline --fixtures-dir testdata/fixtures --multiplier 200
```

## Troubleshooting

- Coverage low or missing tokens: inspect `metrics/*.ndjson`, verify `missing_tokens.json` on fatal, and check shard assignment in `manifest.json`.
- Reconnect loops: look for repeated `reconnect` events in `runlog.ndjson`.
- Disk stalls: ensure free disk is above `min_free_disk_gb`; low disk triggers a fatal.
- Backpressure stalls: watch `backpressure_ns_p99` in `metrics/*.ndjson` and tune `capture_backpressure_fatal_ms` if needed.
- Corruption/truncation: run `capture-verify` to locate the first bad offset; truncated files often indicate an unclean shutdown.

## Configuration

All config flags have env overrides (ENV wins). Example:
```bash
PM_ARB_SIZES="10,50,100" PM_ARB_BUFFER_PER_SHARE="0.004" pm_arb scan --offline
```

## Guardrails

- Never write per-tick WS messages to NDJSON (event-level only).
- Disk guardrail: exit non-zero if free disk is below `min_free_disk_gb`.
- Taint propagation on integrity failures; windows must not look good.
- Fixed-point integer math for sweep/cost logic.

See `AGENTS.md` for the authoritative Phase-1 scope and acceptance criteria.
