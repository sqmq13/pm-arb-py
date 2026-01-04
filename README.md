# pm-arb-py - Phase 1 Capture and Scanner

Phase-1 data collection and measurement only. This repository does not place orders, sign transactions, manage wallets, or bypass restrictions. Geoblock checks are detection-only and may fail.

## Phase 1 North Star (Capture)

- Capture raw Polymarket CLOB WS frames (full L2 when present) for a large binary-market universe.
- Timestamp each received frame with `rx_mono_ns` (monotonic receive time).
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
- `pm_arb capture` (online capture; requires network access)
- `pm_arb capture --offline` (fixtures-only capture)
- `pm_arb capture-verify` (verifies `.frames` and optional `.idx`; add `--summary-only` for totals-only JSON)
- `pm_arb capture-bench --offline` (bench fixtures; prints msgs/sec, bytes/sec, p99 write/ingest)
- `pm_arb capture-inspect --run-dir <path>` (summarize manifest/metrics/idx)
- `pm_arb capture-slice --run-dir <path> --start-mono-ns <ns> --end-mono-ns <ns>` (time window)
- `pm_arb capture-slice --run-dir <path> --start-offset <bytes> --end-offset <bytes>` (offset window)

All commands exit non-zero on failure.

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

Frames are binary and preserve WS frame boundaries:
- Path: `data/runs/<run_id>/capture/shard_<NN>.frames`
- Layout (little-endian):
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
  - offset_frames: u64
  - rx_mono_ns: u64
  - payload_len: u32
  - payload_crc32: u32

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

## Troubleshooting

- Coverage low or missing tokens: inspect `metrics/*.ndjson`, verify `missing_tokens.json` on fatal, and check shard assignment in `manifest.json`.
- Reconnect loops: look for repeated `reconnect` events in `runlog.ndjson`.
- Disk stalls: ensure free disk is above `min_free_disk_gb`; low disk triggers a fatal.
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
