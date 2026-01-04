# Phase 2 Readiness Contract

Phase 2 replay and fill simulation relies on the Phase 1 capture dataset being complete, reproducible, and timestamped.

## Required Fields

Per-frame fields (in `.frames` and `.idx`):
- `rx_mono_ns` (u64): monotonic receive timestamp, used for deterministic ordering and replay timing.
- `rx_wall_ns_utc` (u64): UTC wall-clock timestamp, used for alignment with external systems/logs.
- `payload_bytes` (raw): exact WS payload bytes as received, no normalization.

Manifest fields (`manifest.json`):
- `capture_schema_version`: schema version for frame and idx decoding.
- `config`: full config snapshot used for the run.
- `pinned_tokens`: token universe for the run.
- `pinned_markets`: market metadata for the run.
- `shards.assignments`: shard -> token_ids and subscribe groups.
- `environment`: python version, platform, hostname.
- `git_commit`: git SHA when available, else `unknown`.

## Phase 2 Latency Decomposition

To decompose end-to-end latency during replay and simulated fills:
1) Use `rx_mono_ns` to schedule replay events and preserve ordering within each shard.
2) Use `rx_wall_ns_utc` to align replayed frames to external logs (order placement, fills).
3) Combine capture times with Phase 2 order placement timestamps to compute:
   - recv_to_place = place_mono_ns - rx_mono_ns
   - recv_to_fill = fill_mono_ns - rx_mono_ns
   - wall_clock_skew = place_wall_ns_utc - rx_wall_ns_utc

## Determinism Guarantees

- `rx_mono_ns` ordering is the authoritative sequence per shard.
- `rx_wall_ns_utc` is captured per frame to anchor replay to UTC time.
- `payload_bytes` remain untouched to allow full L2 reconstruction in Phase 2.
