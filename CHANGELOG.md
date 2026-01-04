# Changelog

## Unreleased
- Added Stage 0 baseline inventory documentation.
- Default capture universe now uses active-binary selection with opt-in regex filtering to avoid regex drift.
- Added `discover --universe active-binary` plus manifest fields recording universe mode for traceability.
- Confirmation now uses decodable non-keepalive payloads and per-shard confirm sets; coverage gates are optional and disabled by default.
- Added backpressure p99 fatal gate with enhanced missing_tokens.json forensic detail.
- Capture schema v2 now records per-frame `rx_wall_ns_utc` alongside `rx_mono_ns`, with v1 read/verify compatibility and a v1 write switch.
- Added capture-bench `--multiplier` for scaled offline benchmarking plus a regression test for bench metrics.
- Documented Capture Dataset Contract and Phase 2 readiness requirements.
