# Baseline Inventory (Stage 0)

## Entrypoints and Commands
- Makefile targets:
  - setup: uv venv .venv; uv pip install -r requirements.lock; uv pip install -e .
  - test: uv run python -m pytest
  - run: uv run pm_arb scan
  - report: uv run pm_arb report
  - contract-test: uv run pm_arb contract-test
  - discover: uv run pm_arb discover
  - run-offline: uv run pm_arb scan --offline --fixtures-dir testdata/fixtures
  - report-offline: uv run pm_arb report
- CLI subcommands (pm_arb/cli.py):
  - scan, report, contract-test, discover, capture, capture-verify, capture-bench, capture-inspect, capture-slice

## Required Commands
- make test
- uv run pm_arb capture
- uv run pm_arb discover
- uv run pm_arb capture-verify
- uv run pm_arb capture-bench --offline

## Online Capture Call Chain
run_capture_online -> _capture_online_async -> _run_shard -> append_record

## Capture Data Format
- Frames:
  - FRAMES_MAGIC = "PMCAPv01"
  - FRAMES_SCHEMA_VERSION = 1
  - FRAMES_HEADER_STRUCT "<8sHHQII": magic(8s), schema_version(uint16), flags(uint16), rx_mono_ns(uint64), payload_len(uint32), payload_crc32(uint32)
- Index:
  - IDX_STRUCT "<QQII": offset_frames(uint64), rx_mono_ns(uint64), payload_len(uint32), payload_crc32(uint32)
- Flags:
  - FLAG_TEXT_PAYLOAD = 1
  - FLAG_BINARY_PAYLOAD = 2
  - FLAG_DECODED_PAYLOAD = 4
- Manifest (pm_arb/capture.py):
  - manifest_version = 1
  - run_id
  - t0_wall_ns_utc
  - t0_mono_ns
  - manifest_extra is appended by caller (e.g., capture_online adds capture_schema_version, config, pinned universe)

## Coverage Gate Location
- Warmup/sustained coverage logic: pm_arb/capture_online.py:_heartbeat_loop()

## Tests
- Command: make test (uv run python -m pytest)
- Test files:
  - tests/test_capture_format.py
  - tests/test_capture_inspect.py
  - tests/test_capture_offline.py
  - tests/test_capture_online_utils.py
  - tests/test_capture_slice.py
  - tests/test_cli.py
  - tests/test_config.py
  - tests/test_discover.py
  - tests/test_engine_offline.py
  - tests/test_fixed.py
  - tests/test_gamma.py
  - tests/test_reconcile.py
  - tests/test_sweep.py
  - tests/test_ws_parser.py
