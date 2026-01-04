# Purge Baseline (Stage 0)

## Current CLI Commands
- scan: legacy scan engine (online/offline) with orderbook parsing and strategy metrics.
- report: generates scan-era reports.
- contract-test: scan-era ws/rest contract checks + reconcile/sweep logic.
- discover: prints market candidates; supports regex matching and active-binary universe inspection.
- capture: online/offline frame capture into run bundles.
- capture-verify: validates .frames and .idx integrity.
- capture-bench: offline bench with throughput/latency stats.
- capture-inspect: summarizes run manifest/metrics/index.
- capture-slice: slices a run by mono timestamp or offset window.

## CLI Imports (Modules)
- pm_arb.book: scan-era WS parse + OrderBook.
- pm_arb.clob_rest: REST client for scan/contract-test.
- pm_arb.clob_ws: subscribe helpers + wait_for_decodable_book.
- pm_arb.config: combined scan + capture config.
- pm_arb.capture: run bootstrap + manifest helpers.
- pm_arb.capture_offline: offline capture/bench.
- pm_arb.capture_inspect: run inspection.
- pm_arb.capture_online: online capture engine.
- pm_arb.capture_format: frame/idx encoding + verify.
- pm_arb.capture_slice: slicing tool.
- pm_arb.engine: scan engine.
- pm_arb.gamma: market fetch + token parsing.
- pm_arb.market_select: market filtering + active-binary selector.
- pm_arb.reconcile: scan-era reconcile logic.
- pm_arb.report: scan-era report generation.
- pm_arb.sweep: scan-era sweep cost logic.

## Tests Not Directly About Capture/Gamma/Market Selection/Capture Tooling
- tests/test_ws_parser.py: scan-era WS book parser.
- tests/test_sweep.py: sweep cost logic.
- tests/test_reconcile.py: reconcile behavior.
- tests/test_fixed.py: fixed-point math helpers.
- tests/test_engine_offline.py: scan engine offline path.
- tests/test_cli.py: includes scan/report/contract-test CLI behaviors.
- tests/test_discover.py: discover CLI (non-capture inspection).

## Dependencies (pyproject.toml)
- orjson: JSON encoding/decoding (capture + scan).
- requests: Gamma REST + scan-era REST usage.
- uvloop: optional event loop (capture + scan).
- websockets: WS client for capture + scan.
- websocket-client: used only by pm_arb/tools/base_scan.py audit mode (legacy-only).
