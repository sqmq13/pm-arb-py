# AGENTS.md (authoritative)

This file is the working contract for coding agents operating on this repository. If a rule here conflicts with a comment, doc, or old instruction elsewhere in the repo, this file wins.

The program is multi-phase:
- Phase 1: Capture / ingest (correct, reproducible, high-throughput dataset).
- Phase 2: Offline processing + replay + paper trading (no real money).
- Phase 3: Live execution (real money).

Unless a task explicitly says otherwise, assume Phase 1 only.

---

## 1) Repo entrypoints and contracts you MUST NOT break

CLI entrypoint:
- `pyproject.toml` defines `pm_arb = "pm_arb.cli:main"`.
- CLI implementation is `pm_arb/cli.py` (argparse subcommands).

Existing CLI commands that MUST keep working:
- `pm_arb scan`
- `pm_arb report`
- `pm_arb contract-test`
- `pm_arb discover`

Test harness:
- `Makefile` defines:
  - `make setup` (creates `.venv`, installs deps, installs package editable)
  - `make test` (runs `uv run python -m pytest`)
- The `tests/` directory contains 17 tests. Do not reduce coverage.

Offline fixtures:
- `testdata/fixtures/` is used by `pm_arb scan --offline` and `pm_arb contract-test --offline`.

Logging/output contract:
- `pm_arb scan` writes NDJSON to `./data/logs/<YYYY-MM-DD>/events.ndjson` by default.
- This schema is produced by `pm_arb/engine.py` (see `SCHEMA_VERSION` and `Engine._record()`).
- Do not change scan output semantics unless explicitly authorized.

---

## 2) Required workflow (anti-ambiguity fences)

Before editing any code, you MUST output four sections:

A) FILES TO EDIT
- List exact paths you will modify/add/remove.
- One reason per path.

B) PLAN
- Ordered steps.
- Each step includes how it will be verified (test, invariant, benchmark).

C) BEHAVIOR CHANGES
- This section MUST be empty unless the task explicitly permits changing behavior.
- If non-empty, specify exact CLI/format changes.

D) ROLLBACK PLAN
- File-level rollback steps.

After edits, you MUST provide:
- The exact command(s) you ran for verification (at minimum: `make test`).
- The result summary (pass/fail + relevant output).
- Any new tests added and what they assert.

Do not “clean up” unrelated code. Keep diffs tight.

---

## 3) Phase 1 capture engine rules (what Phase 1 is and is not)

Phase 1 goal:
- Produce a correct, reproducible, high-throughput dataset of Polymarket CLOB WS book updates for ~2000 binary markets (~4000 tokens).

Phase 1 MUST NOT include:
- Any trading/execution behavior (orders, signing, keys, wallets).
- Any arbitrage evaluation logic (edge computation, windowing, sweep-cost decision logic).
- Any transformations that irreversibly discard WS data.

Definition: “full L2” for Phase 1 capture
- Each captured update MUST preserve both `bids` and `asks` if they are present in the WS payload.
- Capture MUST NOT apply `top_k`, MUST NOT sort/merge levels, and MUST NOT do Decimal math in the capture hot path.

Repo-specific warning:
- `pm_arb/book.py::parse_ws_message()` and `pm_arb/book.py::OrderBook` currently parse and store asks only, and apply `top_k` via `normalize_asks()`.
- Phase 1 capture MUST NOT use `parse_ws_message()` / `OrderBook` in the capture hot path.

If you add a capture command:
- Implement it as a new argparse subcommand in `pm_arb/cli.py` following the existing pattern.
- Keep `pm_arb/engine.py::Engine.scan_online()` behavior unchanged unless explicitly authorized.

---

## 4) Performance and correctness are measurable (no vague wins)

Any “performance improvement” claim MUST reference at least one metric that can be measured in this repo:
- msgs/sec ingested
- bytes/sec written
- p99 recv→append time (end-to-end ingest latency)
- p99 write duration
- CPU% and RSS memory
- coverage % (tokens observed at least once within warmup)
- dropped frames (count and rate)
- reconnect rate

Hot-path prohibitions unless explicitly justified with measured impact:
- per-record file open/close (e.g., `with path.open(...)` per event)
- per-record disk free checks (e.g., `shutil.disk_usage(...)` per event)
- per-record wall-clock timestamps (e.g., `datetime.now(...)` per event)
- unbounded queues between WS recv and disk write
- Decimal parsing in per-message loops (`pm_arb/fixed.py` uses `Decimal` and sets context per call)
- full sorts/merges of book levels during capture (`pm_arb/book.py::normalize_asks()`)

---

## 5) Failure handling policy (fail fast with artifacts)

Phase 1 capture MUST prefer “fail fast with artifacts” over “continue in a bad state”.

If you implement capture, fatal conditions MUST be explicit (examples):
- low disk (see `pm_arb/engine.py::EventLogger._check_disk()` for existing behavior)
- coverage warmup gate failure (token set not observed)
- any dropped frames if drop policy is “no drops”
- sustained write-latency breach (p99 above threshold for N consecutive heartbeats)
- reconnect storms above a defined cap
- corrupted/truncated capture files detected by a verifier tool (if you introduce a binary format)

On fatal exit, write forensic artifacts (paths and filenames must be stable and documented):
- a JSON summary of counters and failure reason
- a bounded missing-token dump (if relevant)
- a bounded sample of recent frames/messages (if relevant)

Do not hide failure behind “tainted” state for Phase 1 capture. The run must fail.

---

## 6) Reproducibility requirements (Phase 2 depends on this)

Phase 1 capture output MUST be reproducible:
- The run MUST emit a manifest before opening WS sockets.
- The manifest MUST include:
  - a schema version for capture files
  - the git commit hash if available, else “unknown”
  - full config snapshot (all `pm_arb/config.py::Config` fields used)
  - the pinned token set and market mapping used for the run
  - shard mapping if sharding is used
  - environment metadata needed to reproduce (python version, OS, hostname)

Do not rely on wall-clock timestamps per frame for reproducibility. Use monotonic receive time for ordering, and store wall-clock in the manifest and periodic heartbeats only.

---

## 7) README.md is part of the deliverable

Agents MUST keep `README.md` accurate and current.

If you add a command, format, or operational behavior, you MUST update `README.md` in the same change.

Minimum README sections to maintain:
- Setup (the repo’s actual `make setup` / `make test` workflow)
- CLI usage (`pm_arb scan/report/contract-test/discover`, plus any new commands you add)
- Output layout (`data/` structure, what files mean, retention/rotation)
- Data formats and schema versioning
- Verification (how to verify capture outputs if a verifier exists)
- Troubleshooting (coverage issues, reconnect loops, disk stalls, corruption signals)

If README is not updated when contracts change, the work is incomplete.

---

## 8) Operational constraints (do not violate)

- Do not add anything related to geoblock/KYC bypassing.
- Do not add any secret material (private keys, seed phrases) to repo outputs or logs.
- Do not assume the operator will run the bot on the desktop; capture runs on the VPS, analysis can run elsewhere.
