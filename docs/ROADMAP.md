# Roadmap

This repository implements Phase 1 capture only. Later phases are intentionally not implemented here.

## Phase 2 (future)

- Replay captured frames deterministically from `data/runs/<run_id>/`.
- Use `rx_mono_ns` and `rx_wall_ns_utc` to align event time with external logs.
- Build fill simulations against the recorded frame stream without altering Phase 1 data.

## Phase 3 (future)

- Execution engine that consumes validated Phase 2 models.
- Separate deployment and key management from this repository.
