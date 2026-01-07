# Phase 1.5 Notes (Universe Segments + Fees + Churn)

## Segment metadata
- Stored in run manifests at `data/runs/<run_id>/manifest.json` under `segment_metadata` with `by_token_id` and `by_market_id`.
- Updated in-memory on refresh in `pm_arb.capture_online.UniverseState` as `segment_by_token_id` and `segment_by_market_id`.
- Build helper: `pm_arb.segments.build_segment_maps(markets, fee_results_by_token=...)`.

## Fee discovery + estimator
- Fee-rate endpoint (Maker Rebates docs): https://docs.polymarket.com/
- Runtime client: `pm_arb.fees.FeeRateClient` with TTL cache, bounded in-flight, and refresh timeouts.
- Order accessor: `pm_arb.fees.required_fee_rate_bps` (for create-order `feeRateBps`).
- Estimator: `pm_arb.fees.estimate_taker_fee` uses doc-table points (1/100 shares only) and returns `None` for non-table points.
- Fee denomination note: buy fees in tokens, sell fees in USDC (Maker Rebates docs).

## Fee regime circuit breaker
- Monitor: `pm_arb.fees.FeeRegimeMonitor` (stable sampling seeded by run_id + universe_version, canary tokens supported).
- State surface: `fee_regime_state`, `circuit_breaker_reason`, `sampled_tokens_count`, `fee_rate_unknown_count` in `metrics/global.ndjson`.
- Phase-2 gate: read the latest `fee_regime_state` in metrics or `CaptureState.universe.fee_regime_state` at runtime.

## Expected churn classifier
- Implemented in `pm_arb.capture_online` and used to bypass churn guard when expected.
- Inputs: cadence bucket + expiry timestamps when present, otherwise wall-clock boundary windows.
- Output: `expected_churn_bool`, `expected_churn_reason`, `rollover_window_hit`, `expiry_window_hit` in global metrics.

## Policy mapping (scaffolding only)
- Config: `policy_default_id` plus JSON rules in `policy_rules_json` (first-match wins).
- Manifest: `policy_map` and `policy_assignments` show segment-to-policy distribution.
- Metrics: `policy_counts` summarizes per-policy token counts.

## Config knobs (summary)
- Fee discovery: `fee_rate_enable`, `fee_rate_base_url`, `fee_rate_timeout_seconds`, `fee_rate_cache_ttl_seconds`,
  `fee_rate_max_in_flight`, `fee_rate_prefetch_max_tokens`, `fee_rate_refresh_timeout_seconds`.
- Fee regime: `fee_regime_sample_size_per_cycle`, `fee_regime_canary_token_ids`,
  `fee_regime_expect_15m_crypto_fee_enabled`, `fee_regime_expect_other_fee_free`,
  `fee_regime_expect_unknown_fee_free`, `fee_regime_expected_fee_rate_bps_values`.
- Expected churn: `capture_expected_churn_enable`, `capture_expected_churn_window_seconds_5m`,
  `capture_expected_churn_window_seconds_15m`, `capture_expected_churn_window_seconds_30m`,
  `capture_expected_churn_window_seconds_60m`, `capture_expected_churn_expiry_window_seconds`,
  `capture_expected_churn_min_ratio`.
- Policy: `policy_default_id`, `policy_rules_json`.
