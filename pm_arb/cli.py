from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from .book import BookParseError, OrderBook, parse_ws_message
from .clob_rest import RestClient, fetch_book_with_retries
from .clob_ws import wait_for_decodable_book
from .config import Config
from .capture import bootstrap_run
from .capture_offline import quantile, run_capture_offline
from .capture_inspect import inspect_run
from .capture_online import run_capture_online
from .capture_format import verify_frames
from .capture_slice import slice_run
from .engine import Engine
from .gamma import fetch_markets, parse_clob_token_ids
from .market_select import select_active_binary_markets
from .reconcile import Reconciler
from .report import generate_report
from .sweep import sweep_cost


def _is_field_type(field_type, expected: type, expected_name: str) -> bool:
    if field_type is expected:
        return True
    if isinstance(field_type, str) and field_type == expected_name:
        return True
    return False


def _str2bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid bool: {value}")


def _add_config_args(parser: argparse.ArgumentParser) -> None:
    for field in fields(Config):
        name = field.name.replace("_", "-")
        if _is_field_type(field.type, bool, "bool"):
            if field.name == "offline":
                group = parser.add_mutually_exclusive_group()
                group.add_argument(
                    f"--{name}",
                    dest=field.name,
                    nargs="?",
                    const=True,
                    default=None,
                    type=_str2bool,
                )
                group.add_argument(f"--no-{name}", dest=field.name, action="store_false")
                continue
            group = parser.add_mutually_exclusive_group()
            group.add_argument(f"--{name}", dest=field.name, action="store_true")
            group.add_argument(f"--no-{name}", dest=field.name, action="store_false")
            parser.set_defaults(**{field.name: None})
        else:
            parser.add_argument(f"--{name}", dest=field.name, default=None)


def _cli_overrides(ns: argparse.Namespace) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for field in fields(Config):
        value = getattr(ns, field.name, None)
        if value is None:
            continue
        if _is_field_type(field.type, bool, "bool"):
            overrides[field.name] = _str2bool(value)
        elif _is_field_type(field.type, int, "int"):
            overrides[field.name] = int(value)
        elif _is_field_type(field.type, float, "float"):
            overrides[field.name] = float(value)
        else:
            overrides[field.name] = value
    return overrides


def contract_test_offline(config: Config, fixtures_dir: Path) -> int:
    ws_clean = json.loads((fixtures_dir / "ws_book_clean.json").read_text(encoding="utf-8"))
    ws_extra = json.loads((fixtures_dir / "ws_book_extra_fields.json").read_text(encoding="utf-8"))
    ws_bad = json.loads((fixtures_dir / "ws_book_malformed.json").read_text(encoding="utf-8"))
    rest_fixture = json.loads((fixtures_dir / "rest_book.json").read_text(encoding="utf-8"))
    if not ws_clean or not ws_extra or not ws_bad:
        raise RuntimeError("fixtures missing")
    # decode clean + extra
    for msg in ws_clean + ws_extra:
        parse_ws_message(msg)
    # malformed must raise
    malformed_ok = False
    for msg in ws_bad:
        try:
            parse_ws_message(msg)
        except BookParseError:
            malformed_ok = True
            break
    if not malformed_ok:
        raise RuntimeError("malformed fixture did not trigger parse error")
    # sweep sanity
    token_id, asks, _ = parse_ws_message(ws_clean[0])
    book = OrderBook(token_id=token_id)
    book.update_from_asks(asks, config.top_k)
    cost, _, ok = sweep_cost(book.asks, 10 * 1_000_000)
    if not ok or cost <= 0:
        raise RuntimeError("sweep cost invalid")
    rest_book = OrderBook(token_id=token_id)
    rest_book.update_from_rest(rest_fixture[token_id], config.top_k, price_scale=config.price_scale)
    # reconcile persistence
    recon = Reconciler(
        persist_n=config.reconcile_mismatch_persist_n,
        tick_tolerance=config.reconcile_tick_tolerance,
        rel_tol=config.reconcile_rel_tol,
    )
    rest_book.update_from_asks([(800_000, 10 * 1_000_000)], config.top_k)
    sizes = [10]
    for idx in range(config.reconcile_mismatch_persist_n - 1):
        result = recon.compare(token_id, book, rest_book, sizes)
        if result.desynced:
            raise RuntimeError("desynced too early")
    result = recon.compare(token_id, book, rest_book, sizes)
    if not result.desynced:
        raise RuntimeError("desync persistence failed")
    print("contract-test --offline PASS")
    return 0


def contract_test_online(config: Config) -> int:
    markets = fetch_markets(
        config.gamma_base_url,
        config.rest_timeout,
        limit=config.gamma_limit,
        max_markets=config.max_markets,
    )
    regex = config.market_regex
    engine = Engine(config)
    filtered = engine._filter_markets(markets)
    if filtered:
        candidates = filtered
    else:
        candidates = [
            market
            for market in engine._discover_candidates(markets)
            if market.get("enableOrderBook") is not False
        ]
    for market in candidates[:200]:
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        token_a, token_b = str(token_ids[0]), str(token_ids[1])
        rest = RestClient(
            base_url=config.clob_rest_base_url,
            timeout=config.rest_timeout,
            rate_per_sec=config.rest_rate_per_sec,
            burst=config.rest_burst,
        )
        book_a = fetch_book_with_retries(rest, token_a, config.rest_retry_max)
        book_b = fetch_book_with_retries(rest, token_b, config.rest_retry_max)
        if not book_a.get("asks") and not book_b.get("asks"):
            continue
        try:
            timeout = float(config.contract_timeout)
            payload_used, ws_msg, attempted_payloads = asyncio.run(
                wait_for_decodable_book(
                    config.clob_ws_url,
                    [token_a, token_b],
                    timeout,
                    lambda msg: parse_ws_message(msg, price_scale=config.price_scale),
                )
            )
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc
        ws_keys: list[str] = []
        if isinstance(ws_msg, list) and ws_msg:
            first = ws_msg[0]
            if isinstance(first, dict):
                ws_keys = list(first.keys())
        elif isinstance(ws_msg, dict):
            ws_keys = list(ws_msg.keys())
        _, asks, _ = parse_ws_message(ws_msg, price_scale=config.price_scale)
        if not asks:
            raise RuntimeError(
                f"WS contract failed: empty asks; ws_url={config.clob_ws_url}; payloads={attempted_payloads}"
            )
        recon = Reconciler(
            persist_n=config.reconcile_mismatch_persist_n,
            tick_tolerance=config.reconcile_tick_tolerance,
            rel_tol=config.reconcile_rel_tol,
        )
        ws_book = OrderBook(token_id=token_a)
        ws_book.update_from_rest(book_a, config.top_k, price_scale=config.price_scale)
        rest_book = OrderBook(token_id=token_a)
        rest_book.update_from_rest(book_a, config.top_k, price_scale=config.price_scale)
        result = recon.compare(token_a, ws_book, rest_book, [10])
        print(
            "contract-test PASS",
            json.dumps(
                {
                    "market_id": market.get("id"),
                    "token_a": token_a,
                    "token_b": token_b,
                    "payload": payload_used,
                    "ws_keys": ws_keys,
                    "reconcile_desynced": result.desynced,
                }
            ),
        )
        return 0
    raise RuntimeError(f"no suitable market found; regex={regex}")


def _resolve_verify_targets(args: argparse.Namespace) -> list[tuple[Path, Path | None]]:
    if args.frames:
        if args.run_dir:
            raise ValueError("use --frames or --run-dir, not both")
        if args.idx and not args.frames:
            raise ValueError("--idx requires --frames")
        frames_path = Path(args.frames)
        idx_path = Path(args.idx) if args.idx else None
        return [(frames_path, idx_path)]
    if args.run_dir:
        run_dir = Path(args.run_dir)
        capture_dir = run_dir / "capture"
        frames_paths = sorted(capture_dir.glob("*.frames"))
        if not frames_paths:
            raise ValueError(f"no frames found in {capture_dir}")
        targets: list[tuple[Path, Path | None]] = []
        for frames_path in frames_paths:
            idx_path = frames_path.with_suffix(".idx")
            targets.append((frames_path, idx_path if idx_path.exists() else None))
        return targets
    raise ValueError("must provide --frames or --run-dir")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pm_arb")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    _add_config_args(common)

    scan = subparsers.add_parser("scan", parents=[common])
    scan.add_argument("--fixtures-dir", default="testdata/fixtures")

    report = subparsers.add_parser("report", parents=[common])

    contract = subparsers.add_parser("contract-test", parents=[common])
    contract.add_argument("--fixtures-dir", default="testdata/fixtures")

    discover = subparsers.add_parser("discover", parents=[common])
    discover.add_argument("--only-matching", action="store_true", default=False)
    discover.add_argument("--universe", default=None, choices=["active-binary"])

    capture = subparsers.add_parser("capture", parents=[common])
    capture.add_argument("--run-id", default=None)
    capture.add_argument("--fixtures-dir", default="testdata/fixtures")

    capture_verify = subparsers.add_parser("capture-verify", parents=[common])
    capture_verify.add_argument("--frames", default=None)
    capture_verify.add_argument("--idx", default=None)
    capture_verify.add_argument("--run-dir", default=None)
    capture_verify.add_argument("--quiet", action="store_true", default=False)
    capture_verify.add_argument("--summary-only", action="store_true", default=False)

    capture_bench = subparsers.add_parser("capture-bench", parents=[common])
    capture_bench.add_argument("--run-id", default=None)
    capture_bench.add_argument("--fixtures-dir", default="testdata/fixtures")
    capture_bench.add_argument("--multiplier", type=int, default=1)

    capture_inspect = subparsers.add_parser("capture-inspect", parents=[common])
    capture_inspect.add_argument("--run-dir", required=True)

    capture_slice = subparsers.add_parser("capture-slice", parents=[common])
    capture_slice.add_argument("--run-dir", required=True)
    capture_slice.add_argument("--out-dir", default=None)
    capture_slice.add_argument("--slice-id", default=None)
    capture_slice.add_argument("--shard", type=int, default=None)
    capture_slice.add_argument("--start-mono-ns", type=int, default=None)
    capture_slice.add_argument("--end-mono-ns", type=int, default=None)
    capture_slice.add_argument("--start-offset", type=int, default=None)
    capture_slice.add_argument("--end-offset", type=int, default=None)

    args = parser.parse_args(argv)
    overrides = _cli_overrides(args)
    config = Config.from_env_and_cli(overrides, os.environ)

    if args.command == "scan":
        engine = Engine(config, fixtures_dir=args.fixtures_dir)
        if config.offline:
            return engine.scan_offline()
        return engine.scan_online()
    if args.command == "report":
        generate_report(config.data_dir)
        print(f"report written to {Path(config.data_dir) / 'reports'}")
        return 0
    if args.command == "contract-test":
        if config.offline:
            return contract_test_offline(config, Path(args.fixtures_dir))
        return contract_test_online(config)
    if args.command == "discover":
        if args.universe == "active-binary":
            markets = fetch_markets(
                config.gamma_base_url,
                config.rest_timeout,
                limit=config.gamma_limit,
                max_markets=config.capture_max_markets,
            )
            candidates = select_active_binary_markets(
                markets, max_markets=config.capture_max_markets
            )
            for market in candidates:
                slug = market.get("slug", "")
                question = market.get("question", "")
                print(f"{market.get('id','unknown')}\tuniverse\t{slug}\t{question}")
            return 0
        engine = Engine(config)
        candidates = engine.discover()
        for market in candidates:
            reasons = market.get("_match_reasons", [])
            match_status = "match" if reasons else "no_match"
            if args.only_matching and match_status == "no_match":
                continue
            slug = market.get("slug", "")
            question = market.get("question", "")
            print(f"{market.get('id','unknown')}\t{match_status}\t{slug}\t{question}")
        return 0
    if args.command == "capture":
        if config.offline:
            result = run_capture_offline(config, Path(args.fixtures_dir), run_id=args.run_id)
            print(f"capture run dir: {result.run.run_dir}")
            return 0
        return run_capture_online(config, run_id=args.run_id)
    if args.command == "capture-verify":
        try:
            targets = _resolve_verify_targets(args)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        summaries: list[dict[str, Any]] = []
        all_ok = True
        for frames_path, idx_path in targets:
            try:
                summary = verify_frames(frames_path, idx_path=idx_path)
            except FileNotFoundError as exc:
                print(f"file not found: {exc}", file=sys.stderr)
                return 2
            summary["frames_path"] = str(frames_path)
            summary["idx_path"] = str(idx_path) if idx_path is not None else None
            summaries.append(summary)
            all_ok = all_ok and summary.get("ok", False)
        summaries.sort(key=lambda entry: entry.get("frames_path") or "")
        totals = {
            "records": sum(entry.get("records", 0) for entry in summaries),
            "errors": sum(entry.get("errors", 0) for entry in summaries),
            "crc_mismatch": sum(entry.get("crc_mismatch", 0) for entry in summaries),
            "truncated_shards": [
                entry.get("frames_path") for entry in summaries if entry.get("truncated")
            ],
            "idx_bad_shards": [
                entry.get("frames_path")
                for entry in summaries
                if entry.get("idx_ok") is False
            ],
        }
        if not args.quiet:
            payload = {"ok": all_ok, "totals": totals}
            if not args.summary_only:
                payload["summaries"] = summaries
            print(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        return 0 if all_ok else 1
    if args.command == "capture-bench":
        if not config.offline:
            print("capture-bench requires --offline", file=sys.stderr)
            return 2
        result = run_capture_offline(
            config,
            Path(args.fixtures_dir),
            run_id=args.run_id,
            multiplier=args.multiplier,
        )
        elapsed_sec = max(result.elapsed_ns / 1_000_000_000.0, 1e-9)
        summary = {
            "run_id": result.run.run_id,
            "frames": result.stats.frames,
            "bytes_written": result.stats.bytes_written,
            "elapsed_ns": result.elapsed_ns,
            "msgs_per_sec": result.stats.frames / elapsed_sec,
            "bytes_per_sec": result.stats.bytes_written / elapsed_sec,
            "write_ns_p99": quantile(result.stats.write_durations_ns, 99),
            "ingest_ns_p99": quantile(result.stats.ingest_latencies_ns, 99),
        }
        print(json.dumps(summary, ensure_ascii=True, separators=(",", ":")))
        return 0
    if args.command == "capture-inspect":
        try:
            summary = inspect_run(Path(args.run_dir))
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 2
        print(json.dumps(summary.payload, ensure_ascii=True, separators=(",", ":")))
        return 0
    if args.command == "capture-slice":
        try:
            result = slice_run(
                Path(args.run_dir),
                out_dir=Path(args.out_dir) if args.out_dir else None,
                slice_id=args.slice_id,
                shard=args.shard,
                start_mono_ns=args.start_mono_ns,
                end_mono_ns=args.end_mono_ns,
                start_offset=args.start_offset,
                end_offset=args.end_offset,
            )
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 2
        print(f"slice dir: {result.slice_dir}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
