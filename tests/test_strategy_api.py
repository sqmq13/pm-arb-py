import ast
from pathlib import Path

from pm_arb.runtime.events import TopOfBookUpdate
from pm_arb.runtime.state import MarketState
from pm_arb.runtime.strategy import PortfolioView, StrategyContext
from pm_arb.strategies.toy_spread import ToySpreadStrategy


def test_strategy_emits_intent():
    strategy = ToySpreadStrategy(size_e6=1_000_000, min_spread_e6=100_000)
    event = TopOfBookUpdate(
        market_id="market-1",
        bid_px_e6=900_000,
        bid_sz_e6=1_000_000,
        ask_px_e6=1_100_000,
        ask_sz_e6=1_000_000,
        ts_event=1000,
        ts_recv=2000,
        seq=1,
    )
    market_state = MarketState(market_id="market-1")
    portfolio = PortfolioView()
    ctx = StrategyContext(strategy_id=strategy.strategy_id)
    intents = strategy.on_top_of_book(ctx, event, market_state, portfolio)
    assert len(intents) == 2
    assert [intent.side for intent in intents] == ["buy", "sell"]
    for intent in intents:
        assert intent.market_id == "market-1"
        assert intent.size_e6 == 1_000_000
        assert intent.tag == "toy_spread"


def test_strategy_no_imports():
    path = Path("pm_arb/strategies/toy_spread.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    banned_prefixes = (
        "pm_arb.capture",
        "pm_arb.clob_ws",
        "pm_arb.ws_decode",
        "pm_arb.runtime.execution_sim",
        "pm_arb.runtime.orchestrator",
        "pm_arb.runtime.live",
        "pm_arb.runtime.replay",
    )
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith(banned_prefixes)
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module.startswith(banned_prefixes):
                raise AssertionError(f"banned import: {module}")
            if module == "pm_arb":
                for alias in node.names:
                    if alias.name.startswith("capture") or alias.name in {
                        "ws_decode",
                        "clob_ws",
                    }:
                        raise AssertionError(f"banned import: pm_arb.{alias.name}")
            if module == "pm_arb.runtime":
                for alias in node.names:
                    if alias.name in {"execution_sim", "orchestrator", "live", "replay"}:
                        raise AssertionError(f"banned import: pm_arb.runtime.{alias.name}")
