import pm_arb.cli as cli
from pm_arb.config import Config
from pm_arb.engine import Engine


def test_slug_regex_match():
    config = Config(market_regex=r"^slug-match$")
    engine = Engine(config)
    markets = [
        {
            "id": "m1",
            "slug": "slug-match",
            "question": "not a match",
            "clobTokenIds": ["t1", "t2"],
            "active": True,
        }
    ]
    results = engine._discover_candidates(markets)
    assert results
    assert "regex" in results[0].get("_match_reasons", [])


def test_discover_only_matching(monkeypatch, capsys):
    class DummyEngine:
        def __init__(self, config):
            self.config = config

        def discover(self):
            return [
                {"id": "m1", "slug": "s1", "question": "q1", "_match_reasons": ["regex"]},
                {"id": "m2", "slug": "s2", "question": "q2", "_match_reasons": []},
            ]

    monkeypatch.setattr(cli, "Engine", DummyEngine)
    exit_code = cli.main(["discover", "--only-matching"])
    assert exit_code == 0
    output = capsys.readouterr().out.strip().splitlines()
    assert len(output) == 1
    assert output[0].startswith("m1\tmatch\t")


def test_discover_universe_active_binary(monkeypatch, capsys):
    markets = [
        {
            "id": "m1",
            "slug": "s1",
            "question": "q1",
            "active": True,
            "enableOrderBook": True,
            "clobTokenIds": ["t1", "t2"],
        },
        {
            "id": "m2",
            "slug": "s2",
            "question": "q2",
            "active": True,
            "clobTokenIds": ["t3"],
        },
        {
            "id": "m3",
            "slug": "s3",
            "question": "q3",
            "active": False,
            "clobTokenIds": ["t4", "t5"],
        },
        {
            "id": "m4",
            "slug": "s4",
            "question": "q4",
            "active": True,
            "enableOrderBook": False,
            "clobTokenIds": ["t6", "t7"],
        },
    ]
    monkeypatch.setattr(cli, "fetch_markets", lambda *args, **kwargs: markets)
    exit_code = cli.main(["discover", "--universe", "active-binary"])
    assert exit_code == 0
    output = capsys.readouterr().out.strip().splitlines()
    assert output == ["m1\tuniverse\ts1\tq1"]
