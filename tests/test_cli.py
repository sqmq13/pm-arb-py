import pm_arb.cli as cli


def _run_scan(monkeypatch, args):
    calls = {"offline": None, "scan_offline": 0, "scan_online": 0}
    monkeypatch.delenv("PM_ARB_OFFLINE", raising=False)

    class DummyEngine:
        def __init__(self, config, fixtures_dir=None):
            calls["offline"] = config.offline

        def scan_offline(self):
            calls["scan_offline"] += 1
            return 0

        def scan_online(self):
            calls["scan_online"] += 1
            return 0

    monkeypatch.setattr(cli, "Engine", DummyEngine)
    exit_code = cli.main(["scan", *args])
    return calls, exit_code


def test_offline_flag_const(monkeypatch):
    calls, exit_code = _run_scan(monkeypatch, ["--offline"])
    assert exit_code == 0
    assert calls["offline"] is True
    assert calls["scan_offline"] == 1
    assert calls["scan_online"] == 0


def test_offline_flag_true(monkeypatch):
    calls, exit_code = _run_scan(monkeypatch, ["--offline", "true"])
    assert exit_code == 0
    assert calls["offline"] is True
    assert calls["scan_offline"] == 1
    assert calls["scan_online"] == 0


def test_offline_flag_false(monkeypatch):
    calls, exit_code = _run_scan(monkeypatch, ["--offline", "false"])
    assert exit_code == 0
    assert calls["offline"] is False
    assert calls["scan_online"] == 1
    assert calls["scan_offline"] == 0
