from pathlib import Path

from pm_arb.capture_inspect import inspect_run
from pm_arb.capture_offline import run_capture_offline
from pm_arb.config import Config


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_capture_inspect_summary(tmp_path: Path) -> None:
    fixtures_dir = _repo_root() / "testdata" / "fixtures"
    config = Config(data_dir=str(tmp_path), offline=True)
    result = run_capture_offline(config, fixtures_dir, run_id="inspect-run")
    summary = inspect_run(result.run.run_dir)
    payload = summary.payload

    assert payload["run_id"] == result.run.run_id
    assert payload["shards"]["count"] == 1
    assert payload["shards"]["records"] == result.stats.frames
    assert payload["metrics"]["global"]["count"] >= 1
