import hashlib
import json

import pytest
from conftest import FakeJobLauncher, write
from typer.testing import CliRunner

from brik2_dp_dispatcher import cli
from brik2_dp_dispatcher.dispatcher import Dispatcher
from brik2_dp_dispatcher.jobs import JobPhase
from brik2_dp_dispatcher.state import StateStore, Status

runner = CliRunner()


@pytest.fixture
def env(monkeypatch, tmp_path, watch_dir):
    db = tmp_path / "state.db"
    monkeypatch.setenv("BRIK_DISPATCH_WATCH_DIR", str(watch_dir))
    monkeypatch.setenv("BRIK_DISPATCH_DB_PATH", str(db))
    monkeypatch.setenv("BRIK_DISPATCH_STABLE_SCANS", "1")
    return db


def invoke(*args):
    return runner.invoke(cli.app, list(args))


def populate(db, watch_dir):
    """Two files: one failed permanently, one done."""
    settings = cli.load_settings(max_attempts=1)
    launcher = FakeJobLauncher()
    write(watch_dir, "a/bad.laz", b"bad")
    write(watch_dir, "b/good.laz", b"good")
    with StateStore(db) as store:
        dispatcher = Dispatcher(settings, store, launcher)
        dispatcher.run_once()
        for name in launcher.active:
            request = launcher.requests[name]
            ok = request.items[0].local_path.name == "good.laz"
            launcher.finish(name, JobPhase.SUCCEEDED if ok else JobPhase.FAILED, "Failed")
        dispatcher.run_once()


def test_read_commands_require_existing_db(env):
    result = invoke("status")
    assert result.exit_code == 1 and "not found" in result.output


def test_status_list_show_retry(env, watch_dir):
    populate(env, watch_dir)
    bad = hashlib.sha256(b"bad").hexdigest()

    result = invoke("status")
    assert result.exit_code == 0
    assert "failed_permanently  1" in result.output and "done                1" in result.output

    result = invoke("list", "--status", "failed_permanently", "--json")
    rows = json.loads(result.output)
    assert [r["content_hash"] for r in rows] == [bad]

    result = invoke("list")
    assert "a/bad.laz" in result.output and "b/good.laz" in result.output

    result = invoke("show", bad[:8])
    assert result.exit_code == 0 and "a/bad.laz" in result.output and "attempts" in result.output

    assert invoke("show", "zzzz").exit_code == 1
    assert invoke("retry").exit_code == 2

    result = invoke("retry", "--all-failed")
    assert result.exit_code == 0 and "reset 1" in result.output
    with StateStore(env) as store:
        assert store.get(bad).status is Status.PENDING


def test_scan_dry_run_writes_nothing(env, watch_dir):
    write(watch_dir, "x/cloud.laz", b"cloud")
    result = invoke("scan", "--once", "--dry-run")
    assert result.exit_code == 0, result.output
    assert "x/cloud.laz -> /mnt/brik/incoming/x/cloud.laz" in result.output
    assert "1 job(s) would be launched" in result.output
    assert not env.exists()


def test_scan_once_uses_launcher(env, watch_dir, monkeypatch):
    launcher = FakeJobLauncher()
    monkeypatch.setattr(cli, "_launcher", lambda settings: launcher)
    write(watch_dir, "x.laz", b"x")
    result = invoke("scan", "--once")
    assert result.exit_code == 0, result.output
    assert "launched 1" in result.output and len(launcher.created) == 1


def test_health(env, watch_dir, monkeypatch):
    monkeypatch.setattr(cli, "_launcher", lambda settings: FakeJobLauncher())
    invoke("scan", "--once")
    assert invoke("health").exit_code == 0
    assert invoke("health", "--max-age", "-1").exit_code == 1


def test_invalid_configuration_is_reported(env, monkeypatch):
    monkeypatch.setenv("BRIK_DISPATCH_MAX_CONCURRENT_JOBS", "0")
    result = invoke("status")
    assert result.exit_code == 2 and "invalid configuration" in result.output


def test_cli_option_overrides_env(env, tmp_path, watch_dir):
    other = tmp_path / "other"
    other.mkdir()
    write(other, "only-here.laz", b"o")
    result = invoke("--watch-dir", str(other), "scan", "--dry-run")
    assert "only-here.laz" in result.output


def test_run_without_job_image_fails_cleanly(env):
    result = invoke("run")
    assert result.exit_code == 2 and "job_image" in result.output
