from pathlib import Path, PurePosixPath

import pytest

from brik2_dp_dispatcher.config import Settings
from brik2_dp_dispatcher.jobs import job_name, translate_path


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/data/incoming/2026/f1/cloud.laz", "/mnt/brik/incoming/2026/f1/cloud.laz"),
        ("2026/f1/cloud.laz", "/mnt/brik/incoming/2026/f1/cloud.laz"),
        (Path("/data/incoming/top.laz"), "/mnt/brik/incoming/top.laz"),
        ("/data/incoming/with space/ł.laz", "/mnt/brik/incoming/with space/ł.laz"),
    ],
)
def test_translate_path(path, expected):
    assert translate_path(path, "/data/incoming", "/mnt/brik/incoming") == PurePosixPath(expected)


@pytest.mark.parametrize("path", ["/data/other/x.laz", "/data/incoming-2/x.laz", "../x.laz"])
def test_translate_path_rejects_paths_outside_root(path):
    with pytest.raises(ValueError):
        translate_path(path, "/data/incoming", "/mnt/brik/incoming")


def test_job_name_is_deterministic_and_dns_safe():
    name = job_name("BRIK_dp", "0123456789abcdef" * 4, 3)
    assert name == "brik-dp-0123456789ab-a3"
    assert job_name("x" * 100, "a" * 64, 1).endswith("-aaaaaaaaaaaa-a1")
    assert len(job_name("x" * 100, "a" * 64, 1)) <= 63


def test_settings_validation(tmp_path):
    with pytest.raises(ValueError):
        Settings(job_nfs_server="nfs", _env_file=None)
    with pytest.raises(ValueError):
        Settings(job_nfs_server="nfs", job_nfs_path="/x", job_pvc_name="pvc", _env_file=None)
    with pytest.raises(ValueError):
        Settings(poll_interval=3600, job_ttl_seconds_after_finished=60, _env_file=None)


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("BRIK_DISPATCH_WATCH_DIR", "/srv/in")
    monkeypatch.setenv("BRIK_DISPATCH_MAX_CONCURRENT_JOBS", "4")
    monkeypatch.setenv("BRIK_DISPATCH_JOB_ARGS", '["--input", "{input_path}"]')
    monkeypatch.setenv("BRIK_DISPATCH_IGNORE_SUFFIXES", '[".PART"]')
    settings = Settings(_env_file=None)
    assert settings.watch_dir == Path("/srv/in")
    assert settings.max_concurrent_jobs == 4
    assert settings.job_args == ["--input", "{input_path}"]
    assert settings.ignore_suffixes == [".part"]
    assert Settings().watch_dir == Path("/srv/in")


def test_empty_env_values_fall_back_to_defaults(monkeypatch):
    monkeypatch.setenv("BRIK_DISPATCH_KUBE_CONTEXT", "")
    monkeypatch.setenv("BRIK_DISPATCH_JOB_ENV", "")
    monkeypatch.setenv("BRIK_DISPATCH_POLL_INTERVAL", "")
    settings = Settings(_env_file=None)
    assert settings.kube_context is None and settings.job_env == {} and settings.poll_interval == 30
