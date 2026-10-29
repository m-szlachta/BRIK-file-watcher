import hashlib
import logging
import shutil
from pathlib import PurePosixPath

import pytest
from conftest import write

from brik2_dp_dispatcher.dispatcher import Dispatcher
from brik2_dp_dispatcher.jobs import JobPhase
from brik2_dp_dispatcher.state import StateStore, Status


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def only_job(launcher) -> str:
    assert len(launcher.active) == 1
    return launcher.active[0]


def test_new_file_launches_job_with_translated_path(watch_dir, dispatcher, launcher, store):
    write(watch_dir, "2026/flight1/cloud.laz", b"cloud")
    report = dispatcher.run_once()
    assert report.new_files == 1 and len(report.launched) == 1
    request = launcher.requests[only_job(launcher)]
    assert request.items[0].job_path == PurePosixPath("/mnt/brik/incoming/2026/flight1/cloud.laz")
    assert request.attempt == 1
    record = store.get(sha(b"cloud"))
    assert record.status is Status.PROCESSING and record.job_name == request.name


def test_unstable_file_waits_for_second_scan(watch_dir, settings, store, launcher):
    settings.stable_scans = 2
    dispatcher = Dispatcher(settings, store, launcher)
    write(watch_dir, "x.laz", b"x")
    write(watch_dir, "y.laz.part", b"y")
    assert dispatcher.run_once().launched == []
    assert len(dispatcher.run_once().launched) == 1
    assert store.get(sha(b"y")) is None


def test_moved_file_is_not_reprocessed(watch_dir, dispatcher, launcher, store):
    write(watch_dir, "a/x.laz", b"same")
    dispatcher.run_once()
    launcher.finish(only_job(launcher), JobPhase.SUCCEEDED)
    dispatcher.run_once()
    (watch_dir / "b").mkdir()
    shutil.move(watch_dir / "a/x.laz", watch_dir / "b/x.laz")
    report = dispatcher.run_once()
    assert report.launched == [] and report.new_files == 0
    record = store.get(sha(b"same"))
    assert record.status is Status.DONE
    assert record.last_seen_path == "b/x.laz"
    assert [p.path for p in store.paths(record.content_hash)] == ["a/x.laz", "b/x.laz"]
    assert len(launcher.created) == 1


def test_moved_pending_file_launches_from_new_location(watch_dir, settings, store, launcher):
    settings.max_concurrent_jobs = 1
    dispatcher = Dispatcher(settings, store, launcher)
    write(watch_dir, "busy.laz", b"busy")
    dispatcher.run_once()
    write(watch_dir, "a/x.laz", b"x")
    dispatcher.run_once()
    shutil.move(watch_dir / "a", watch_dir / "b")
    launcher.finish(only_job(launcher), JobPhase.SUCCEEDED)
    report = dispatcher.run_once()
    assert [str(r.items[0].job_path) for r in report.launched] == ["/mnt/brik/incoming/b/x.laz"]


def test_same_name_different_content_are_two_files(watch_dir, settings, store, launcher):
    write(watch_dir, "a/data.laz", b"first")
    write(watch_dir, "b/data.laz", b"second")
    report = Dispatcher(settings, store, launcher).run_once()
    assert report.new_files == 2 and len(report.launched) == 2
    assert {r.content_hash for r in store.list_files()} == {sha(b"first"), sha(b"second")}


def test_identical_content_is_one_file(watch_dir, dispatcher, launcher, store, caplog):
    write(watch_dir, "a/one.laz", b"dup")
    write(watch_dir, "b/two.laz", b"dup")
    with caplog.at_level(logging.INFO):
        report = dispatcher.run_once()
    assert report.new_files == 1 and len(report.launched) == 1
    assert len(store.list_files()) == 1
    assert {p.path for p in store.paths(sha(b"dup"))} == {"a/one.laz", "b/two.laz"}
    assert any("identical content" in r.message and r.levelno == logging.INFO for r in caplog.records)


def test_identical_content_appearing_later_is_not_reprocessed(watch_dir, dispatcher, launcher, store, caplog):
    write(watch_dir, "a/one.laz", b"dup")
    dispatcher.run_once()
    write(watch_dir, "b/copy.laz", b"dup")
    with caplog.at_level(logging.INFO):
        report = dispatcher.run_once()
    assert report.launched == []
    assert store.get(sha(b"dup")).last_seen_path == "a/one.laz"
    assert any("new path" in r.message for r in caplog.records)


def test_max_concurrent_jobs_is_respected(watch_dir, settings, store, launcher):
    settings.max_concurrent_jobs = 2
    dispatcher = Dispatcher(settings, store, launcher)
    for i in range(5):
        write(watch_dir, f"f{i}.laz", f"content {i}".encode())
    for _ in range(3):
        dispatcher.run_once()
        assert len(launcher.active) == 2
    launcher.finish(launcher.active[0], JobPhase.SUCCEEDED)
    dispatcher.run_once()
    assert len(launcher.active) == 2
    assert store.count_by_status()[Status.DONE] == 1
    assert store.count_by_status()[Status.PROCESSING] == 2
    assert len(launcher.created) == 3


def test_success_marks_done_and_deletes_job(watch_dir, dispatcher, launcher, store):
    write(watch_dir, "x.laz", b"x")
    dispatcher.run_once()
    name = only_job(launcher)
    launcher.finish(name, JobPhase.SUCCEEDED)
    report = dispatcher.run_once()
    assert report.finished == 1
    record = store.get(sha(b"x"))
    assert record.status is Status.DONE and record.finished_at is not None
    assert launcher.deleted == [name]


def test_failures_retry_then_fail_permanently(watch_dir, settings, store, launcher):
    settings.max_attempts = 3
    dispatcher = Dispatcher(settings, store, launcher)
    write(watch_dir, "x.laz", b"x")
    dispatcher.run_once()
    names = []
    for attempt in range(1, 4):
        name = only_job(launcher)
        names.append(name)
        assert name.endswith(f"-a{attempt}")
        launcher.finish(name, JobPhase.FAILED, "DeadlineExceeded: Job was active longer than specified deadline")
        dispatcher.run_once()
        record = store.get(sha(b"x"))
        assert record.attempts == attempt
        assert record.last_error.startswith("DeadlineExceeded")
    assert record.status is Status.FAILED_PERMANENTLY
    assert launcher.active == []
    assert launcher.deleted == names
    assert dispatcher.run_once().launched == []


def test_error_status_is_visible_between_attempts(watch_dir, dispatcher, launcher, store):
    write(watch_dir, "x.laz", b"x")
    dispatcher.run_once()
    launcher.finish(only_job(launcher), JobPhase.FAILED, "BackoffLimitExceeded")
    dispatcher.sync_jobs()
    assert store.get(sha(b"x")).status is Status.ERROR


def test_rejected_job_counts_as_failed_attempt(watch_dir, settings, store, launcher):
    settings.max_attempts = 1
    launcher.reject_with = "422 invalid spec"
    write(watch_dir, "x.laz", b"x")
    report = Dispatcher(settings, store, launcher).run_once()
    record = store.get(sha(b"x"))
    assert report.launched == [] and record.status is Status.FAILED_PERMANENTLY
    assert "422" in record.last_error


def test_unknown_launch_failure_is_resolved_by_next_sync(watch_dir, dispatcher, launcher, store):
    write(watch_dir, "x.laz", b"x")
    launcher.fail_with = TimeoutError("api timeout")
    dispatcher.run_once()
    assert store.get(sha(b"x")).status is Status.PROCESSING
    launcher.fail_with = None
    report = dispatcher.run_once()
    assert report.released == 1 and len(report.launched) == 1
    assert store.get(sha(b"x")).attempts == 0


def test_deleted_file_is_not_launched(watch_dir, settings, store, launcher):
    settings.max_concurrent_jobs = 1
    dispatcher = Dispatcher(settings, store, launcher)
    paths = {sha(data): write(watch_dir, f"{data.decode()}.laz", data) for data in (b"a", b"b")}
    dispatcher.run_once()
    (waiting,) = store.list_files(Status.PENDING)
    paths[waiting.content_hash].unlink()
    launcher.finish(only_job(launcher), JobPhase.SUCCEEDED)
    assert dispatcher.run_once().launched == []
    assert store.get(waiting.content_hash).status is Status.PENDING


def test_missing_watch_dir_does_not_break_monitoring(watch_dir, dispatcher, launcher, store):
    write(watch_dir, "x.laz", b"x")
    dispatcher.run_once()
    launcher.finish(only_job(launcher), JobPhase.SUCCEEDED)
    shutil.rmtree(watch_dir)
    report = dispatcher.run_once()
    assert report.finished == 1 and report.errors
    assert store.get(sha(b"x")).status is Status.DONE


# Restart recovery ------------------------------------------------------------------------------------------------


@pytest.fixture
def restart(settings, launcher):
    """Start a new dispatcher process on the same database and cluster."""
    stores = []

    def start() -> tuple[Dispatcher, StateStore]:
        stores.append(StateStore(settings.db_path))
        return Dispatcher(settings, stores[-1], launcher), stores[-1]

    yield start
    for store in stores:
        store.close()


@pytest.fixture
def crashed(watch_dir, restart, launcher):
    """A dispatcher that launched one Job and then died."""
    write(watch_dir, "x.laz", b"x")
    first, _ = restart()
    first.run_once()
    return only_job(launcher)


def test_recovery_keeps_monitoring_running_job(restart, crashed, settings, launcher):
    dispatcher, store = restart()
    dispatcher.reconcile()
    record = store.get(sha(b"x"))
    assert record.status is Status.PROCESSING and record.job_name == crashed
    assert dispatcher.run_once().launched == []
    launcher.finish(crashed, JobPhase.SUCCEEDED)
    dispatcher.run_once()
    assert store.get(sha(b"x")).status is Status.DONE
    assert launcher.created == [crashed]


@pytest.mark.parametrize(("phase", "status"), [(JobPhase.SUCCEEDED, Status.DONE), (JobPhase.FAILED, Status.ERROR)])
def test_recovery_records_job_finished_while_down(restart, crashed, settings, launcher, phase, status):
    launcher.finish(crashed, phase, "Failed" if phase is JobPhase.FAILED else None)
    dispatcher, store = restart()
    report = dispatcher.reconcile()
    assert report.finished == 1
    assert store.get(sha(b"x")).status is status
    assert crashed in launcher.deleted


def test_recovery_returns_missing_job_to_pending(restart, crashed, settings, launcher):
    launcher.jobs.clear()
    dispatcher, store = restart()
    report = dispatcher.reconcile()
    record = store.get(sha(b"x"))
    assert report.released == 1
    assert record.status is Status.PENDING and record.attempts == 0
    assert len(dispatcher.run_once().launched) == 1


def test_crash_between_claim_and_create_does_not_double_launch(restart, watch_dir, settings, store, launcher):
    """The claim is committed but the process dies before (or right after) the API call."""
    write(watch_dir, "x.laz", b"x")
    dispatcher = Dispatcher(settings, store, launcher)
    request = dispatcher.pending_requests(dispatcher.register(dispatcher.scan()), dispatcher.free_slots())[0]
    store.claim(request.content_hashes, request.name)
    launcher.launch(request)  # created, but the process died before logging it
    dispatcher, store = restart()
    dispatcher.reconcile()
    dispatcher.run_once()
    assert launcher.created == [request.name]
    assert store.get(sha(b"x")).status is Status.PROCESSING


def test_recovery_matches_job_by_hash_label(restart, crashed, settings, launcher):
    job = launcher.jobs.pop(crashed)
    launcher.jobs["renamed"] = job
    launcher.finish("renamed", JobPhase.SUCCEEDED)
    dispatcher, store = restart()
    dispatcher.reconcile()
    assert store.get(sha(b"x")).status is Status.DONE
    assert launcher.deleted == ["renamed"]
