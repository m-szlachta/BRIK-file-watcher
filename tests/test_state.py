import sqlite3

import pytest

from brik2_dp_dispatcher.state import MIGRATIONS, Observation, Sighting, StateStore, Status

H1 = "a" * 64
H2 = "b" * 64


@pytest.fixture
def store(tmp_path):
    with StateStore(tmp_path / "state.db") as s:
        yield s


def test_schema_is_migrated_and_wal_enabled(tmp_path, store):
    assert store.schema_version == len(MIGRATIONS)
    conn = sqlite3.connect(tmp_path / "state.db")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    # Reopening does not re-run migrations.
    StateStore(tmp_path / "state.db").close()


def test_missing_db_is_not_created_for_read_only_use(tmp_path):
    with pytest.raises(FileNotFoundError):
        StateStore(tmp_path / "nope.db", create=False)


def test_register_sighting_tracks_paths(store):
    assert store.register_sighting(H1, 1, "a/x.laz") is Sighting.NEW
    assert store.register_sighting(H1, 1, "a/x.laz") is Sighting.KNOWN_PATH
    assert store.register_sighting(H1, 1, "b/x.laz") is Sighting.NEW_PATH
    assert [p.path for p in store.paths(H1)] == ["a/x.laz", "b/x.laz"]
    assert store.get(H1).status is Status.PENDING


def test_failure_retries_then_fails_permanently(store):
    store.register_sighting(H1, 1, "x")
    for attempt in range(1, 4):
        assert [f.content_hash for f in store.launchable(max_attempts=3)] == [H1]
        store.claim([H1], f"job-{attempt}")
        assert store.get(H1).status is Status.PROCESSING
        store.mark_failed(f"job-{attempt}", "boom", max_attempts=3)
    record = store.get(H1)
    assert record.status is Status.FAILED_PERMANENTLY
    assert record.attempts == 3 and record.last_error == "boom"
    assert store.launchable(max_attempts=3) == []


def test_error_status_between_attempts(store):
    store.register_sighting(H1, 1, "x")
    store.claim([H1], "job-1")
    store.mark_failed("job-1", "DeadlineExceeded", max_attempts=3)
    assert store.get(H1).status is Status.ERROR


def test_done_and_release(store):
    store.register_sighting(H1, 1, "x")
    store.register_sighting(H2, 1, "y")
    store.claim([H1], "job-1")
    store.claim([H2], "job-2")
    assert store.active_job_names() == {"job-1", "job-2"}
    assert store.mark_done("job-1") == 1
    assert store.release("job-2", "missing") == 1
    assert store.get(H1).status is Status.DONE
    released = store.get(H2)
    assert released.status is Status.PENDING and released.attempts == 0 and released.job_name is None


def test_claim_rejects_non_launchable_file(store):
    store.register_sighting(H1, 1, "x")
    store.claim([H1], "job-1")
    with pytest.raises(ValueError):
        store.claim([H1], "job-2")
    assert store.get(H1).job_name == "job-1"


def test_failed_transaction_rolls_back(store):
    store.register_sighting(H1, 1, "x")
    store.register_sighting(H2, 1, "y")
    store.claim([H2], "job-0")
    with pytest.raises(ValueError):
        store.claim([H1, H2], "job-1")
    assert store.get(H1).status is Status.PENDING


def test_reset_and_prefix_lookup(store):
    store.register_sighting(H1, 1, "x")
    store.claim([H1], "job-1")
    assert store.reset([H1]) == 0  # processing files are not reset
    store.mark_failed("job-1", "x", max_attempts=1)
    assert store.reset([H1]) == 1
    record = store.get(H1)
    assert record.status is Status.PENDING and record.attempts == 0
    assert [r.content_hash for r in store.find_by_prefix("AAA")] == [H1]
    assert store.find_by_prefix("%") == []


def test_observations_roundtrip(store):
    obs = [Observation("a", 1, 2, 3, H1), Observation("b", 4, 5, 1, None)]
    store.replace_observations(obs)
    assert store.observations() == {"a": obs[0], "b": obs[1]}
    store.replace_observations(obs[:1])
    assert list(store.observations()) == ["a"]


def test_counts(store):
    store.register_sighting(H1, 1, "x")
    counts = store.count_by_status()
    assert counts[Status.PENDING] == 1 and counts[Status.DONE] == 0
