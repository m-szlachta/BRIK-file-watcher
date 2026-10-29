from __future__ import annotations

from pathlib import Path

import pytest

from brik2_dp_dispatcher.config import Settings
from brik2_dp_dispatcher.dispatcher import Dispatcher
from brik2_dp_dispatcher.jobs import JobObservation, JobPhase, JobRejected, JobRequest, hash_label
from brik2_dp_dispatcher.state import StateStore


class FakeJobLauncher:
    """In-memory stand-in for the Kubernetes launcher."""

    def __init__(self) -> None:
        self.jobs: dict[str, JobObservation] = {}
        self.requests: dict[str, JobRequest] = {}
        self.created: list[str] = []
        self.deleted: list[str] = []
        self.reject_with: str | None = None
        self.fail_with: Exception | None = None

    def launch(self, request: JobRequest) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        if self.reject_with is not None:
            raise JobRejected(self.reject_with)
        if request.name in self.jobs:
            return
        self.created.append(request.name)
        self.requests[request.name] = request
        label = hash_label(request.items[0].content_hash)
        self.jobs[request.name] = JobObservation(request.name, JobPhase.ACTIVE, label)

    def list_jobs(self) -> dict[str, JobObservation]:
        return dict(self.jobs)

    def delete(self, name: str) -> None:
        self.deleted.append(name)
        self.jobs.pop(name, None)

    def finish(self, name: str, phase: JobPhase, reason: str | None = None) -> None:
        job = self.jobs[name]
        self.jobs[name] = JobObservation(name, phase, job.content_hash_label, reason)

    @property
    def active(self) -> list[str]:
        return [name for name, job in self.jobs.items() if job.phase is JobPhase.ACTIVE]


def write(root: Path, rel: str, data: bytes) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


@pytest.fixture
def watch_dir(tmp_path: Path) -> Path:
    path = tmp_path / "incoming"
    path.mkdir()
    return path


@pytest.fixture
def settings(tmp_path: Path, watch_dir: Path) -> Settings:
    return Settings(
        watch_dir=watch_dir,
        db_path=tmp_path / "state" / "state.db",
        stable_scans=1,
        job_mount_path="/mnt/brik/incoming",
        _env_file=None,  # type: ignore[call-arg]
    )


@pytest.fixture
def store(settings: Settings):
    with StateStore(settings.db_path) as s:
        yield s


@pytest.fixture
def launcher() -> FakeJobLauncher:
    return FakeJobLauncher()


@pytest.fixture
def dispatcher(settings: Settings, store: StateStore, launcher: FakeJobLauncher) -> Dispatcher:
    return Dispatcher(settings, store, launcher)
