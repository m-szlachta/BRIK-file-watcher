from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePath, PurePosixPath
from typing import Protocol

LABEL_APP = "app.kubernetes.io/name"
LABEL_MANAGED_BY = "app.kubernetes.io/managed-by"
LABEL_CONTENT_HASH = "brik.dispatch/content-hash"
LABEL_ATTEMPT = "brik.dispatch/attempt"
ANNOTATION_CONTENT_HASHES = "brik.dispatch/content-hashes"
ANNOTATION_INPUT_PATHS = "brik.dispatch/input-paths"

LABEL_VALUE_MAX = 63
JOB_NAME_MAX = 63


class JobPhase(StrEnum):
    ACTIVE = "active"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class JobItem:
    """One input file of a Job: ``local_path`` is relative to the watched root, ``job_path`` is inside the pod."""

    content_hash: str
    local_path: PurePosixPath
    job_path: PurePosixPath


@dataclass(frozen=True)
class JobRequest:
    """A Job to create. Currently always one item; the list shape leaves room for a batch mode."""

    name: str
    attempt: int
    items: tuple[JobItem, ...]

    @property
    def content_hashes(self) -> list[str]:
        return [item.content_hash for item in self.items]


@dataclass(frozen=True)
class JobObservation:
    name: str
    phase: JobPhase
    content_hash_label: str | None = None
    reason: str | None = None


class JobRejected(Exception):
    """The cluster definitively refused to create the Job, so it will not run (unlike a network error)."""


class JobLauncher(Protocol):
    def launch(self, request: JobRequest) -> None:
        """Create the Job. Must succeed silently when a Job with the same name already exists."""
        ...

    def list_jobs(self) -> Mapping[str, JobObservation]:
        """All Jobs managed by this dispatcher, keyed by Job name."""
        ...

    def delete(self, name: str) -> None:
        """Delete a Job and its pods in the background; a missing Job is not an error."""
        ...


def hash_label(content_hash: str) -> str:
    return content_hash[:LABEL_VALUE_MAX]


def job_name(prefix: str, content_hash: str, attempt: int) -> str:
    """Deterministic DNS-1123 Job name, so re-creating a Job after a crash hits ``AlreadyExists``."""
    clean = re.sub(r"[^a-z0-9-]+", "-", prefix.lower()).strip("-") or "job"
    suffix = f"-{content_hash[:12]}-a{attempt}"
    return clean[: JOB_NAME_MAX - len(suffix)].rstrip("-") + suffix


def translate_path(path: PurePath | str, local_root: PurePath | str, job_root: str) -> PurePosixPath:
    """Map a file under the dispatcher's watched root to the same file under the Job's mount path.

    ``path`` may be absolute (under ``local_root``) or already relative to it.
    """
    path = PurePosixPath(PurePath(path).as_posix())
    local_root = PurePosixPath(PurePath(local_root).as_posix())
    if path.is_absolute():
        try:
            path = path.relative_to(local_root)
        except ValueError:
            raise ValueError(f"{path} is not under the watched root {local_root}") from None
    if ".." in path.parts:
        raise ValueError(f"path escapes the watched root: {path}")
    return PurePosixPath(job_root) / path
