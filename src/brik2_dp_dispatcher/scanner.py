from __future__ import annotations

import hashlib
import logging
import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from brik2_dp_dispatcher.state import Observation

log = logging.getLogger(__name__)

Hasher = Callable[[Path], str]


@dataclass(frozen=True)
class ScannedFile:
    """A stable file present in the watched directory. ``path`` is relative to the watched root."""

    path: PurePosixPath
    size: int
    mtime_ns: int
    content_hash: str


@dataclass
class ScanResult:
    stable: list[ScannedFile] = field(default_factory=list)
    unstable: list[PurePosixPath] = field(default_factory=list)
    observations: list[Observation] = field(default_factory=list)
    hashed: int = 0


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def is_ignored(name: str, ignore_suffixes: Sequence[str], ignore_hidden: bool) -> bool:
    if ignore_hidden and name.startswith("."):
        return True
    return name.lower().endswith(tuple(ignore_suffixes))


def walk_files(
    root: Path, ignore_suffixes: Sequence[str], ignore_hidden: bool
) -> Iterator[tuple[Path, os.stat_result]]:
    """Yield regular files below ``root``; symlinks are not followed, ignored directories are pruned."""
    stack = [root]
    while stack:
        directory = stack.pop()
        try:
            entries = sorted(os.scandir(directory), key=lambda e: e.name)
        except OSError as exc:
            log.warning("cannot list directory", extra={"path": str(directory), "reason": str(exc)})
            continue
        subdirectories = []
        for entry in entries:
            if is_ignored(entry.name, ignore_suffixes, ignore_hidden):
                continue
            try:
                if entry.is_dir(follow_symlinks=False):
                    subdirectories.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    yield Path(entry.path), entry.stat(follow_symlinks=False)
            except OSError as exc:
                log.warning("cannot stat file", extra={"path": entry.path, "reason": str(exc)})
        stack.extend(reversed(subdirectories))


def scan(
    root: Path,
    previous: Mapping[str, Observation],
    *,
    stable_scans: int = 2,
    ignore_suffixes: Sequence[str] = (".part", ".tmp"),
    ignore_hidden: bool = True,
    hasher: Hasher = sha256_file,
) -> ScanResult:
    """Scan ``root`` once, comparing against the ``previous`` scan's observations.

    A file is stable once its (size, mtime_ns) has been identical for ``stable_scans`` consecutive scans. Only
    stable files are hashed, and a hash is reused while (path, size, mtime_ns) is unchanged.
    """
    result = ScanResult()
    if not root.is_dir():
        raise FileNotFoundError(f"watched directory does not exist: {root}")
    for absolute, stat in walk_files(root, ignore_suffixes, ignore_hidden):
        rel = PurePosixPath(absolute.relative_to(root).as_posix())
        key = str(rel)
        before = previous.get(key)
        unchanged = before is not None and before.size == stat.st_size and before.mtime_ns == stat.st_mtime_ns
        stable_count = before.stable_count + 1 if unchanged and before else 1
        content_hash = before.content_hash if unchanged and before else None

        if stable_count >= stable_scans and content_hash is None:
            try:
                content_hash = hasher(absolute)
                after = absolute.stat()
            except OSError as exc:
                log.warning("cannot hash file", extra={"path": key, "reason": str(exc)})
                continue
            result.hashed += 1
            if (after.st_size, after.st_mtime_ns) != (stat.st_size, stat.st_mtime_ns):
                # Changed while being read: start the stability check again from the new state.
                content_hash = None
                stable_count = 1
                stat = after

        result.observations.append(Observation(key, stat.st_size, stat.st_mtime_ns, stable_count, content_hash))
        if stable_count >= stable_scans and content_hash is not None:
            result.stable.append(ScannedFile(rel, stat.st_size, stat.st_mtime_ns, content_hash))
        else:
            result.unstable.append(rel)
    return result
