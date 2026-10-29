from __future__ import annotations

import functools
import logging
import threading
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import PurePosixPath

from brik2_dp_dispatcher.config import Settings
from brik2_dp_dispatcher.jobs import (
    JobItem,
    JobLauncher,
    JobObservation,
    JobPhase,
    JobRejected,
    JobRequest,
    hash_label,
    job_name,
    translate_path,
)
from brik2_dp_dispatcher.scanner import ScannedFile, ScanResult, scan, sha256_file
from brik2_dp_dispatcher.state import FileRecord, Sighting, StateStore, Status, utcnow

log = logging.getLogger(__name__)

HEARTBEAT_KEY = "last_iteration_at"


@dataclass
class IterationReport:
    finished: int = 0
    released: int = 0
    new_files: int = 0
    launched: list[JobRequest] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class DryRun:
    requests: list[JobRequest]
    new_files: list[ScannedFile]
    unstable: list[PurePosixPath]
    free_slots: int


def plan_batches(files: Sequence[FileRecord]) -> list[list[FileRecord]]:
    """Group launchable files into Jobs. One Job per file; a batch mode would group here."""
    return [[record] for record in files]


class Dispatcher:
    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        launcher: JobLauncher | None,
        *,
        clock: Callable[[], str] = utcnow,
    ) -> None:
        self.settings = settings
        self.store = store
        self.launcher = launcher
        self.clock = clock
        self.hasher = functools.partial(sha256_file, chunk_size=settings.hash_chunk_size)

    def _require_launcher(self) -> JobLauncher:
        if self.launcher is None:
            raise RuntimeError("a Kubernetes launcher is required for this operation")
        return self.launcher

    # Job monitoring ------------------------------------------------------------------------------------------

    def reconcile(self) -> IterationReport:
        """Startup recovery: resolve every ``processing`` record against the Jobs present in the cluster."""
        report = IterationReport()
        jobs = self._require_launcher().list_jobs()
        tracked = self.store.active_job_names()
        for observation in jobs.values():
            if observation.name not in tracked:
                log.warning(
                    "cluster has a managed Job that is not tracked as processing",
                    extra={"job_name": observation.name, "status": observation.phase},
                )
        log.info("reconciling processing files", extra={"count": len(tracked)})
        self._sync(jobs, report)
        return report

    def sync_jobs(self, report: IterationReport | None = None) -> IterationReport:
        report = report or IterationReport()
        if self.store.active_job_names():
            self._sync(self._require_launcher().list_jobs(), report)
        return report

    def _sync(self, jobs: Mapping[str, JobObservation], report: IterationReport) -> None:
        by_hash: dict[str, list[JobObservation]] = defaultdict(list)
        for observation in jobs.values():
            if observation.content_hash_label:
                by_hash[observation.content_hash_label].append(observation)

        records: dict[str, list[FileRecord]] = defaultdict(list)
        for record in self.store.processing():
            assert record.job_name, "claim() always records the job name"
            records[record.job_name].append(record)

        for name, members in records.items():
            observation = jobs.get(name)
            if observation is None:
                # Fall back to the content-hash label in case the recorded name and the cluster disagree.
                candidates = by_hash.get(hash_label(members[0].content_hash), [])
                observation = candidates[0] if len(candidates) == 1 else None
            self._apply(name, members, observation, report)

    def _apply(
        self, name: str, members: list[FileRecord], observation: JobObservation | None, report: IterationReport
    ) -> None:
        context = {"job_name": name, "content_hash": members[0].content_hash}
        now = self.clock()
        if observation is None:
            report.released += self.store.release(name, "job not found in cluster")
            log.warning("job missing from cluster, file returned to pending", extra=context)
            return
        if observation.phase is JobPhase.ACTIVE:
            return
        if observation.phase is JobPhase.SUCCEEDED:
            report.finished += self.store.mark_done(name, now=now)
            log.info("job succeeded", extra=context)
        else:
            reason = observation.reason or "job failed"
            report.finished += self.store.mark_failed(name, reason, self.settings.max_attempts, now=now)
            record = self.store.get(members[0].content_hash)
            log.warning(
                "job failed",
                extra={**context, "reason": reason, "status": record.status if record else None},
            )
        try:
            self._require_launcher().delete(observation.name)
        except Exception as exc:  # TTL cleans up eventually; a failed delete must not block the loop.
            log.warning("could not delete finished job", extra={**context, "reason": str(exc)})

    # Scanning ------------------------------------------------------------------------------------------------

    def scan(self) -> ScanResult:
        settings = self.settings
        return scan(
            settings.watch_dir,
            self.store.observations(),
            stable_scans=settings.stable_scans,
            ignore_suffixes=settings.ignore_suffixes,
            ignore_hidden=settings.ignore_hidden,
            hasher=self.hasher,
        )

    def register(self, result: ScanResult, report: IterationReport | None = None) -> dict[str, ScannedFile]:
        """Persist a scan and return the stable files present now, one per content hash."""
        report = report or IterationReport()
        now = self.clock()
        present: dict[str, ScannedFile] = {}
        for scanned in result.stable:
            path = str(scanned.path)
            context = {"content_hash": scanned.content_hash, "path": path}
            first = present.setdefault(scanned.content_hash, scanned)
            if first is not scanned:
                log.info(
                    "identical content at another path, treated as one file",
                    extra={**context, "same_as": str(first.path)},
                )
            sighting = self.store.register_sighting(scanned.content_hash, scanned.size, path, now=now)
            if sighting is Sighting.NEW:
                report.new_files += 1
                log.info("new file", extra=context)
            elif sighting is Sighting.NEW_PATH:
                record = self.store.get(scanned.content_hash)
                status = record.status if record else None
                log.info("known content seen at a new path, not processed again", extra={**context, "status": status})

        present_paths = {str(s.path) for s in result.stable}
        for content_hash, scanned in present.items():
            record = self.store.get(content_hash)
            # Keep the current path if it is still there (duplicates); otherwise follow the file to where it is now.
            path = record.last_seen_path if record and record.last_seen_path in present_paths else str(scanned.path)
            self.store.touch(content_hash, path, now=now)
        self.store.replace_observations(result.observations)
        return present

    # Launching -----------------------------------------------------------------------------------------------

    def build_request(self, members: Sequence[FileRecord]) -> JobRequest:
        first = members[0]
        attempt = first.attempts + 1
        items = tuple(
            JobItem(
                content_hash=record.content_hash,
                local_path=PurePosixPath(record.last_seen_path),
                job_path=translate_path(record.last_seen_path, self.settings.watch_dir, self.settings.job_mount_path),
            )
            for record in members
        )
        return JobRequest(job_name(self.settings.job_name_prefix, first.content_hash, attempt), attempt, items)

    def pending_requests(self, present: dict[str, ScannedFile], free_slots: int) -> list[JobRequest]:
        candidates = [r for r in self.store.launchable(self.settings.max_attempts) if r.content_hash in present]
        return [self.build_request(batch) for batch in plan_batches(candidates)[: max(free_slots, 0)]]

    def free_slots(self) -> int:
        return self.settings.max_concurrent_jobs - len(self.store.active_job_names())

    def launch_pending(self, present: dict[str, ScannedFile], report: IterationReport | None = None) -> IterationReport:
        report = report or IterationReport()
        launcher = self._require_launcher()
        for request in self.pending_requests(present, self.free_slots()):
            context = {"job_name": request.name, "content_hash": request.items[0].content_hash}
            # Committed before the API call: a crash in between leaves a record that reconcile resolves.
            self.store.claim(request.content_hashes, request.name, now=self.clock())
            try:
                launcher.launch(request)
            except JobRejected as exc:
                self.store.mark_failed(
                    request.name, f"job rejected: {exc}", self.settings.max_attempts, now=self.clock()
                )
                log.error("cluster rejected job", extra={**context, "reason": str(exc)})
                report.errors.append(str(exc))
                continue
            except Exception as exc:
                # Outcome unknown (e.g. timeout); the next sync finds the Job or returns the file to pending.
                log.error("job creation failed, will verify on next sync", extra={**context, "reason": str(exc)})
                report.errors.append(str(exc))
                break
            report.launched.append(request)
            log.info(
                "job launched",
                extra={**context, "path": str(request.items[0].job_path), "attempt": request.attempt},
            )
        return report

    def dry_run(self) -> DryRun:
        """What the next iteration would launch, computed without writing state or calling the cluster."""
        result = self.scan()
        now = self.clock()
        present: dict[str, ScannedFile] = {}
        for scanned in result.stable:
            present.setdefault(scanned.content_hash, scanned)
        new_files = [s for h, s in present.items() if self.store.get(h) is None]
        candidates = [r for r in self.store.launchable(self.settings.max_attempts) if r.content_hash in present]
        candidates += [
            FileRecord(s.content_hash, s.size, str(s.path), now, now, Status.PENDING, 0, None, None, None, None)
            for s in new_files
        ]
        free = self.free_slots()
        requests = [self.build_request(batch) for batch in plan_batches(candidates)[: max(free, 0)]]
        return DryRun(requests, new_files, result.unstable, free)

    # Loop ----------------------------------------------------------------------------------------------------

    def run_once(self) -> IterationReport:
        report = IterationReport()
        for phase in (self.sync_jobs, self._scan_and_launch):
            try:
                phase(report)
            except Exception as exc:
                log.exception("iteration phase failed", extra={"phase": phase.__name__})
                report.errors.append(str(exc))
        self.store.set_meta(HEARTBEAT_KEY, self.clock())
        return report

    def _scan_and_launch(self, report: IterationReport) -> None:
        present = self.register(self.scan(), report)
        self.launch_pending(present, report)

    def run_forever(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                self.reconcile()
                break
            except Exception:
                log.exception("startup reconciliation failed, retrying")
                stop.wait(self.settings.poll_interval)
        while not stop.is_set():
            self.run_once()
            stop.wait(self.settings.poll_interval)
        log.info("dispatcher stopped")
