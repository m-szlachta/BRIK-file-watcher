from __future__ import annotations

import json
import logging
import signal
import threading
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError

from brik2_dp_dispatcher.config import Settings, load_settings
from brik2_dp_dispatcher.dispatcher import HEARTBEAT_KEY, Dispatcher
from brik2_dp_dispatcher.logging_setup import configure_logging
from brik2_dp_dispatcher.state import FileRecord, StateStore, Status

app = typer.Typer(help="Watch a directory and dispatch brik data processing Jobs to Kubernetes.", no_args_is_help=True)
log = logging.getLogger("brik2_dp_dispatcher")


class Context:
    def __init__(self) -> None:
        self.overrides: dict[str, object] = {}

    def settings(self, **extra: object) -> Settings:
        try:
            settings = load_settings(**self.overrides, **extra)
        except ValidationError as exc:
            typer.echo(f"invalid configuration:\n{exc}", err=True)
            raise typer.Exit(2) from None
        configure_logging(settings.log_level, settings.log_format)
        return settings


def _store(settings: Settings, *, create: bool = False) -> StateStore:
    try:
        return StateStore(settings.db_path, create=create)
    except FileNotFoundError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from None


def _resolve(store: StateStore, prefix: str) -> FileRecord:
    matches = store.find_by_prefix(prefix)
    if not matches:
        typer.echo(f"no file with hash prefix {prefix!r}", err=True)
        raise typer.Exit(1)
    if len(matches) > 1:
        typer.echo(f"hash prefix {prefix!r} is ambiguous ({len(matches)} matches)", err=True)
        raise typer.Exit(1)
    return matches[0]


def _launcher(settings: Settings):
    from kubernetes.config import ConfigException

    from brik2_dp_dispatcher.kubernetes_launcher import KubernetesJobLauncher

    try:
        return KubernetesJobLauncher(settings)
    except (ValueError, OSError, ConfigException) as exc:
        typer.echo(f"cannot set up Kubernetes access: {exc}", err=True)
        raise typer.Exit(2) from None


@app.callback()
def main_options(
    ctx: typer.Context,
    watch_dir: Annotated[Path | None, typer.Option(help="Watched directory [env: BRIK_DISPATCH_WATCH_DIR]")] = None,
    db: Annotated[Path | None, typer.Option(help="SQLite state database [env: BRIK_DISPATCH_DB_PATH]")] = None,
    log_level: Annotated[str | None, typer.Option(help="Log level [env: BRIK_DISPATCH_LOG_LEVEL]")] = None,
    log_format: Annotated[str | None, typer.Option(help="text or json [env: BRIK_DISPATCH_LOG_FORMAT]")] = None,
) -> None:
    context = Context()
    context.overrides = {"watch_dir": watch_dir, "db_path": db, "log_level": log_level, "log_format": log_format}
    ctx.obj = context


@app.command()
def run(
    ctx: typer.Context,
    poll_interval: Annotated[float | None, typer.Option(help="Seconds between scans")] = None,
    max_concurrent_jobs: Annotated[int | None, typer.Option(help="Upper bound on running Jobs")] = None,
) -> None:
    """Run the dispatcher loop until SIGTERM/SIGINT."""
    settings = ctx.obj.settings(poll_interval=poll_interval, max_concurrent_jobs=max_concurrent_jobs)
    launcher = _launcher(settings)
    stop = threading.Event()

    def request_stop(signum: int, _frame: object) -> None:
        log.info("shutdown requested, finishing current iteration", extra={"reason": signal.Signals(signum).name})
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    log.info("dispatcher starting", extra={"path": str(settings.watch_dir)})
    with _store(settings, create=True) as store:
        Dispatcher(settings, store, launcher).run_forever(stop)


@app.command()
def scan(
    ctx: typer.Context,
    once: Annotated[bool, typer.Option("--once", help="Single pass (the only mode; use 'run' for the loop)")] = True,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Only print what would be launched")] = False,
) -> None:
    """Run a single iteration: sync Jobs, scan, launch."""
    settings = ctx.obj.settings()
    if dry_run:
        exists = settings.db_path.exists()
        with StateStore(settings.db_path if exists else Path(":memory:")) as store:
            plan = Dispatcher(settings, store, None).dry_run()
        for request in plan.requests:
            for item in request.items:
                typer.echo(f"would launch {request.name}: {item.local_path} -> {item.job_path}")
        typer.echo(
            f"{len(plan.requests)} job(s) would be launched ({plan.free_slots} free slot(s)), "
            f"{len(plan.new_files)} new file(s), {len(plan.unstable)} not yet stable"
        )
        return
    with _store(settings, create=True) as store:
        dispatcher = Dispatcher(settings, store, _launcher(settings))
        dispatcher.reconcile()
        report = dispatcher.run_once()
    typer.echo(f"launched {len(report.launched)}, finished {report.finished}, new files {report.new_files}")
    if report.errors:
        raise typer.Exit(1)


@app.command()
def status(ctx: typer.Context) -> None:
    """Number of tracked files per status."""
    settings = ctx.obj.settings()
    with _store(settings) as store:
        for name, count in store.count_by_status().items():
            typer.echo(f"{name:<20}{count}")
        typer.echo(f"{'last iteration':<20}{store.get_meta(HEARTBEAT_KEY) or 'never'}")


@app.command("list")
def list_files(
    ctx: typer.Context,
    status: Annotated[Status | None, typer.Option(help="Only files with this status")] = None,
    as_json: Annotated[bool, typer.Option("--json", help="Machine-readable output")] = False,
) -> None:
    """List tracked files."""
    settings = ctx.obj.settings()
    with _store(settings) as store:
        records = store.list_files(status)
    if as_json:
        typer.echo(json.dumps([asdict(r) for r in records], indent=2))
        return
    typer.echo(f"{'HASH':<14}{'STATUS':<20}{'TRIES':<7}{'PATH'}")
    for r in records:
        error = f"  [{r.last_error[:80]}]" if r.last_error and r.status is not Status.DONE else ""
        typer.echo(f"{r.content_hash[:12]:<14}{r.status:<20}{r.attempts:<7}{r.last_seen_path}{error}")


@app.command()
def show(ctx: typer.Context, hash_prefix: Annotated[str, typer.Argument(help="Content hash or unique prefix")]) -> None:
    """Details and path history of one file."""
    settings = ctx.obj.settings()
    with _store(settings) as store:
        record = _resolve(store, hash_prefix)
        paths = store.paths(record.content_hash)
    for key, value in asdict(record).items():
        typer.echo(f"{key:<16}{value if value is not None else '-'}")
    typer.echo("paths:")
    for p in paths:
        typer.echo(f"  {p.path}  (first seen {p.first_seen_at}, last seen {p.last_seen_at})")


@app.command()
def retry(
    ctx: typer.Context,
    hash_prefix: Annotated[str | None, typer.Argument(help="Content hash or unique prefix")] = None,
    all_failed: Annotated[bool, typer.Option("--all-failed", help="Reset every error/failed_permanently file")] = False,
) -> None:
    """Reset files to pending with a fresh attempt budget."""
    if bool(hash_prefix) == all_failed:
        typer.echo("give either a hash prefix or --all-failed", err=True)
        raise typer.Exit(2)
    settings = ctx.obj.settings()
    with _store(settings) as store:
        if all_failed:
            hashes = [r.content_hash for s in (Status.ERROR, Status.FAILED_PERMANENTLY) for r in store.list_files(s)]
        else:
            record = _resolve(store, hash_prefix or "")
            if record.status is Status.PROCESSING:
                typer.echo(f"{record.content_hash[:12]} is processing; not reset", err=True)
                raise typer.Exit(1)
            hashes = [record.content_hash]
        count = store.reset(hashes)
    typer.echo(f"reset {count} file(s) to pending")


@app.command()
def health(
    ctx: typer.Context,
    max_age: Annotated[
        float | None, typer.Option(help="Max seconds since last iteration [default: 3x poll + 60]")
    ] = None,
) -> None:
    """Exit non-zero when the service loop has not completed an iteration recently (container healthcheck)."""
    settings = ctx.obj.settings()
    limit = max_age if max_age is not None else 3 * settings.poll_interval + 60
    with _store(settings) as store:
        last = store.get_meta(HEARTBEAT_KEY)
    if last is None:
        typer.echo("no iteration recorded yet", err=True)
        raise typer.Exit(1)
    age = (datetime.now(UTC) - datetime.fromisoformat(last)).total_seconds()
    if age > limit:
        typer.echo(f"last iteration {age:.0f}s ago (limit {limit:.0f}s)", err=True)
        raise typer.Exit(1)
    typer.echo(f"ok, last iteration {age:.0f}s ago")


def main() -> None:
    app()
