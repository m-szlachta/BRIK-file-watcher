import json
import logging
import threading

from conftest import write

from brik2_dp_dispatcher.logging_setup import JsonFormatter, TextFormatter
from brik2_dp_dispatcher.state import Status


def test_run_forever_stops_after_current_iteration(watch_dir, dispatcher, launcher, store, settings):
    settings.poll_interval = 0.01
    write(watch_dir, "x.laz", b"x")
    stop = threading.Event()
    original = dispatcher.run_once

    def run_once_then_stop():
        report = original()
        stop.set()  # as the SIGTERM handler would, mid-iteration
        return report

    dispatcher.run_once = run_once_then_stop
    dispatcher.run_forever(stop)
    assert len(launcher.created) == 1
    # Running Jobs are left alone and the state is consistent for the next start.
    assert launcher.active == launcher.created and launcher.deleted == []
    assert store.count_by_status()[Status.PROCESSING] == 1


def test_run_forever_retries_failed_reconcile(dispatcher, launcher, settings):
    settings.poll_interval = 0.01
    stop = threading.Event()
    calls = []

    def flaky_list():
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("api down")
        stop.set()
        return {}

    launcher.list_jobs = flaky_list
    dispatcher.run_forever(stop)
    assert len(calls) == 2


def record(**extra):
    rec = logging.LogRecord("brik", logging.INFO, __file__, 1, "job launched", None, None)
    rec.__dict__.update(extra)
    return rec


def test_json_log_contains_context():
    line = json.loads(JsonFormatter().format(record(content_hash="abc", job_name="brik-dp-abc-a1")))
    assert line["msg"] == "job launched" and line["content_hash"] == "abc" and line["job_name"] == "brik-dp-abc-a1"


def test_text_log_contains_context():
    line = TextFormatter().format(record(content_hash="abc"))
    assert line.endswith("job launched content_hash=abc")
