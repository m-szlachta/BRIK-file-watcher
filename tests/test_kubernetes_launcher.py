from pathlib import PurePosixPath
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kubernetes import client
from kubernetes.client.exceptions import ApiException

from brik2_dp_dispatcher.config import Settings
from brik2_dp_dispatcher.jobs import LABEL_CONTENT_HASH, JobItem, JobPhase, JobRejected, JobRequest
from brik2_dp_dispatcher.kubernetes_launcher import KubernetesJobLauncher, build_job, observe_job

HASH = "f" * 64


def make_settings(**overrides) -> Settings:
    values = dict(
        namespace="brik",
        job_image="registry.local/brik-dp:1.0",
        job_nfs_server="nfs.local",
        job_nfs_path="/export/incoming",
        job_mount_path="/mnt/in",
        job_args=["--input", "{input_path}"],
        job_env={"RESULTS_DIR": "/mnt/out"},
        _env_file=None,
    )
    return Settings(**{**values, **overrides})


def make_request() -> JobRequest:
    item = JobItem(HASH, PurePosixPath("a/x.laz"), PurePosixPath("/mnt/in/a/x.laz"))
    return JobRequest("brik-dp-ffffffffffff-a2", 2, (item,))


def test_build_job_spec():
    job = build_job(make_settings(), make_request())
    spec = job.spec
    assert spec.backoff_limit == 0
    assert spec.active_deadline_seconds == 21600
    assert spec.ttl_seconds_after_finished == 3600
    labels = job.metadata.labels
    assert labels[LABEL_CONTENT_HASH] == HASH[:63] and len(labels[LABEL_CONTENT_HASH]) <= 63
    assert labels["brik.dispatch/attempt"] == "2"
    assert spec.template.metadata.labels == labels
    pod = spec.template.spec
    assert pod.restart_policy == "Never"
    container = pod.containers[0]
    assert container.args == ["--input", "/mnt/in/a/x.laz"]
    env = {e.name: e.value for e in container.env}
    assert env["BRIK_INPUT_PATH"] == "/mnt/in/a/x.laz"
    assert env["BRIK_CONTENT_HASH"] == HASH and env["RESULTS_DIR"] == "/mnt/out"
    assert container.resources.limits["nvidia.com/gpu"] == "1"
    assert container.volume_mounts[0].mount_path == "/mnt/in" and container.volume_mounts[0].read_only
    assert pod.volumes[0].nfs.server == "nfs.local" and pod.volumes[0].nfs.read_only
    # The manifest must serialise cleanly for the API.
    assert client.ApiClient().sanitize_for_serialization(job)["spec"]["backoffLimit"] == 0


def test_build_job_with_pvc_and_no_gpu():
    settings = make_settings(job_nfs_server=None, job_nfs_path=None, job_pvc_name="brik-incoming", job_gpu_count=0)
    pod = build_job(settings, make_request()).spec.template.spec
    assert pod.volumes[0].persistent_volume_claim.claim_name == "brik-incoming"
    assert "nvidia.com/gpu" not in (pod.containers[0].resources.limits or {})


def test_build_job_requires_volume():
    with pytest.raises(ValueError):
        build_job(make_settings(job_nfs_server=None, job_nfs_path=None), make_request())


def job_with(conditions, name="j"):
    return client.V1Job(
        metadata=client.V1ObjectMeta(name=name, labels={LABEL_CONTENT_HASH: "abc"}),
        status=client.V1JobStatus(conditions=conditions),
    )


def cond(type_, reason=None, message=None, status="True"):
    return client.V1JobCondition(type=type_, status=status, reason=reason, message=message)


def test_observe_job_phases():
    assert observe_job(job_with(None)).phase is JobPhase.ACTIVE
    assert observe_job(job_with([cond("Complete")])).phase is JobPhase.SUCCEEDED
    failed = observe_job(job_with([cond("Failed", "DeadlineExceeded", "too long")]))
    assert failed.phase is JobPhase.FAILED and failed.reason == "DeadlineExceeded: too long"
    assert observe_job(job_with([cond("Failed", status="False")])).phase is JobPhase.ACTIVE
    assert observe_job(job_with(None)).content_hash_label == "abc"


@pytest.fixture
def launcher():
    instance = KubernetesJobLauncher(make_settings(), api=client.ApiClient())
    instance.batch = MagicMock()
    instance.core = MagicMock()
    return instance


def test_launch_treats_conflict_as_success(launcher):
    launcher.batch.create_namespaced_job.side_effect = ApiException(status=409, reason="Conflict")
    launcher.launch(make_request())


def test_launch_rejection_and_transient_errors(launcher):
    launcher.batch.create_namespaced_job.side_effect = ApiException(status=422, reason="Unprocessable")
    with pytest.raises(JobRejected):
        launcher.launch(make_request())
    launcher.batch.create_namespaced_job.side_effect = ApiException(status=503, reason="Unavailable")
    with pytest.raises(ApiException):
        launcher.launch(make_request())


def test_list_jobs_paginates_and_adds_pod_detail(launcher):
    pages = [
        SimpleNamespace(items=[job_with(None, "a")], metadata=SimpleNamespace(_continue="t")),
        SimpleNamespace(items=[job_with([cond("Failed", "BackoffLimitExceeded")], "b")], metadata=None),
    ]
    launcher.batch.list_namespaced_job.side_effect = pages
    terminated = client.V1ContainerStateTerminated(exit_code=137, reason="OOMKilled")
    pod = client.V1Pod(
        status=client.V1PodStatus(
            container_statuses=[
                client.V1ContainerStatus(
                    name="c",
                    image="i",
                    image_id="",
                    ready=False,
                    restart_count=0,
                    state=client.V1ContainerState(terminated=terminated),
                )
            ]
        )
    )
    launcher.core.list_namespaced_pod.return_value = SimpleNamespace(items=[pod])
    jobs = launcher.list_jobs()
    assert jobs["a"].phase is JobPhase.ACTIVE
    assert jobs["b"].reason == "BackoffLimitExceeded; container OOMKilled (exit code 137)"
    assert launcher.batch.list_namespaced_job.call_args.kwargs["label_selector"] == (
        "app.kubernetes.io/managed-by=brik2-dp-dispatcher"
    )


def test_delete_uses_background_propagation_and_ignores_missing(launcher):
    launcher.batch.delete_namespaced_job.side_effect = ApiException(status=404)
    launcher.delete("j")
    assert launcher.batch.delete_namespaced_job.call_args.kwargs["propagation_policy"] == "Background"


def test_launcher_requires_image_and_volume():
    with pytest.raises(ValueError, match="job_image"):
        KubernetesJobLauncher(make_settings(job_image=None), api=client.ApiClient())
    with pytest.raises(ValueError, match="volume"):
        KubernetesJobLauncher(make_settings(job_nfs_server=None, job_nfs_path=None), api=client.ApiClient())
