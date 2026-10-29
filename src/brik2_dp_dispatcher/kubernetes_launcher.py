from __future__ import annotations

import json
import logging

from kubernetes import client, config as kube_config
from kubernetes.client.exceptions import ApiException

from brik2_dp_dispatcher.config import APP_NAME, Settings
from brik2_dp_dispatcher.jobs import (
    ANNOTATION_CONTENT_HASHES,
    ANNOTATION_INPUT_PATHS,
    LABEL_APP,
    LABEL_ATTEMPT,
    LABEL_CONTENT_HASH,
    LABEL_MANAGED_BY,
    JobObservation,
    JobPhase,
    JobRejected,
    JobRequest,
    hash_label,
)

log = logging.getLogger(__name__)

INPUT_PATH_ENV = "BRIK_INPUT_PATH"
INPUT_PATHS_ENV = "BRIK_INPUT_PATHS"
CONTENT_HASH_ENV = "BRIK_CONTENT_HASH"
VOLUME_NAME = "brik-input"
JOB_NAME_POD_LABEL = "batch.kubernetes.io/job-name"


def api_client(settings: Settings) -> client.ApiClient:
    """Kubernetes client from a kubeconfig file, an explicit API host + token, or the in-cluster service account."""
    if settings.kubeconfig:
        return kube_config.new_client_from_config(config_file=str(settings.kubeconfig), context=settings.kube_context)
    if settings.api_host:
        token = settings.api_token
        if settings.api_token_file:
            token = settings.api_token_file.read_text().strip()
        if not token:
            raise ValueError("api_host requires api_token_file or api_token")
        configuration = client.Configuration()
        configuration.host = settings.api_host
        configuration.api_key = {"authorization": token}
        configuration.api_key_prefix = {"authorization": "Bearer"}
        configuration.verify_ssl = settings.api_verify_ssl
        if settings.api_ca_cert:
            configuration.ssl_ca_cert = str(settings.api_ca_cert)
        return client.ApiClient(configuration)
    kube_config.load_incluster_config()
    return client.ApiClient()


def _resources(settings: Settings) -> client.V1ResourceRequirements:
    requests = {"cpu": settings.job_cpu_request, "memory": settings.job_memory_request}
    limits = {"cpu": settings.job_cpu_limit, "memory": settings.job_memory_limit}
    if settings.job_gpu_count:
        # Extended resources such as GPUs must be specified in limits; requests default to the same value.
        limits[settings.job_gpu_resource] = str(settings.job_gpu_count)
    return client.V1ResourceRequirements(
        requests={k: v for k, v in requests.items() if v} or None,
        limits={k: v for k, v in limits.items() if v} or None,
    )


def _volume(settings: Settings) -> client.V1Volume:
    if settings.job_pvc_name:
        return client.V1Volume(
            name=VOLUME_NAME,
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=settings.job_pvc_name, read_only=True
            ),
        )
    if settings.job_nfs_server and settings.job_nfs_path:
        return client.V1Volume(
            name=VOLUME_NAME,
            nfs=client.V1NFSVolumeSource(server=settings.job_nfs_server, path=settings.job_nfs_path, read_only=True),
        )
    raise ValueError("configure job_nfs_server + job_nfs_path or job_pvc_name for the Job input volume")


def _substitute(values: list[str], input_path: str) -> list[str] | None:
    return [value.replace("{input_path}", input_path) for value in values] or None


def build_job(settings: Settings, request: JobRequest) -> client.V1Job:
    if not settings.job_image:
        raise ValueError("job_image must be set to launch Jobs")
    first = request.items[0]
    input_path = str(first.job_path)
    labels = {
        LABEL_APP: "brik-data-processing",
        LABEL_MANAGED_BY: APP_NAME,
        LABEL_CONTENT_HASH: hash_label(first.content_hash),
        LABEL_ATTEMPT: str(request.attempt),
    }
    annotations = {
        ANNOTATION_CONTENT_HASHES: ",".join(request.content_hashes),
        ANNOTATION_INPUT_PATHS: json.dumps([str(item.local_path) for item in request.items]),
    }
    env = [
        client.V1EnvVar(name=INPUT_PATH_ENV, value=input_path),
        client.V1EnvVar(name=INPUT_PATHS_ENV, value="\n".join(str(item.job_path) for item in request.items)),
        client.V1EnvVar(name=CONTENT_HASH_ENV, value=first.content_hash),
        *(client.V1EnvVar(name=k, value=v) for k, v in sorted(settings.job_env.items())),
    ]
    container = client.V1Container(
        name="brik-data-processing",
        image=settings.job_image,
        image_pull_policy=settings.job_image_pull_policy,
        command=_substitute(settings.job_command, input_path),
        args=_substitute(settings.job_args, input_path),
        env=env,
        resources=_resources(settings),
        volume_mounts=[client.V1VolumeMount(name=VOLUME_NAME, mount_path=settings.job_mount_path, read_only=True)],
    )
    pod_spec = client.V1PodSpec(
        restart_policy="Never",
        containers=[container],
        volumes=[_volume(settings)],
        service_account_name=settings.job_service_account,
        node_selector=settings.job_node_selector or None,
        image_pull_secrets=(
            [client.V1LocalObjectReference(name=settings.job_image_pull_secret)]
            if settings.job_image_pull_secret
            else None
        ),
    )
    return client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=request.name, namespace=settings.namespace, labels=labels, annotations=annotations
        ),
        spec=client.V1JobSpec(
            backoff_limit=0,
            active_deadline_seconds=settings.job_active_deadline_seconds,
            ttl_seconds_after_finished=settings.job_ttl_seconds_after_finished,
            template=client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels=labels), spec=pod_spec),
        ),
    )


def observe_job(job: client.V1Job) -> JobObservation:
    metadata = job.metadata
    if metadata is None or not metadata.name:
        raise ValueError("Job without a name in API response")
    name: str = metadata.name
    label = (metadata.labels or {}).get(LABEL_CONTENT_HASH)
    for condition in (job.status and job.status.conditions) or []:
        if condition.status != "True":
            continue
        if condition.type == "Complete":
            return JobObservation(name, JobPhase.SUCCEEDED, label)
        if condition.type == "Failed":
            reason = ": ".join(part for part in (condition.reason, condition.message) if part) or "Failed"
            return JobObservation(name, JobPhase.FAILED, label, reason)
    return JobObservation(name, JobPhase.ACTIVE, label)


class KubernetesJobLauncher:
    def __init__(self, settings: Settings, api: client.ApiClient | None = None) -> None:
        if not settings.job_image:
            raise ValueError("job_image must be set to launch Jobs")
        _volume(settings)  # fail at startup rather than on the first file
        self.settings = settings
        api = api or api_client(settings)
        self.batch = client.BatchV1Api(api)
        self.core = client.CoreV1Api(api)

    @property
    def selector(self) -> str:
        return f"{LABEL_MANAGED_BY}={APP_NAME}"

    def launch(self, request: JobRequest) -> None:
        job = build_job(self.settings, request)
        try:
            self.batch.create_namespaced_job(self.settings.namespace, job)
        except ApiException as exc:
            if exc.status == 409:
                log.info("job already exists", extra={"job_name": request.name})
                return
            if exc.status is not None and 400 <= exc.status < 500 and exc.status not in (408, 429):
                raise JobRejected(f"{exc.status} {exc.reason}: {_message(exc)}") from exc
            raise

    def list_jobs(self) -> dict[str, JobObservation]:
        jobs: dict[str, JobObservation] = {}
        token = None
        while True:
            page = self.batch.list_namespaced_job(
                self.settings.namespace, label_selector=self.selector, limit=500, _continue=token
            )
            for job in page.items:
                observation = observe_job(job)
                if observation.phase is JobPhase.FAILED:
                    observation = JobObservation(
                        observation.name,
                        observation.phase,
                        observation.content_hash_label,
                        _join(observation.reason, self._pod_failure(observation.name)),
                    )
                jobs[observation.name] = observation
            token = page.metadata._continue if page.metadata else None
            if not token:
                return jobs

    def delete(self, name: str) -> None:
        try:
            self.batch.delete_namespaced_job(name, self.settings.namespace, propagation_policy="Background")
        except ApiException as exc:
            if exc.status != 404:
                raise

    def _pod_failure(self, job_name: str) -> str | None:
        """Best-effort container termination detail (e.g. OOMKilled, exit code) for a failed Job."""
        try:
            pods = self.core.list_namespaced_pod(
                self.settings.namespace, label_selector=f"{JOB_NAME_POD_LABEL}={job_name}"
            ).items
        except ApiException:
            return None
        for pod in pods:
            for status in (pod.status and pod.status.container_statuses) or []:
                terminated = status.state and status.state.terminated
                if terminated and terminated.exit_code:
                    return f"container {terminated.reason or 'terminated'} (exit code {terminated.exit_code})"
        return None


def _join(*parts: str | None) -> str:
    return "; ".join(part for part in parts if part)


def _message(exc: ApiException) -> str:
    try:
        return json.loads(exc.body or "{}").get("message", "")
    except (ValueError, AttributeError):
        return str(exc.body or "")
