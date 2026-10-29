from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

APP_NAME = "brik2-dp-dispatcher"
ENV_PREFIX = "BRIK_DISPATCH_"
DEFAULT_WATCH_DIR = Path("/data/incoming")
DEFAULT_DB_PATH = Path("/var/lib/brik-dispatch/state.db")


class Settings(BaseSettings):
    """Runtime configuration; every field can be set as ``BRIK_DISPATCH_<FIELD>``."""

    # env_ignore_empty: Compose passes unset variables as empty strings; those mean "use the default".
    model_config = SettingsConfigDict(env_prefix=ENV_PREFIX, env_ignore_empty=True, extra="ignore")

    # Scanning
    watch_dir: Path = DEFAULT_WATCH_DIR
    db_path: Path = DEFAULT_DB_PATH
    poll_interval: float = Field(30.0, gt=0)
    stable_scans: int = Field(2, ge=1, description="Consecutive scans with unchanged size/mtime before a file is used")
    ignore_suffixes: list[str] = [".part", ".tmp"]
    ignore_hidden: bool = True
    hash_chunk_size: int = Field(8 * 1024 * 1024, gt=0)

    # Dispatching
    max_concurrent_jobs: int = Field(2, ge=1)
    max_attempts: int = Field(3, ge=1)

    # Kubernetes access
    namespace: str = "default"
    kubeconfig: Path | None = None
    kube_context: str | None = None
    api_host: str | None = None
    api_token_file: Path | None = None
    api_token: str | None = None
    api_ca_cert: Path | None = None
    api_verify_ssl: bool = True

    # Job template
    job_name_prefix: str = "brik-dp"
    job_image: str | None = None
    job_image_pull_policy: str = "IfNotPresent"
    job_image_pull_secret: str | None = None
    job_service_account: str | None = None
    job_command: list[str] = []
    job_args: list[str] = []
    job_env: dict[str, str] = {}
    job_node_selector: dict[str, str] = {}
    job_cpu_request: str | None = "2"
    job_memory_request: str | None = "8Gi"
    job_cpu_limit: str | None = None
    job_memory_limit: str | None = "16Gi"
    job_gpu_resource: str = "nvidia.com/gpu"
    job_gpu_count: int = Field(1, ge=0)
    job_nfs_server: str | None = None
    job_nfs_path: str | None = None
    job_pvc_name: str | None = None
    job_mount_path: str = "/mnt/brik/incoming"
    job_active_deadline_seconds: int = Field(6 * 3600, gt=0)
    job_ttl_seconds_after_finished: int = Field(3600, ge=0)

    # Logging
    log_level: str = "INFO"
    log_format: Literal["text", "json"] = "text"

    @model_validator(mode="after")
    def _check(self) -> Settings:
        if self.job_nfs_server and self.job_pvc_name:
            raise ValueError("set either job_nfs_server/job_nfs_path or job_pvc_name, not both")
        if bool(self.job_nfs_server) != bool(self.job_nfs_path):
            raise ValueError("job_nfs_server and job_nfs_path must be set together")
        if not self.job_mount_path.startswith("/"):
            raise ValueError("job_mount_path must be absolute")
        # A Job deleted by its TTL before the dispatcher observed its result would be treated as missing and rerun.
        if self.job_ttl_seconds_after_finished < 2 * self.poll_interval:
            raise ValueError("job_ttl_seconds_after_finished must be at least twice poll_interval")
        self.ignore_suffixes = [s.lower() for s in self.ignore_suffixes]
        return self


def load_settings(**overrides: object) -> Settings:
    """Build settings from the environment, letting non-``None`` CLI overrides win."""
    return Settings(**{k: v for k, v in overrides.items() if v is not None})  # type: ignore[arg-type]
