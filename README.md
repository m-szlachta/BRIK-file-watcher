# brik2-dp-dispatcher

An always-running service that watches a directory (an NFS share, mounted read-only) and launches one
**brik data processing** Kubernetes Job on HPE Private Cloud AI (PCAI) for each new file. It records which files
are pending, processing, done or failed in SQLite, retries failures, and recovers cleanly after restarts.

The dispatcher talks only to the Kubernetes API. It never runs containers locally, never needs the Docker socket,
and never modifies, moves or deletes input files.

```
 SLES host (Docker Compose)                         HPE PCAI (Kubernetes)
┌──────────────────────────────┐   K8s API   ┌───────────────────────────────────┐
│ brik2-dp-dispatcher          │ ──────────► │ Job brik-dp-<hash12>-a<attempt>    │
│  /data/incoming (NFS, ro)    │  create /   │  /mnt/brik/incoming (same NFS)     │
│  /var/lib/brik-dispatch (db) │  list/delete│  BRIK_INPUT_PATH=/mnt/brik/...     │
└──────────────────────────────┘             └───────────────────────────────────┘
```

## How it works

Each iteration (every `poll_interval` seconds, default 30):

1. **Sync Jobs.** The dispatcher lists its Jobs (label `app.kubernetes.io/managed-by=brik2-dp-dispatcher`):
   - succeeded → `done`
   - failed or deadline exceeded → `error` with the reason, and `attempts` goes up by 1. Once `attempts` reaches
     `max_attempts` (default 3) the file becomes `failed_permanently`
   - Jobs with a final status are deleted with `Background` propagation, as a fallback to `ttlSecondsAfterFinished`
2. **Scan.** It walks `watch_dir` recursively. It skips hidden files and directories, names ending in
   `.part`/`.tmp`, symlinks, and files whose size or mtime changed since the previous scan (a file must be unchanged
   for `stable_scans` consecutive scans, default 2).
3. **Launch.** Files in `pending` (and `error` files with attempts left) get a Job, but never more than
   `max_concurrent_jobs` (default 2) running at once.

Status lifecycle: `pending → processing → done`, or `processing → error → processing … → failed_permanently`.

### File identity

A file is identified by the **SHA-256 of its content**, not by its name or path.

- Moving a file to another directory doesn't trigger reprocessing. The new location is added to its path
  history, and `last_seen_path` follows it.
- Two files with the same name but different content (e.g. `a/data.laz` and `b/data.laz`) are two separate files.
- **Two files with identical content are one file**, processed once, at whichever path was seen first. This is
  logged at INFO (`identical content at another path`). Copying a file that was already processed therefore does
  **not** process it again. Use `brik-dispatch retry <hash>` to force that. All empty files share one hash.
- Hashes are computed in chunks and cached per `(path, size, mtime_ns)` in the database, so unchanged files are
  hashed once, even across restarts.

### Reliability

- All state changes are single SQLite transactions (WAL mode, `synchronous=FULL`).
- The Job name is deterministic (`<prefix>-<hash12>-a<attempt>`), and the dispatcher commits the file as
  `processing` *before* calling the API. A crash in between is safe: a repeated create hits `AlreadyExists` and
  is treated as success, and a Job that was never created is detected below.
- **On startup** every `processing` record is checked against the cluster (by Job name, falling back to the
  content-hash label):
  - Job still running → keep monitoring
  - Job finished → record the result
  - Job missing → back to `pending` (no attempt counted)
- A Job that the cluster *rejects* (HTTP 4xx, e.g. invalid spec or quota) counts as a failed attempt. Network
  errors and 5xx leave the file `processing`, and the next sync resolves it.
- SIGTERM/SIGINT: the current iteration finishes, then the process exits. Running Jobs are left alone and picked
  up again on the next start.

### What the Job receives

| | |
|---|---|
| `BRIK_INPUT_PATH` | file path **inside the Job**: `job_mount_path` + path relative to `watch_dir` |
| `BRIK_INPUT_PATHS` | newline-separated paths (same as above while there is one file per Job) |
| `BRIK_CONTENT_HASH` | full SHA-256 |
| args / command | `job_args` / `job_command`, with `{input_path}` replaced by the path |
| labels | `app.kubernetes.io/managed-by`, `brik.dispatch/content-hash` (first 63 chars), `brik.dispatch/attempt` |
| annotations | full hash(es) and local relative path(s) |

Example: `/data/incoming/2026/f1/cloud.laz` in the dispatcher becomes `/mnt/brik/incoming/2026/f1/cloud.laz` in
the Job.

Job spec: `backoffLimit: 0` (the dispatcher handles retries itself), `activeDeadlineSeconds`,
`ttlSecondsAfterFinished`, `restartPolicy: Never`, GPU in `limits`, and the input volume mounted read-only.

## Configuration

Settings come from environment variables with the `BRIK_DISPATCH_` prefix. CLI options override them (e.g.
`brik-dispatch --watch-dir /srv/in run --poll-interval 10`). Lists and maps are JSON. Empty values mean "use the
default".

| Variable | Default | Meaning |
|---|---|---|
| `WATCH_DIR` | `/data/incoming` | watched directory |
| `DB_PATH` | `/var/lib/brik-dispatch/state.db` | SQLite state |
| `POLL_INTERVAL` | `30` | seconds between iterations |
| `STABLE_SCANS` | `2` | consecutive unchanged scans before a file is used (1 = immediately) |
| `IGNORE_SUFFIXES` | `[".part", ".tmp"]` | case-insensitive |
| `IGNORE_HIDDEN` | `true` | skip dot-files and dot-directories |
| `HASH_CHUNK_SIZE` | `8388608` | bytes per read when hashing |
| `MAX_CONCURRENT_JOBS` | `2` | upper bound on running Jobs |
| `MAX_ATTEMPTS` | `3` | attempts before `failed_permanently` |
| `NAMESPACE` | `default` | namespace for Jobs |
| `KUBECONFIG` | – | kubeconfig file (e.g. `/run/secrets/kubeconfig`) |
| `KUBE_CONTEXT` | current | context in the kubeconfig |
| `API_HOST`, `API_TOKEN_FILE` / `API_TOKEN`, `API_CA_CERT`, `API_VERIFY_SSL` | – | alternative to a kubeconfig |
| `JOB_IMAGE` | **required** | brik data processing image |
| `JOB_IMAGE_PULL_POLICY` | `IfNotPresent` | |
| `JOB_IMAGE_PULL_SECRET` | – | secret name in the namespace |
| `JOB_SERVICE_ACCOUNT` | – | service account for the Job pods |
| `JOB_COMMAND`, `JOB_ARGS` | `[]` | JSON lists, `{input_path}` placeholder |
| `JOB_ENV` | `{}` | extra env for the container (JSON map) |
| `JOB_NODE_SELECTOR` | `{}` | JSON map |
| `JOB_CPU_REQUEST` / `JOB_CPU_LIMIT` | `2` / – | |
| `JOB_MEMORY_REQUEST` / `JOB_MEMORY_LIMIT` | `8Gi` / `16Gi` | |
| `JOB_GPU_RESOURCE` / `JOB_GPU_COUNT` | `nvidia.com/gpu` / `1` | `0` disables the GPU request |
| `JOB_NFS_SERVER` + `JOB_NFS_PATH` | – | inline NFS volume for the Job… |
| `JOB_PVC_NAME` | – | …or an existing PVC (see below) |
| `JOB_MOUNT_PATH` | `/mnt/brik/incoming` | where the share appears in the Job |
| `JOB_ACTIVE_DEADLINE_SECONDS` | `21600` | Job timeout |
| `JOB_TTL_SECONDS_AFTER_FINISHED` | `3600` | must be at least 2 × `POLL_INTERVAL` |
| `JOB_NAME_PREFIX` | `brik-dp` | |
| `LOG_LEVEL` / `LOG_FORMAT` | `INFO` / `text` | `json` for structured logs |

Credentials are only ever read from mounted files or the environment. None are baked into the image.

## Deployment

### 1. Kubernetes side (PCAI)

`deploy/k8s/rbac.yaml` creates a ServiceAccount whose Role, in one namespace, allows only:

- `batch/jobs`: create, get, list, watch, delete
- `pods`: get, list (adds OOMKilled and exit code to failure reasons)

```sh
sed -i 's/namespace: brik/namespace: <your-namespace>/' deploy/k8s/rbac.yaml
kubectl apply -f deploy/k8s/rbac.yaml
```

Then build `secrets/kubeconfig` from `deploy/k8s/kubeconfig.example.yaml`, using the token and CA certificate of
the `brik2-dp-dispatcher-token` secret.

If the namespace enforces the *restricted* Pod Security profile, inline `nfs` volumes are not allowed. In that
case apply `deploy/k8s/input-volume.yaml` (PV + PVC) and set `BRIK_DISPATCH_JOB_PVC_NAME=brik-incoming`.

### 2. SLES host

```sh
cp .env.example .env           # set namespace, Job image, NFS server/export, host paths
mkdir -p secrets && cp /path/to/kubeconfig secrets/kubeconfig
chmod 0440 secrets/kubeconfig  # readable by DISPATCHER_UID/GID (default 10001)
just build && just up          # or: docker compose up -d
just logs
```

- The container runs as UID/GID 10001. Set `DISPATCHER_UID`/`DISPATCHER_GID` to an identity that can read the NFS
  share (check `root_squash`/`all_squash` on the export).
- If you change the UID, the `dispatcher-state` volume must be writable by it.

The image has a healthcheck (`brik-dispatch health`). It fails when no iteration has completed for
3 × `poll_interval` + 60 s.

## Inspecting state

```sh
just cli status                         # counts per status + time of the last iteration
just cli list --status error            # or --json
just cli show 5891b5b5                  # details and path history for a hash prefix
just cli retry 5891b5b5                 # back to pending, attempts reset
just cli retry --all-failed             # every error/failed_permanently file
just cli scan --once --dry-run          # what would be launched now, writes nothing
```

(`just cli …` runs `docker compose exec dispatcher brik-dispatch …`.) The database is plain SQLite (`files`,
`file_paths`, `observations`, `meta`), so `sqlite3 state.db` works for ad-hoc queries.

## Development

```sh
just setup     # uv sync (Python 3.13, app + test group)
just test      # pytest + coverage, no cluster needed
just lint      # ruff check, ruff format --check, pyright
just fmt
just build     # linux/amd64 image; CONTAINER_ENGINE=podman supported
just push      # DISPATCHER_REGISTRY / DISPATCHER_TAG select the target
just clean     # venv, caches, local state, compose containers and state volume
```

Code map (`src/brik2_dp_dispatcher/`):

| Module | Contents |
|---|---|
| `config.py` | settings |
| `scanner.py` | walk, stability check and hashing |
| `state.py` | SQLite store and migrations |
| `jobs.py` | `JobLauncher` protocol, Job naming, path translation |
| `kubernetes_launcher.py` | real launcher |
| `dispatcher.py` | business logic. `plan_batches()` is where a batch mode (one Job per group of files) would plug in |
| `cli.py` | Typer CLI |

Tests use an in-memory `FakeJobLauncher` (`tests/conftest.py`).
