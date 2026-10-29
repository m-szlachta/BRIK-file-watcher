set positional-arguments
set dotenv-load := false

export DISPATCHER_REGISTRY := env("DISPATCHER_REGISTRY", "msiniarskiitwl")
export DISPATCHER_TAG := env("DISPATCHER_TAG", "latest")
export DISPATCHER_IMAGE := env("DISPATCHER_IMAGE", DISPATCHER_REGISTRY + "/brik2-dp-dispatcher:" + DISPATCHER_TAG)
container := env("CONTAINER_ENGINE", "docker")

# List available project commands.
default:
    @just --list

# Create .venv with locked application and test dependencies.
setup *args:
    uv sync --frozen "$@"

# Run pytest with coverage; optional arguments select tests.
test *args:
    uv run --frozen pytest --cov --cov-report=term-missing:skip-covered "$@"

# Check lint, formatting and types.
lint:
    uv run --frozen ruff check src tests
    uv run --frozen ruff format --check src tests
    uv run --frozen pyright

# Apply Ruff fixes and formatting.
fmt:
    uv run --frozen ruff check --fix src tests
    uv run --frozen ruff format src tests

# Build the Linux AMD64 image.
build *args:
    {{container}} build --platform linux/amd64 --tag "$DISPATCHER_IMAGE" "$@" .

# Push the locally built image (DISPATCHER_REGISTRY / DISPATCHER_TAG select the target).
push:
    {{container}} push "$DISPATCHER_IMAGE"

# Start the dispatcher with Docker Compose.
up *args:
    {{container}} compose up -d "$@"

# Stop the dispatcher (the state volume is kept).
down *args:
    {{container}} compose down "$@"

# Follow dispatcher logs.
logs *args:
    {{container}} compose logs -f "$@"

# Run a dispatcher CLI command inside the running container, e.g. `just cli status`.
cli *args:
    {{container}} compose exec dispatcher brik-dispatch "$@"

# Remove the venv, caches, local state, and the Compose containers and state volume.
clean:
    -{{container}} compose down --volumes --remove-orphans
    rm -rf .venv .pytest_cache .ruff_cache .coverage htmlcov build dist state
    find . -name __pycache__ -type d -prune -exec rm -rf {} +
    find src -name '*.egg-info' -type d -prune -exec rm -rf {} +
