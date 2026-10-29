# syntax=docker/dockerfile:1
ARG PYTHON_VERSION=3.13

FROM python:${PYTHON_VERSION}-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/dispatcher/.venv
WORKDIR /src
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,id=uv,target=/root/.cache/uv \
    uv sync --frozen --no-default-groups --no-install-project
COPY README.md ./
COPY src/ ./src/
RUN --mount=type=cache,id=uv,target=/root/.cache/uv \
    uv sync --frozen --no-default-groups --no-editable

FROM python:${PYTHON_VERSION}-slim AS runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/dispatcher/.venv/bin:${PATH}" \
    BRIK_DISPATCH_WATCH_DIR=/data/incoming \
    BRIK_DISPATCH_DB_PATH=/var/lib/brik-dispatch/state.db
RUN groupadd --gid 10001 dispatcher \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin dispatcher \
    && mkdir -p /var/lib/brik-dispatch /data/incoming \
    && chown 10001:10001 /var/lib/brik-dispatch
COPY --from=builder /opt/dispatcher/.venv /opt/dispatcher/.venv

USER 10001:10001
VOLUME ["/var/lib/brik-dispatch"]
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
    CMD ["brik-dispatch", "health"]
ENTRYPOINT ["brik-dispatch"]
CMD ["run"]
