# syntax=docker/dockerfile:1

# --- builder: resolve dependencies into a self-contained venv -------------
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --upgrade pip && pip install .

# --- runtime --------------------------------------------------------------
FROM python:3.12-slim AS runtime

# ffmpeg/ffprobe do the chunking and probing.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg tini \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY alembic.ini ./
COPY migrations ./migrations
COPY src ./src

# Run unprivileged; the shared audio volume only needs to be readable.
RUN useradd --create-home --uid 10001 transcriber \
    && mkdir -p /data/audio /tmp/transcriber \
    && chown -R transcriber:transcriber /app /tmp/transcriber
USER transcriber

ENV PYTHONPATH=/app/src

EXPOSE 8000
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "transcriber", "worker"]
