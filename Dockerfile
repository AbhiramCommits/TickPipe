# syntax=docker/dockerfile:1

FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY pyproject.toml setup.py MANIFEST.in ./
COPY cpp ./cpp
COPY src ./src

RUN pip install --no-cache-dir --upgrade pip \
    && pip wheel . --no-deps --wheel-dir dist

FROM python:3.11-slim AS runtime

RUN useradd --create-home --uid 1000 tickpipe \
    && mkdir -p /app/data /app/scripts \
    && chown tickpipe:tickpipe /app/data

WORKDIR /app

COPY --from=builder /build/dist/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/*.whl \
    && rm -rf /tmp/wheels

COPY experiments /app/experiments
COPY scripts/quickstart.sh /app/scripts/quickstart.sh
RUN chmod +x /app/scripts/quickstart.sh \
    && chown -R tickpipe:tickpipe /app

USER tickpipe
ENTRYPOINT ["tickpipe"]
CMD ["--help"]
