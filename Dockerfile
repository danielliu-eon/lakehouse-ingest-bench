# The harness: the corpus generator, the producer, the table tools and the
# scorer, as the console scripts a run driver calls.
#
# No platform is pinned. Every dependency here publishes a Linux aarch64
# wheel, so the image is native wherever it is built — unlike the engine
# image beside it, which PyFlink forces to amd64.
#
# The uv version is pinned, not just the Python version: `python3.12-bookworm-slim`
# alone floats with every uv release, so the installer that resolves a locked
# dependency set would differ between two builds of the same commit.
FROM ghcr.io/astral-sh/uv:0.9.30-python3.12-bookworm-slim

WORKDIR /app
# `copy` because the cache and /app are on different layers, so uv's default
# hardlinking cannot apply. Bytecode is compiled at build time so a container
# that runs one command does not pay for compiling the whole dependency set.
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 PATH="/app/.venv/bin:${PATH}"

# The dependency set first and the project after, so editing a source file
# reuses the cached dependency layer. `--no-install-project` is what keeps
# this layer independent of the sources; the second sync installs the project
# itself and is the only layer a source edit invalidates. README.md is copied
# because pyproject names it as the package readme, so the build backend
# needs it present.
#
# The `aws` extra is installed unconditionally: it carries the MSK IAM token
# signer, which a site on another cloud never calls, and resolving it at image
# build time is what keeps one image able to run against any site.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --extra aws --no-install-project

COPY ingest_bench ./ingest_bench
COPY engines ./engines
COPY workloads ./workloads
RUN uv sync --frozen --no-dev --extra aws

# Presets and schemas live beside the package in a checkout and are copied to
# a fixed path here, so every command in this image finds them without a flag.
ENV INGEST_BENCH_WORKLOADS=/app/workloads

# A shell entrypoint is what makes `docker compose run --rm harness "<command
# string>"` work: the argument is one string holding a whole command line,
# which the shell splits.
#
# The shell stays PID 1 and forwards nothing, so a stopped container kills the
# command without warning it. That is tolerable because every command here is
# a one-shot that flushes as it goes — the producer appends its publish log
# record by record precisely so a killed producer still published its history.
# A command that needed a graceful stop would have to be `exec`'d by the
# caller's own string.
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["gen-corpus --help"]
