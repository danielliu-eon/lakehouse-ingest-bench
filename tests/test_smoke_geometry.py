# SPDX-License-Identifier: Apache-2.0
"""Exercise local smoke geometry without starting Docker."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    any(shutil.which(tool) is None for tool in ("bash", "jq", "yq", "curl")),
    reason="smoke.sh requires bash, jq, yq and curl",
)
RUN_ID = "smoke-external-20260910T120000Z"
TABLE = "ingest_bench.t_smoke_external_20260910T120000Z"
GEOMETRY = {
    "final": {
        "live": {
            "files": 4,
            "size_quantiles": {"p50": 40_100_000.0},
            "small_file_share_32mib": 0.25,
        }
    }
}

DOCKER_STUB = r"""
printf '%s\n' "$*" >>"$STUB_DOCKER_LOG"
if [[ ${1:-} == wait ]]; then
	printf '0\n'
	exit 0
fi
[[ ${1:-} == compose ]] || exit 0
command="${*: -1}"
case "$command" in
stage\ *)
	mkdir -p "$STUB_REPO/runs/$STUB_RUN_ID/scores"
	cp "$STUB_FACTS" "$STUB_REPO/runs/$STUB_RUN_ID/facts.json"
	cp "$STUB_SUMMARY" "$STUB_REPO/runs/$STUB_RUN_ID/scores/summary.json"
	printf 'run_id: %s\n' "$STUB_RUN_ID"
	;;
file-sizes\ *)
	printf '%s\n' "$command" >>"$STUB_FILE_SIZES_LOG"
	if [[ $STUB_GEOMETRY_STATUS == 0 ]]; then
		printf '%s\n' "$STUB_GEOMETRY" >"$STUB_REPO/runs/$STUB_RUN_ID/scores/geometry.json"
	elif [[ $STUB_GEOMETRY_STATUS == 4 ]]; then
		printf '{"final": null}\n' >"$STUB_REPO/runs/$STUB_RUN_ID/scores/geometry.json"
	fi
	exit "$STUB_GEOMETRY_STATUS"
	;;
esac
"""


@dataclass(frozen=True)
class SmokeRun:
    result: subprocess.CompletedProcess[str]
    docker_calls: list[str]
    file_sizes_calls: list[str]
    run_dir: Path


def _run_smoke(tmp_path: Path, *, geometry_status: int = 0, offsets: list[int] | None = None) -> SmokeRun:
    root = tmp_path / "repo"
    for directory in ("scripts", "runs", "engines/flink", "deploy/compose/local"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    for name in ("smoke.sh", "_lib.sh"):
        shutil.copy2(REPO_ROOT / "scripts" / name, root / "scripts" / name)
    shutil.copy2(REPO_ROOT / "engines/flink/compose.yaml", root / "engines/flink/compose.yaml")

    spec = {
        "name": "smoke-external",
        "engine": "external",
        "corpus": "smoke",
        "table": {"partition": "identity(partition_key)"},
        "producer": {"shards": 1},
        "scoring": {"geometry_offsets_s": [] if offsets is None else offsets},
    }
    spec_path = root / "runs/spec.yaml"
    spec_path.write_text(yaml.safe_dump(spec))

    facts = root / "facts.json"
    facts.write_text(
        json.dumps(
            {
                "bootstrap": "kafka:9092",
                "corpus_uri": "s3://corpus/smoke-test",
                "table": TABLE,
                "key_column": None,
                "value_encoding": "raw",
                "schema_id": None,
            }
        )
    )
    summary = root / "summary.json"
    summary.write_text(
        json.dumps(
            {
                "run_valid": True,
                "state": "drained",
                "reason": "",
                "producer_bound": False,
                "prefix": 10,
                "last_batch": 10,
                "committed_rows": 100,
                "offered_rows": 100,
                "freshness": {"window": {}},
                "exactness": {"exact": True, "loss_rows": 0, "duplicate_rows": 0},
                "keepup": {},
            }
        )
    )

    ready = root / "ready"
    ready.touch()
    docker_log = root / "docker.log"
    docker_log.touch()
    file_sizes_log = root / "file-sizes.log"
    file_sizes_log.touch()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(f"#!/usr/bin/env bash\nset -euo pipefail\n{DOCKER_STUB}")
    docker.chmod(0o755)

    result = subprocess.run(
        [
            str(root / "scripts/smoke.sh"),
            "--engine",
            "external",
            "--spec",
            str(spec_path),
            "--external-ready-file",
            str(ready),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "EPOCH_LEAD_S": "0",
            "STUB_DOCKER_LOG": str(docker_log),
            "STUB_FILE_SIZES_LOG": str(file_sizes_log),
            "STUB_REPO": str(root),
            "STUB_RUN_ID": RUN_ID,
            "STUB_FACTS": str(facts),
            "STUB_SUMMARY": str(summary),
            "STUB_GEOMETRY": json.dumps(GEOMETRY),
            "STUB_GEOMETRY_STATUS": str(geometry_status),
        },
    )
    return SmokeRun(
        result=result,
        docker_calls=docker_log.read_text().splitlines(),
        file_sizes_calls=file_sizes_log.read_text().splitlines(),
        run_dir=root / "runs" / RUN_ID,
    )


def test_smoke_measures_geometry_through_the_live_catalog_before_teardown(tmp_path: Path) -> None:
    run = _run_smoke(tmp_path, offsets=[5, 10])
    assert run.result.returncode == 0, run.result.stderr
    assert len(run.file_sizes_calls) == 1
    command = run.file_sizes_calls[0]
    assert f"file-sizes --table {TABLE}" in command
    assert "--catalog-prop-file /catalog.props" in command
    assert "--epoch " in command
    assert f"--out /runs/{RUN_ID}/scores" in command
    assert "--offsets 5,10" in command
    assert (run.run_dir / "scores/geometry.json").exists()
    assert "geometry: p50 38.2 MiB, small (<32 MiB) 25%, 4 files" in run.result.stdout

    scorer = next(call for call in run.docker_calls if "score --corpus" in call)
    producer = next(call for call in run.docker_calls if "produce --corpus" in call)
    scorer_epoch = re.search(r"--epoch (\d+)", scorer)
    producer_epoch = re.search(r"--epoch (\d+)", producer)
    geometry_epoch = re.search(r"--epoch (\d+)", command)
    assert scorer_epoch is not None and producer_epoch is not None and geometry_epoch is not None
    assert scorer_epoch.group(1) == producer_epoch.group(1) == geometry_epoch.group(1)

    measured = next(index for index, call in enumerate(run.docker_calls) if "file-sizes --table" in call)
    teardown = next(index for index, call in enumerate(run.docker_calls) if " down -v --remove-orphans" in call)
    assert measured < teardown


@pytest.mark.parametrize(
    ("status", "message"),
    [(4, "no geometry: the table never committed"), (7, "file-sizes exited 7")],
)
def test_smoke_handles_geometry_exit_statuses(tmp_path: Path, status: int, message: str) -> None:
    run = _run_smoke(tmp_path, geometry_status=status)
    assert run.result.returncode == (0 if status == 4 else 1)
    assert message in run.result.stderr
    assert "geometry: p50" not in run.result.stdout
    assert "--offsets" not in run.file_sizes_calls[0]
