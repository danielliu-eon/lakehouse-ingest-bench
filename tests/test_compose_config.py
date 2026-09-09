# SPDX-License-Identifier: Apache-2.0
"""The local stack's compose file has to render before anything can run it.

`docker compose config` resolves the `include`, every profile's services and
every interpolation, so it catches the mistakes that only surface at `up`:
a service referenced across profiles, a variable with no default, a key the
schema does not know. It is the cheapest check that the file is a project.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from engines.spark import knobs, stream_to_iceberg
from ingest_bench.catalog import load_catalog_props
from ingest_bench.specs.model import load_site

REPO_ROOT = Path(__file__).resolve().parents[1]
LOCAL = REPO_ROOT / "deploy" / "compose" / "local"
COMPOSE = LOCAL / "docker-compose.yml"


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
def test_compose_config_renders() -> None:
    out = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE),
            "--profile",
            "flink",
            "--profile",
            "flink-job",
            "--profile",
            "spark",
            "--profile",
            "tools",
            "config",
        ],
        capture_output=True,
        text=True,
        # A fixed PATH and a single variable, so the rendering a developer's
        # exported JM_MEM_MB or SLOTS would produce is not what is asserted on.
        # RUN_DIR has no default by design — the submitter mounts a staged run
        # directory or must not start — so the one profile that names it can
        # only render with it set.
        env={"PATH": "/usr/local/bin:/usr/bin:/bin", "RUN_DIR": "/tmp"},
    )
    assert out.returncode == 0, out.stderr
    for service in (
        "kafka",
        "minio",
        "iceberg-rest",
        "schema-registry",
        "harness",
        "flink-jobmanager",
        "flink-taskmanager",
        "flink-job",
        "spark-job",
    ):
        assert f"  {service}:" in out.stdout


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
def test_the_spark_service_submits_the_files_the_renderer_writes() -> None:
    """The submission line names the mount, the properties file and the job.

    Three of the four are the renderer's own constants and the fourth is the
    path the job reads its documents from, so a rename on either side leaves
    this command line pointing at a file nothing writes — which surfaces as a
    driver that exits before the run, minutes after a topic was created.
    """
    service = yaml.safe_load((REPO_ROOT / "engines" / "spark" / "compose.yaml").read_text())["services"]["spark-job"]
    mount = str(stream_to_iceberg.RUN_DIR)
    assert service["volumes"] == [f"${{RUN_DIR:-/nonexistent/run}}:{mount}:ro"]
    command = service["command"]
    assert command[-1] == "/opt/bench/engines/spark/stream_to_iceberg.py"
    assert command[command.index("--properties-file") + 1] == f"{mount}/{knobs.CONF_FILE}"
    # The two variables the driver is sized by are the ones `job.env` sets.
    rendered = " ".join(command)
    for variable in (knobs.LOCAL_CORES_VAR, knobs.DRIVER_MEM_VAR):
        assert f"${{{variable}:-" in rendered


def test_catalog_props_file_matches_the_site_catalog_block() -> None:
    """The two ways the stack names its catalog have to agree.

    `site.yaml` is what the run driver reads and `catalog.props` is what the
    table tools take on `--catalog-prop-file`; a run uses both. Editing one
    alone would point the table tools at a different catalog than the driver,
    and the divergence would surface as a table that exists nowhere the
    scorer looks.
    """
    assert load_catalog_props([], [str(LOCAL / "catalog.props")]) == load_site(LOCAL / "site.yaml").catalog_props


def test_local_site_addresses_the_compose_services() -> None:
    """The checked-in site config names services this compose file declares.

    It is checked in precisely because it describes this stack, so a service
    rename that left it behind would only fail inside whichever tool dialled
    the stale name first.
    """
    site = load_site(LOCAL / "site.yaml")
    services = set(yaml.safe_load(COMPOSE.read_text())["services"])
    assert site.kafka_bootstrap.split(":")[0] in services
    assert site.catalog_props["uri"].split("//")[1].split(":")[0] in services
    # The registry is reached by service name over the compose network, by the
    # harness that registers a schema and by the engine that reads it back.
    assert site.schema_registry is not None
    assert site.schema_registry.url.split("//")[1].split(":")[0] in services


@pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")
def test_nothing_the_stack_mounts_is_excluded_from_the_repo() -> None:
    """The stack's own config files have to survive a clone.

    They are bind mounts, so an excluded one is not a missing file at `up`:
    the daemon creates a directory at the mount point and the service fails
    on config it cannot parse. The way it happens is an unanchored ignore
    pattern written for the operator's own root-level copy of a file, which
    matches this directory's copy just as well.
    """
    mounted = [
        Path(volume.split(":", 1)[0])
        for service in yaml.safe_load(COMPOSE.read_text())["services"].values()
        for volume in service.get("volumes", [])
        if volume.startswith("./")
    ]
    assert mounted, "no relative bind mounts found; has the compose file's volume syntax changed?"
    for relative in mounted:
        path = (LOCAL / relative).resolve()
        assert path.is_file(), path
        check = subprocess.run(["git", "check-ignore", "-q", str(path)], cwd=LOCAL)
        assert check.returncode != 0, f"{path} is excluded by .gitignore but the stack mounts it"
