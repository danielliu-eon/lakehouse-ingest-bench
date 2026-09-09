"""The manifests a driver applies have to render, and to render into what a Job needs.

Nothing here reaches a cluster: rendering is string substitution, and the whole
of what can go wrong before `kubectl` sees a document is a marker nobody gave a
value for, a value nobody's marker asked for, and a rendered document that is
not the Job the driver meant. Each of those costs an operator a failed apply
minutes into a run, and each is checkable with no cluster at all.

Every template's markers are enumerated here by name rather than discovered, so
a template that gains one is a failure in this file instead of an unrendered
`__NAME__` reaching the API server.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest
import yaml

from ingest_bench.k8s.render import MARKER_RE, main, render_template

TEMPLATES = Path(__file__).resolve().parents[1] / "deploy" / "k8s"

# One value per marker, shaped like the real ones: a command carrying the shell
# variable the indexed Jobs expand, and placement values as the JSON flow style
# the drivers render maps and lists in.
SAMPLE = {
    "NAME": "a-job",
    "NAMESPACE": "a-namespace",
    "SERVICE_ACCOUNT": "an-account",
    "IMAGE": "123456789012.dkr.ecr.eu-west-1.amazonaws.com/lakehouse-ingest-bench/harness:abc1234",
    "COMMAND": "gen-corpus --preset smoke --out s3://a-bucket/corpus/shards/$JOB_COMPLETION_INDEX --shard-count 4",
    "ENV": '[{"name": "AWS_REGION", "value": "eu-west-1"}, {"name": "AWS_DEFAULT_REGION", "value": "eu-west-1"}]',
    "NODE_SELECTOR": '{"kubernetes.io/arch": "amd64"}',
    "TOLERATIONS": '[{"key": "a-taint", "operator": "Exists", "effect": "NoSchedule"}]',
    "COUNT": "4",
    "SPEC_CONFIGMAP": "stage-a-run-spec",
    "SITE_CONFIGMAP": "stage-a-run-site",
}


@dataclass(frozen=True)
class Expectation:
    """What one shipped template takes, and what it has to render into."""

    markers: frozenset[str]
    cpu: str
    memory: str
    indexed: bool
    work_volume: bool


_ONE_OFF = frozenset(
    {"NAME", "NAMESPACE", "SERVICE_ACCOUNT", "IMAGE", "COMMAND", "ENV", "NODE_SELECTOR", "TOLERATIONS"}
)
_INDEXED = _ONE_OFF | {"COUNT"}
_MOUNTED = _ONE_OFF | {"SPEC_CONFIGMAP", "SITE_CONFIGMAP"}

# The one shipped template that is not a harness Job: a Deployment and a
# Service, applied by deploy/aws/setup.sh rather than by a run driver, and
# checked in tests/test_scripts.py beside the script that applies it.
NOT_A_JOB = frozenset({"schema-registry.yaml.tmpl"})

EXPECTATIONS = {
    "harness-job.yaml.tmpl": Expectation(_ONE_OFF, "500m", "1Gi", indexed=False, work_volume=False),
    "stage-job.yaml.tmpl": Expectation(_MOUNTED, "500m", "1Gi", indexed=False, work_volume=True),
    "corpus-gen-job.yaml.tmpl": Expectation(_INDEXED, "1", "2Gi", indexed=True, work_volume=False),
    "producer-job.yaml.tmpl": Expectation(_INDEXED, "1", "2Gi", indexed=True, work_volume=True),
    "scorer-job.yaml.tmpl": Expectation(_ONE_OFF, "1", "2Gi", indexed=False, work_volume=True),
}


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict), f"expected a mapping, got {type(value).__name__}"
    return {str(key): entry for key, entry in cast(dict[object, object], value).items()}


def _sequence(value: object) -> list[object]:
    assert isinstance(value, list), f"expected a list, got {type(value).__name__}"
    return cast(list[object], value)


def _pod_spec(document: dict[str, object]) -> dict[str, object]:
    return _mapping(_mapping(_mapping(document["spec"])["template"])["spec"])


def _container(document: dict[str, object]) -> dict[str, object]:
    containers = _sequence(_pod_spec(document)["containers"])
    assert len(containers) == 1, "a harness Job runs one container"
    return _mapping(containers[0])


# ---------------------------------------------------------------------------
# The renderer
# ---------------------------------------------------------------------------


def test_every_marker_is_substituted(tmp_path: Path) -> None:
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("name: __NAME__\nalso: __NAME__\nimage: __IMAGE__\n")
    rendered = render_template(template, {"NAME": "a-job", "IMAGE": "a-registry/an-image:a-tag"})
    assert rendered == "name: a-job\nalso: a-job\nimage: a-registry/an-image:a-tag\n"


def test_a_marker_with_no_value_is_refused(tmp_path: Path) -> None:
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("name: __NAME__\nimage: __IMAGE__\n")
    with pytest.raises(ValueError, match="IMAGE"):
        render_template(template, {"NAME": "a-job"})


def test_a_variable_that_matched_nothing_is_refused(tmp_path: Path) -> None:
    """A misspelled `--set` is a value that silently did not reach the manifest.

    Left unchecked it renders a Job with the template's own default for
    whatever it was meant to set, which is the class of mistake that puts a
    pod on the wrong nodes or under the wrong identity.
    """
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("name: __NAME__\n")
    with pytest.raises(ValueError, match="NAMESPACE"):
        render_template(template, {"NAME": "a-job", "NAMESPACE": "a-namespace"})


def test_a_value_holding_a_marker_is_not_rendered_again(tmp_path: Path) -> None:
    """Substitution is one pass, so a value is data and never a template.

    A command line or a node-selector key may legitimately hold the marker
    syntax, and a second pass over the rendered document would either refuse
    it or replace part of a value the caller meant literally.
    """
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("args: __COMMAND__\n")
    assert render_template(template, {"COMMAND": "echo __NAME__"}) == "args: echo __NAME__\n"


def test_the_console_script_prints_the_rendered_document(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("name: __NAME__\n")
    assert main([str(template), "--set", "NAME=a-job"]) == 0
    assert capsys.readouterr().out == "name: a-job\n"


# ---------------------------------------------------------------------------
# The shipped templates
# ---------------------------------------------------------------------------


def test_every_shipped_template_is_expected_here() -> None:
    shipped = {path.name for path in TEMPLATES.glob("*.yaml.tmpl")} - NOT_A_JOB
    assert shipped == set(EXPECTATIONS), "a template was added or renamed and this file does not know about it"


@pytest.mark.parametrize("filename", sorted(EXPECTATIONS))
def test_a_shipped_template_renders_to_the_job_the_driver_meant(filename: str) -> None:
    expectation = EXPECTATIONS[filename]
    template = TEMPLATES / filename
    assert {match[2:-2] for match in MARKER_RE.findall(template.read_text())} == set(expectation.markers), (
        f"{filename}'s markers and the ones this test fills have drifted"
    )

    document = _mapping(yaml.safe_load(render_template(template, {k: SAMPLE[k] for k in expectation.markers})))
    assert document["kind"] == "Job" and document["apiVersion"] == "batch/v1"
    metadata = _mapping(document["metadata"])
    assert metadata["name"] == "a-job" and metadata["namespace"] == "a-namespace"

    spec = _mapping(document["spec"])
    # Zero retries: a Job that reran a producer shard would offer its batches a
    # second time, and the scorer would read the repeat as the engine
    # duplicating rows.
    assert spec["backoffLimit"] == 0
    if expectation.indexed:
        assert spec["completionMode"] == "Indexed"
        assert spec["completions"] == 4 and spec["parallelism"] == 4
    else:
        assert spec["completions"] == 1
        assert "completionMode" not in spec

    pod = _pod_spec(document)
    assert pod["restartPolicy"] == "Never"
    assert pod["serviceAccountName"] == "an-account"
    assert pod["nodeSelector"] == {"kubernetes.io/arch": "amd64"}
    assert pod["tolerations"] == [{"key": "a-taint", "operator": "Exists", "effect": "NoSchedule"}]

    container = _container(document)
    assert container["name"] == "harness"
    assert container["image"] == SAMPLE["IMAGE"]
    # The image's entrypoint is `/bin/sh -c`, so the whole command line is one
    # argument and the shell it names expands `$JOB_COMPLETION_INDEX`.
    assert container["args"] == [SAMPLE["COMMAND"]]
    assert container["env"] == [
        {"name": "AWS_REGION", "value": "eu-west-1"},
        {"name": "AWS_DEFAULT_REGION", "value": "eu-west-1"},
    ]
    requests = _mapping(_mapping(container["resources"])["requests"])
    assert requests["cpu"] == expectation.cpu and requests["memory"] == expectation.memory

    if expectation.work_volume:
        mount = _mapping(_sequence(container["volumeMounts"])[0])
        assert mount["mountPath"] == "/work"
        volume = _mapping(_sequence(pod["volumes"])[0])
        assert volume["name"] == mount["name"] and "emptyDir" in volume
    else:
        assert "volumeMounts" not in container and "volumes" not in pod


def test_the_stage_job_mounts_the_spec_and_the_site_read_only() -> None:
    """Staging reads two files, and both reach it as ConfigMaps the driver made.

    The mount paths are half of the command line `stage.sh` renders — `--spec
    /runs/<name>` and `--site /site/site.yaml` — so a path changed in one place
    and not the other is a Job that cannot find its own spec. Read-only because
    nothing in the pod may edit the record of what was asked for.
    """
    template = TEMPLATES / "stage-job.yaml.tmpl"
    values = {key: SAMPLE[key] for key in EXPECTATIONS["stage-job.yaml.tmpl"].markers}
    document = _mapping(yaml.safe_load(render_template(template, values)))

    mounts = {
        str(_mapping(entry)["name"]): _mapping(entry) for entry in _sequence(_container(document)["volumeMounts"])
    }
    assert mounts["spec"]["mountPath"] == "/runs" and mounts["spec"]["readOnly"] is True
    assert mounts["site"]["mountPath"] == "/site" and mounts["site"]["readOnly"] is True

    volumes = {str(_mapping(entry)["name"]): _mapping(entry) for entry in _sequence(_pod_spec(document)["volumes"])}
    assert set(volumes) == set(mounts)
    assert _mapping(volumes["spec"]["configMap"])["name"] == SAMPLE["SPEC_CONFIGMAP"]
    assert _mapping(volumes["site"]["configMap"])["name"] == SAMPLE["SITE_CONFIGMAP"]


def test_a_site_naming_no_region_renders_an_empty_env() -> None:
    """A cluster off AWS names no region, and the env list is then empty.

    An `AWS_REGION` rendered empty would reach an SDK as a region it cannot
    resolve, which surfaces as a signing failure far from the site config.
    """
    values = {key: SAMPLE[key] for key in EXPECTATIONS["harness-job.yaml.tmpl"].markers} | {"ENV": "[]"}
    document = _mapping(yaml.safe_load(render_template(TEMPLATES / "harness-job.yaml.tmpl", values)))
    assert _container(document)["env"] == []
