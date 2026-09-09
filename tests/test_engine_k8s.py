"""What a shell driver is told about an engine it is about to address on a cluster.

The drivers hold no engine's names. They ask for the kind of resource a run is,
where its state sits in the status and which Service carries its HTTP API, and
then apply, poll, tunnel and delete against the answers. So a wrong or missing
answer here is a driver waiting on an object nobody created — minutes into a
staged run, with a topic and a table already made — and every answer is
checkable with no cluster at all.
"""

from __future__ import annotations

import pytest

from ingest_bench.k8s.engine import main, render
from ingest_bench.specs import engines
from ingest_bench.specs.kubernetes import FIELDS, NAME, for_name

RUN_OBJECT = "smoke-flink-20260908t000000z"


def test_the_flink_descriptor_names_what_the_operator_named() -> None:
    """Every name here belongs to the Flink operator rather than to this harness.

    It publishes the JobManager's REST endpoint as `<deployment>-rest` and
    labels the pods it creates itself, so these are read from its behaviour
    and not chosen — which is why they are pinned rather than derived.
    """
    descriptor = engines.kubernetes_for("flink")
    assert descriptor.kind == "flinkdeployment"
    assert descriptor.running_state == "RUNNING"
    assert descriptor.state_jsonpath == "{.status.jobStatus.state}"
    assert descriptor.rest_service_suffix == "-rest"
    assert descriptor.rest_port == 8081
    assert for_name(descriptor.log_target, RUN_OBJECT) == f"deploy/{RUN_OBJECT}"
    assert for_name(descriptor.provenance_selector, RUN_OBJECT) == f"app={RUN_OBJECT},component=jobmanager"
    # The whole graph is read off the job's own REST endpoint, so its check is
    # handed no pod list.
    assert descriptor.pods_selector == ""
    assert descriptor.document_file == "flinkdeployment.yaml"
    assert descriptor.configmap_file == "flink-job-configmap.yaml"


def test_a_job_that_ended_before_the_run_started_is_a_failure_state() -> None:
    """Cancelled and finished end a staging wait as surely as failed does.

    Each of the three leaves no fleet, so waiting one out to the driver's
    timeout would postpone the same refusal by ten minutes.
    """
    assert engines.kubernetes_for("flink").failed_states == ("FAILED", "CANCELED", "FINISHED")


def test_an_engine_nothing_registered_is_refused_by_name() -> None:
    with pytest.raises(ValueError, match="not a managed engine"):
        engines.kubernetes_for("nothing")


def test_one_field_prints_its_value_alone() -> None:
    """A driver assigns this to a variable, so the field's name is not in it."""
    assert render("flink", ["kind"]) == "flinkdeployment\n"
    assert render("flink", ["rest_port"]) == "8081\n"
    # Empty is an answer, and it has to survive as one: a driver reads it to
    # mean this engine's check needs no pod list.
    assert render("flink", ["pods_selector"]) == "\n"


def test_every_field_is_printed_when_none_is_asked_for() -> None:
    """One call is what a driver makes, so the whole descriptor is one answer."""
    printed = render("flink", [])
    assert [entry.split("=", 1)[0] for entry in printed.splitlines()] == list(FIELDS)
    assert "failed_states=FAILED,CANCELED,FINISHED" in printed.splitlines()
    assert f"log_target=deploy/{NAME}" in printed.splitlines()


def test_several_fields_are_named_so_the_lines_can_be_told_apart() -> None:
    assert render("flink", ["kind", "rest_port"]) == "kind=flinkdeployment\nrest_port=8081\n"


def test_a_field_that_does_not_exist_is_refused_rather_than_answered_empty() -> None:
    """A driver that read an empty answer would address the cluster with no name."""
    with pytest.raises(ValueError, match="no such field"):
        render("flink", ["rest_service"])


def test_the_console_script_prints_a_field_and_refuses_the_rest(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["flink", "kind"]) == 0
    assert capsys.readouterr().out == "flinkdeployment\n"
    assert main(["flink", "kimd"]) == 2
    assert "no such field" in capsys.readouterr().err
    assert main(["nothing", "kind"]) == 2
    assert "not a managed engine" in capsys.readouterr().err
