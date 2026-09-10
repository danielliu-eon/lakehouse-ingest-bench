# SPDX-License-Identifier: Apache-2.0
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


@pytest.mark.parametrize("engine", sorted(engines.MANAGED))
def test_every_managed_engine_says_how_it_is_addressed(engine: str) -> None:
    """A managed engine with no descriptor is one no driver could stage.

    The printed fields are checked against the dataclass's own, because a field
    added without a line in `texts` would be one a driver asks for and is
    refused.
    """
    descriptor = engines.kubernetes_for(engine)
    assert set(descriptor.texts()) == set(FIELDS)
    assert descriptor.kind and descriptor.running_state and descriptor.failed_states
    assert descriptor.state_jsonpath.startswith("{.status.") and descriptor.rest_port > 0
    # A document the operator rejected reports no state, so the error field
    # is the only thing that tells a rejection from a run still starting.
    assert descriptor.error_jsonpath.startswith("{.status.")
    assert descriptor.error_jsonpath != descriptor.state_jsonpath
    # What says whether the operator has given up, so that an error it is
    # still retrying is not read as a rejection. The same path as the state
    # for an engine whose application state is its lifecycle.
    assert descriptor.lifecycle_jsonpath.startswith("{.status.")
    # Two documents, named apart: a driver applies the ConfigMap first because
    # the other one mounts it.
    assert descriptor.document_file.endswith(".yaml") and descriptor.configmap_file.endswith(".yaml")
    assert descriptor.document_file != descriptor.configmap_file


def test_the_spark_descriptor_names_what_the_operator_named() -> None:
    """Every name here belongs to the spark-operator rather than to this harness.

    It publishes the driver's UI as `<application>-ui-svc` on Spark's own 4040,
    names the driver pod `<application>-driver`, and labels both halves of the
    fleet with the application's name — so `spark-role` is what tells the two
    apart when provenance wants the driver's image alone.
    """
    descriptor = engines.kubernetes_for("spark")
    assert descriptor.kind == "sparkapplication"
    assert descriptor.state_jsonpath == "{.status.applicationState.state}"
    assert descriptor.error_jsonpath == "{.status.applicationState.errorMessage}"
    # A SparkApplication has no lifecycle apart from that state.
    assert descriptor.lifecycle_jsonpath == descriptor.state_jsonpath
    assert descriptor.rest_service_suffix == "-ui-svc"
    assert descriptor.rest_port == 4040
    assert for_name(descriptor.log_target, RUN_OBJECT) == f"pod/{RUN_OBJECT}-driver"
    assert for_name(descriptor.provenance_selector, RUN_OBJECT) == (
        f"spark-role=driver,sparkoperator.k8s.io/app-name={RUN_OBJECT}"
    )
    # How many executors there are, and whether their CPU is guaranteed, are
    # properties of the pods and are reported nowhere in the driver's answers.
    assert for_name(descriptor.pods_selector, RUN_OBJECT) == f"sparkoperator.k8s.io/app-name={RUN_OBJECT}"
    # A query that ended — cleanly or not — leaves no fleet, and at staging
    # time the topic is empty, so any of the five means the run cannot start.
    assert descriptor.failed_states == ("FAILED", "SUBMISSION_FAILED", "FAILING", "COMPLETED", "SUCCEEDING")
    # The spark-operator publishes no name length of its own, so none is
    # declared: `spec.name`'s own pattern already keeps every object name
    # derived from it inside the 63 characters a label value takes.
    assert descriptor.max_object_name_length is None


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
    assert descriptor.error_jsonpath == "{.status.error}"
    assert descriptor.lifecycle_jsonpath == "{.status.lifecycleState}"
    assert descriptor.rest_service_suffix == "-rest"
    assert descriptor.rest_port == 8081
    assert for_name(descriptor.log_target, RUN_OBJECT) == f"deploy/{RUN_OBJECT}"
    assert for_name(descriptor.provenance_selector, RUN_OBJECT) == f"app={RUN_OBJECT},component=jobmanager"
    # The whole graph is read off the job's own REST endpoint, so its check is
    # handed no pod list.
    assert descriptor.pods_selector == ""
    assert descriptor.document_file == "flinkdeployment.yaml"
    assert descriptor.configmap_file == "flink-job-configmap.yaml"
    # The operator validates the deployment's own name at 45 characters,
    # because the Service carrying the REST endpoint is named by it.
    assert descriptor.max_object_name_length == 45


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


def test_the_name_length_a_run_is_refused_by_is_not_a_field_a_driver_can_ask_for() -> None:
    """It is checked in Python, before the run has an object for a driver to address.

    A driver that read it would be reading a number it has nothing to do with:
    by the time one runs, staging has already refused every name past it.
    """
    assert "max_object_name_length" not in render("flink", [])
    with pytest.raises(ValueError, match="no such field"):
        render("flink", ["max_object_name_length"])


def test_the_console_script_prints_a_field_and_refuses_the_rest(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["flink", "kind"]) == 0
    assert capsys.readouterr().out == "flinkdeployment\n"
    assert main(["flink", "kimd"]) == 2
    assert "no such field" in capsys.readouterr().err
    assert main(["nothing", "kind"]) == 2
    assert "not a managed engine" in capsys.readouterr().err
