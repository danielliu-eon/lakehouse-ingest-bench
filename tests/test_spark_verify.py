"""Every drift `verify-spark` can report, against recorded driver and pod answers.

Staging's RUNNING wait says the operator submitted something. What it cannot
say is that what it submitted is the run the spec asked for: Spark accepts a
setting it does not use, an executor the scheduler never placed leaves the
fleet narrower than the one the result is costed for, and a pod whose CPU
request and limit differ runs on cores the node may take back. Each of those is
silent. So these readings are what stands between a published figure and one
attributed to knobs the engine never honoured, and each is exercised here
against the documents the driver and `kubectl` answer rather than a cluster.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from engines.spark import verify as verify_module
from engines.spark.verify import APPLICATIONS, verify
from ingest_bench.readings import DRIFT_EXIT, UNVERIFIED_EXIT
from ingest_bench.specs import engines
from ingest_bench.specs.model import RunSpec, load_run_spec

ROOT = Path(__file__).resolve().parents[1]

# The shipped smoke spec is the run under test: two executors of two cores with
# 2 GiB each, behind a one-core 2 GiB driver. So the fleet it asks for is three
# pods and four shuffle partitions.
SPEC_FILE = ROOT / "runs" / "smoke-spark.yaml"

RUN_ID = "smoke-spark-20260908T120000Z"
RUN_OBJECT = RUN_ID.lower()
APPLICATION = "spark-1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d"

DRIVER_POD = f"{RUN_OBJECT}-driver"
EXECUTOR_PODS = (f"{RUN_OBJECT}-exec-1", f"{RUN_OBJECT}-exec-2")

ENVIRONMENT = f"{APPLICATIONS}/{APPLICATION}/environment"


def _spec() -> RunSpec:
    return load_run_spec(SPEC_FILE)


def _answers(
    *,
    name: str = RUN_ID,
    applications: list[dict[str, object]] | None = None,
    properties: dict[str, str] | None = None,
    drop: tuple[str, ...] = (),
) -> dict[str, object]:
    """The two documents a driver answers, with one reading changed.

    Shaped after a live 3.5 UI: `sparkProperties` is a list of two-element
    arrays rather than an object, and the entries beside the ones read here are
    carried so that a reader which started matching on position rather than on
    name would fail.
    """
    settings = {
        "spark.app.name": name,
        "spark.executor.instances": "2",
        "spark.executor.cores": "2",
        "spark.executor.memory": "2048m",
        "spark.driver.cores": "1",
        "spark.driver.memory": "2048m",
        "spark.sql.shuffle.partitions": "4",
        "spark.master": "k8s://https://kubernetes.default.svc",
        "spark.submit.deployMode": "cluster",
        **(properties or {}),
    }
    for key in drop:
        del settings[key]
    listed = (
        applications
        if applications is not None
        else [{"id": APPLICATION, "name": name, "attempts": [{"completed": False}]}]
    )
    return {
        APPLICATIONS: listed,
        ENVIRONMENT: {
            "runtime": {"javaVersion": "17.0.11", "scalaVersion": "version 2.12.18"},
            "sparkProperties": [[key, value] for key, value in settings.items()],
            "systemProperties": [["java.io.tmpdir", "/tmp"]],
            "classpathEntries": [["/opt/spark/jars/spark-core_2.12-3.5.9.jar", "System Classpath"]],
        },
    }


def _reader(answers: dict[str, object]) -> Callable[[str], Callable[[str], object]]:
    """A `fetch_json` stand-in over recorded answers, refusing an unknown path."""

    def open_endpoint(base_url: str) -> Callable[[str], object]:
        def fetch(path: str) -> object:
            if path not in answers:
                raise ValueError(f"could not read {base_url.rstrip('/')}{path}: no such recorded answer")
            return answers[path]

        return fetch

    return open_endpoint


def _pod(name: str, role: str, *, phase: str = "Running", qos: str | None = "Guaranteed") -> dict[str, object]:
    status: dict[str, object] = {"phase": phase, "podIP": "10.0.1.7"}
    if qos is not None:
        status["qosClass"] = qos
    return {
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": "ingest-bench",
            "labels": {"spark-role": role, "sparkoperator.k8s.io/app-name": RUN_OBJECT},
        },
        "spec": {"nodeName": "a-node"},
        "status": status,
    }


def _pods(items: list[dict[str, object]] | None = None) -> dict[str, object]:
    """The pod list `kubectl get pods -o json` answers for a healthy fleet."""
    if items is None:
        items = [
            _pod(DRIVER_POD, "driver"),
            *(_pod(name, "executor") for name in EXECUTOR_PODS),
        ]
    return {"apiVersion": "v1", "kind": "List", "items": items}


def _drift(*, answers: dict[str, object] | None = None, pods: dict[str, object] | None = None) -> list[str]:
    """The run's drift, with either document replaced by one a test shaped."""
    fetch = _reader(answers if answers is not None else _answers())("http://localhost:14040")
    return verify(_spec(), RUN_ID, fetch, pods if pods is not None else _pods())


def test_spark_is_a_registered_managed_engine() -> None:
    assert engines.verify_for("spark") is verify_module


def test_a_fleet_running_what_the_spec_asked_for_drifts_nowhere() -> None:
    assert _drift() == []


def test_a_setting_the_driver_was_not_given_is_named_rather_than_guessed() -> None:
    """Absence is a reading about the run, not an unreadable document.

    A driver submitted without one of these has a fleet the spec does not
    describe, which is exactly the case worth refusing — so it is reported as
    drift rather than raised as a document that could not be read.
    """
    assert _drift(answers=_answers(drop=("spark.executor.instances",))) == [
        "executors: spec 2, engine not reported",
    ]


@pytest.mark.parametrize(
    ("key", "value", "expected"),
    [
        ("spark.executor.instances", "1", "executors: spec 2, engine 1"),
        ("spark.executor.cores", "4", "executor cores: spec 2, engine 4"),
        ("spark.executor.memory", "1024m", "executor memory: spec 2048m, engine 1024m"),
        ("spark.driver.cores", "2", "driver cores: spec 1, engine 2"),
        ("spark.driver.memory", "512m", "driver memory: spec 2048m, engine 512m"),
        ("spark.sql.shuffle.partitions", "200", "shuffle partitions: spec 4, engine 200"),
    ],
)
def test_every_setting_the_fleet_is_sized_by_is_read_back(key: str, value: str, expected: str) -> None:
    """The operator restates the sizing as submit arguments, so the driver is asked.

    A knob that reached the SparkApplication and not the session would
    otherwise pass unnoticed, and the shuffle width is the writer count under a
    `hash` or `range` distribution — two hundred of them is two hundred files a
    commit.
    """
    assert _drift(answers=_answers(properties={key: value})) == [expected]


def test_an_override_the_run_chose_is_what_it_is_held_to() -> None:
    """`extra_spark_conf` is applied last, so it is what the engine was told.

    Reporting the knob it displaced would be drift on a run whose author chose
    the override.
    """
    spec = replace(_spec(), engine_block={**_spec().engine_block, "extra_spark_conf": {"spark.driver.memory": "4096m"}})
    fetch = _reader(_answers(properties={"spark.driver.memory": "4096m"}))("http://localhost:14040")
    assert verify(spec, RUN_ID, fetch, _pods()) == []
    honoured = _reader(_answers())("http://localhost:14040")
    assert verify(spec, RUN_ID, honoured, _pods()) == ["driver memory: spec 4096m, engine 2048m"]


def test_a_setting_that_pretends_to_move_the_cadence_is_refused() -> None:
    """Spark has no session setting for a trigger, which is what makes this checkable.

    The interval reaches the query through `job.json`. A property under this
    prefix is a run trying to move its own commit cadence through
    configuration nothing reads, so the result would be attributed to an
    interval the query never used.
    """
    pretending = {"spark.sql.streaming.trigger.interval": "60 seconds"}
    assert _drift(answers=_answers(properties=pretending)) == [
        "trigger settings: spec none, engine ['spark.sql.streaming.trigger.interval']",
    ]


# The lowercased run id, which is what the SparkApplication object is called
# because an RFC 1123 name has to be lowercase.
OBJECT_NAME = RUN_ID.lower()


@pytest.mark.parametrize("reported", [RUN_ID, OBJECT_NAME])
def test_either_spelling_of_the_run_s_name_identifies_its_driver(reported: str) -> None:
    """The submitted app name and the object's name differ only in case.

    `render_conf` submits the run id, whose stamp is uppercase; the object is
    named by the lowercased id. Which of the two the driver reports depends on
    whether the operator's own `spark.app.name` displaces the submitted one —
    so refusing either would fail a staging over a letter's case.
    """
    assert _drift(answers=_answers(name=reported)) == []


@pytest.mark.parametrize(
    ("listed", "shown"),
    [
        ([], "none listed"),
        ([{"id": "other", "name": "someone-elses-run", "attempts": []}], "['someone-elses-run']"),
        (
            [
                {"id": APPLICATION, "name": RUN_ID, "attempts": []},
                {"id": "second", "name": OBJECT_NAME, "attempts": []},
            ],
            f"['{RUN_ID}', '{OBJECT_NAME}']",
        ),
    ],
)
def test_an_endpoint_that_is_not_this_runs_driver_names_what_it_found(
    listed: list[dict[str, object]], shown: str
) -> None:
    """A tunnel is addressed by a port, so the name is what identifies the driver.

    The names are in the line and not just how many there were: this is the
    check most likely to fail on a live cluster, the cause is an application
    called something nobody predicted, and nothing else in the run says what
    the operator called it. Nothing further is read either — the settings of an
    application that is not the run's describe someone else's fleet — but the
    pods are still reported, because they are what say whether this run has a
    driver at all.
    """
    # Sorted, so an uppercase stamp comes before its lowercased twin.
    accepted = f"['{RUN_ID}', '{OBJECT_NAME}']"
    assert _drift(answers=_answers(applications=listed)) == [
        f"applications named after the run: spec one of {accepted}, engine {shown}",
    ]


def test_an_executor_the_scheduler_never_placed_is_a_narrower_fleet() -> None:
    """The count is what a result is costed for, and the driver cannot see it.

    An application runs with whatever executors it was given, so a fleet short
    of one publishes a rate attributed to compute that was never paid for.
    """
    short = [_pod(DRIVER_POD, "driver"), _pod(EXECUTOR_PODS[0], "executor")]
    assert _drift(pods=_pods(short)) == ["executor pods: spec 2, engine 1"]


def test_a_pod_that_borrows_its_cores_is_refused() -> None:
    """Burstable means the node may reclaim the CPU mid-run.

    A rate measured on cores the kubelet can take back is the node's answer
    under whatever else was scheduled beside it, not the engine's.
    """
    borrowed = [
        _pod(DRIVER_POD, "driver"),
        _pod(EXECUTOR_PODS[0], "executor", qos="Burstable"),
        _pod(EXECUTOR_PODS[1], "executor"),
    ]
    assert _drift(pods=_pods(borrowed)) == [
        f"pod {EXECUTOR_PODS[0]} qos class: spec Guaranteed, engine Burstable",
    ]


def test_a_pod_the_api_server_has_not_admitted_reports_neither_phase_nor_class() -> None:
    """A QoS class appears at admission, so its absence is a pod not yet running.

    Named as unreported rather than raised: the document is readable and what
    it says is that this half of the fleet is not up.
    """
    pending = [
        _pod(DRIVER_POD, "driver"),
        _pod(EXECUTOR_PODS[0], "executor", phase="Pending", qos=None),
        _pod(EXECUTOR_PODS[1], "executor"),
    ]
    assert _drift(pods=_pods(pending)) == [
        f"pod {EXECUTOR_PODS[0]} phase: spec Running, engine Pending",
        f"pod {EXECUTOR_PODS[0]} qos class: spec Guaranteed, engine not reported",
    ]


def test_a_missing_driver_is_reported_before_its_executors_are_counted() -> None:
    assert _drift(pods=_pods([_pod(name, "executor") for name in EXECUTOR_PODS])) == [
        "driver pods: spec 1, engine 0",
    ]


def test_a_pod_the_selector_matched_and_the_operator_did_not_label_is_named() -> None:
    """A pod sharing the run's labels and neither role is not part of the fleet.

    It is sharing the namespace with the run, which is worth saying: whatever
    it is doing on the node is not what this measures.
    """
    stray = [
        _pod(DRIVER_POD, "driver"),
        *(_pod(name, "executor") for name in EXECUTOR_PODS),
        _pod(f"{RUN_OBJECT}-something", ""),
    ]
    assert _drift(pods=_pods(stray)) == [
        f"pod {RUN_OBJECT}-something role: spec driver or executor, engine not reported",
    ]


@pytest.mark.parametrize(
    ("pods", "message"),
    [
        ({"apiVersion": "v1"}, "answered no 'items'"),
        ({"items": {"a": 1}}, "rather than a JSON array"),
        ({"items": [{"metadata": {"name": "p"}}]}, "answered no 'status'"),
        ({"items": [{"metadata": {}, "status": {}}]}, "answered no 'name'"),
    ],
)
def test_a_pod_list_that_is_not_one_is_refused_rather_than_read_as_a_clean_fleet(
    pods: dict[str, object], message: str
) -> None:
    """An empty verdict reads as a verified run, which is the one answer nobody may guess."""
    with pytest.raises(ValueError, match=message):
        _drift(pods=pods)


@pytest.mark.parametrize(
    ("answers", "message"),
    [
        ({APPLICATIONS: {"id": APPLICATION}}, "rather than a JSON array"),
        ({APPLICATIONS: [{"id": APPLICATION}]}, "answered no 'name'"),
        ({APPLICATIONS: [{"id": APPLICATION, "name": RUN_ID}], ENVIRONMENT: {"runtime": {}}}, "no 'sparkProperties'"),
        (
            {APPLICATIONS: [{"id": APPLICATION, "name": RUN_ID}], ENVIRONMENT: {"sparkProperties": [["only"]]}},
            "rather than a key and a value",
        ),
    ],
)
def test_a_driver_answer_that_cannot_be_read_is_refused(answers: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _drift(answers=answers)


def test_the_console_script_separates_drift_from_a_reading_it_could_not_make(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Three answers, three exit codes: verified, drifted, and could not look.

    Staging retries the last and refuses on the middle one, so a shared
    non-zero status would make it either retry a real drift forever or refuse a
    run over a tunnel that was not up yet.
    """
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(SPEC_FILE.read_text())
    pods_path = tmp_path / "pods.json"
    pods_path.write_text(json.dumps(_pods()))
    arguments = [
        "--spec",
        str(spec_path),
        "--run-id",
        RUN_ID,
        "--rest",
        "http://localhost:14040/",
        "--pods",
        str(pods_path),
    ]

    monkeypatch.setattr(verify_module, "fetch_json", _reader(_answers()))
    assert verify_module.main(arguments) == 0

    monkeypatch.setattr(verify_module, "fetch_json", _reader(_answers(properties={"spark.executor.instances": "1"})))
    assert verify_module.main(arguments) == DRIFT_EXIT
    assert capsys.readouterr().out.splitlines() == ["executors: spec 2, engine 1"]

    def refuse(base_url: str) -> Callable[[str], object]:
        def fetch(path: str) -> object:
            raise ValueError(f"could not read {base_url.rstrip('/')}{path}: Connection refused")

        return fetch

    monkeypatch.setattr(verify_module, "fetch_json", refuse)
    assert verify_module.main(arguments) == UNVERIFIED_EXIT
    assert "could not read http://localhost:14040/api/v1/applications" in capsys.readouterr().err


def test_a_pod_list_the_driver_could_not_write_is_worth_another_look(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """An absent file is an unmade reading, not a refused run.

    `kubectl` writes it a moment before this is called, so a file that is not
    there yet is the same kind of answer as a tunnel that has not come up.
    """
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(SPEC_FILE.read_text())
    monkeypatch.setattr(verify_module, "fetch_json", _reader(_answers()))
    status = verify_module.main(
        [
            "--spec",
            str(spec_path),
            "--run-id",
            RUN_ID,
            "--rest",
            "http://localhost:14040",
            "--pods",
            str(tmp_path / "absent.json"),
        ]
    )
    assert status == UNVERIFIED_EXIT
    assert "could not read the pod list at" in capsys.readouterr().err
