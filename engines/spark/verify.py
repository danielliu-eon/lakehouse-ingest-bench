"""What a running query actually honours, read back and compared with the spec.

Staging waits for the SparkApplication to reach RUNNING, which says the
operator submitted something. It does not say that what it submitted is the run
the spec describes: Spark accepts a setting it does not use, an executor the
scheduler could not place leaves the fleet short of what the run was costed
for, and a pod whose CPU request and limit differ runs on cores the node may
reclaim. None of that fails a submission. A run measured under settings nobody
chose is worse than a run that never started, because it publishes a number
attributed to the wrong knobs.

So the sizing a result means something under — how wide the fleet is, how much
each half of it was given, and whether it holds those cores rather than borrows
them — is read off the driver's own API and off the pods the operator made.
`verify` is a function of the spec and two documents, so every drift it can
report is checked against recorded answers instead of a cluster.

The cadence is the one setting that cannot be read back here. A processing-time
trigger is an argument to `writeStream`, not a session setting, and Spark 3.5
publishes no REST resource for a Structured Streaming query — so the interval
is only observable once a batch has run, which is after the producer starts and
long after staging has to decide. What is checkable is that nothing pretends
otherwise: the interval reaches the query through `job.json`, and a setting
under Spark's streaming-trigger prefix would be a run trying to move its own
cadence through configuration that nothing honours.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from engines.spark.knobs import Knobs, read
from ingest_bench.readings import (
    DRIFT_EXIT,
    NOT_REPORTED,
    UNVERIFIED_EXIT,
    document,
    documents,
    fetch_json,
    field,
    line,
    optional_str_field,
    str_field,
)
from ingest_bench.specs.model import RunSpec, load_run_spec

# The one resource whose path holds no application id. The other is built per
# application, in the function that reads it, so each path is written once and
# the request and the name in an error message cannot disagree.
APPLICATIONS = "/api/v1/applications"

# Where the pod list came from, for the refusals about it. Not a path: the
# document is `kubectl`'s answer, handed to this as a file.
PODS = "the pod list"

# What the operator labels the two halves of a fleet with, and the phase and
# QoS class each of their pods has to report.
_ROLE_LABEL = "spark-role"
_DRIVER = "driver"
_EXECUTOR = "executor"
_RUNNING_PHASE = "Running"
_GUARANTEED = "Guaranteed"

# A setting under this prefix would be a run moving its own commit cadence
# somewhere nothing reads it. Spark has no such setting, which is exactly what
# makes its absence the checkable half of the cadence — see the module note.
_TRIGGER_PREFIX = "spark.sql.streaming.trigger"

# What a setting the driver does not report at all reads as. Absence is a
# reading about the run rather than an unreadable document, so it is compared
# like any other value.
_NOT_SET = "not set"


def _effective(knobs: Knobs, key: str, knob: str) -> str:
    """The value the query was submitted with for ``key``.

    An override in `extra_spark_conf` is applied after the knob it displaces,
    so it is what the engine was told — and reporting the knob instead would be
    drift on a run whose author chose the override.
    """
    if key in knobs.extra_spark_conf:
        return knobs.extra_spark_conf[key]
    return knob


def _application(run_id: str, answer: object) -> tuple[str, list[str]]:
    """The run's application id, and the drift when there is not exactly one.

    Read by name rather than taken as the only one listed: the endpoint is
    reached through a tunnel, which is addressed by port and not by pod, so the
    name is what says the application read is the run just applied.
    """
    applications = documents(answer, APPLICATIONS)
    named = [entry for entry in applications if str_field(entry, "name", APPLICATIONS) == run_id]
    if len(named) != 1:
        return "", [line("applications named after the run", 1, len(named))]
    return str_field(named[0], "id", APPLICATIONS), []


def _properties(answer: object, where: str) -> dict[str, str]:
    """The driver's Spark settings, from the pairs the environment reports them as.

    The endpoint answers `sparkProperties` as a list of two-element arrays
    rather than an object, so the pairs are folded into a mapping here.
    """
    reported = document(answer, where)
    pairs = field(reported, "sparkProperties", where)
    if not isinstance(pairs, list):
        raise ValueError(f"{where} answered sparkProperties as {type(pairs).__name__} rather than a JSON array")
    properties: dict[str, str] = {}
    for index, entry in enumerate(pairs):
        if not isinstance(entry, list) or len(entry) != 2:
            raise ValueError(f"{where} sparkProperties[{index}] is {entry!r} rather than a key and a value")
        key, value = entry
        properties[str(key)] = str(value)
    return properties


def _conf_drift(knobs: Knobs, fetch: Callable[[str], object], application: str) -> list[str]:
    """Every setting the driver was submitted with that is not the one asked for.

    Read off the driver rather than off the document that was applied, because
    the operator restates the fleet's sizing as `spark-submit` arguments: a
    knob that reached the object and not the session would otherwise pass
    unnoticed.
    """
    where = f"{APPLICATIONS}/{application}/environment"
    properties = _properties(fetch(where), where)
    settings: tuple[tuple[str, str, str], ...] = (
        ("executors", "spark.executor.instances", str(knobs.executors)),
        ("executor cores", "spark.executor.cores", str(knobs.executor_cores)),
        ("executor memory", "spark.executor.memory", f"{knobs.executor_mem_mb}m"),
        ("driver cores", "spark.driver.cores", str(knobs.driver_cores)),
        ("driver memory", "spark.driver.memory", f"{knobs.driver_mem_mb}m"),
        ("shuffle partitions", "spark.sql.shuffle.partitions", str(knobs.cores_total())),
    )
    lines: list[str] = []
    for what, key, knob in settings:
        expected = _effective(knobs, key, knob)
        actual = properties[key] if key in properties else _NOT_SET
        if expected != actual:
            lines.append(line(what, expected, actual))
    pretended = sorted(key for key in properties if key.startswith(_TRIGGER_PREFIX))
    if pretended:
        lines.append(line("trigger settings", "none", pretended))
    return lines


@dataclass(frozen=True)
class _Pod:
    """One pod of a run's fleet, as far as this reads them."""

    name: str
    role: str
    phase: str
    qos_class: str


def _pods(answer: object) -> list[_Pod]:
    items = documents(field(document(answer, PODS), "items", PODS), f"{PODS}'s items")
    pods: list[_Pod] = []
    for item in items:
        metadata = document(field(item, "metadata", PODS), f"{PODS}'s metadata")
        status = document(field(item, "status", PODS), f"{PODS}'s status")
        labels = {} if "labels" not in metadata else document(metadata["labels"], f"{PODS}'s labels")
        pods.append(
            _Pod(
                name=str_field(metadata, "name", PODS),
                role=optional_str_field(labels, _ROLE_LABEL, PODS),
                phase=optional_str_field(status, "phase", PODS),
                # A pod carries no QoS class until the API server admits it, so
                # absence here is a pod that is not yet running rather than a
                # document that could not be read.
                qos_class=optional_str_field(status, "qosClass", PODS),
            )
        )
    return pods


def _pod_drift(knobs: Knobs, pods: object) -> list[str]:
    """Every pod of the fleet that is not one the run asked for.

    Two readings the driver cannot make about itself. The count is the fleet a
    result is costed for, and an executor the scheduler never placed leaves a
    run measured on fewer. Guaranteed is the other: a Burstable pod's cores are
    a share the node may reclaim under pressure, so a rate measured on one is
    the node's answer rather than the engine's.
    """
    fleet = _pods(pods)
    drivers = [pod for pod in fleet if pod.role == _DRIVER]
    executors = [pod for pod in fleet if pod.role == _EXECUTOR]
    lines: list[str] = []
    if len(drivers) != 1:
        lines.append(line("driver pods", 1, len(drivers)))
    if len(executors) != knobs.executors:
        lines.append(line("executor pods", knobs.executors, len(executors)))
    for pod in (*drivers, *executors):
        if pod.phase != _RUNNING_PHASE:
            lines.append(line(f"pod {pod.name} phase", _RUNNING_PHASE, pod.phase or NOT_REPORTED))
        if pod.qos_class != _GUARANTEED:
            lines.append(line(f"pod {pod.name} qos class", _GUARANTEED, pod.qos_class or NOT_REPORTED))
    for pod in fleet:
        if pod.role not in (_DRIVER, _EXECUTOR):
            # A pod the selector matched and the operator did not label as
            # either half of the fleet is not part of the run this measures,
            # and it is sharing the run's namespace with it.
            lines.append(line(f"pod {pod.name} role", f"{_DRIVER} or {_EXECUTOR}", pod.role or NOT_REPORTED))
    return lines


def verify(spec: RunSpec, run_id: str, fetch: Callable[[str], object], pods: object) -> list[str]:
    """One line per setting the running query does not honour; empty when it does.

    ``fetch`` answers a path on the driver's API with the parsed document and
    ``pods`` is the run's pod list as `kubectl` reports it, which is what keeps
    this a function of recorded JSON. A document that cannot be read as the one
    it should be raises rather than returning a clean verdict.
    """
    knobs = read(spec.engine_block)
    application, lines = _application(run_id, fetch(APPLICATIONS))
    if application:
        lines.extend(_conf_drift(knobs, fetch, application))
    return [*lines, *_pod_drift(knobs, pods)]


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="verify-spark",
        description="Check a running Spark query's effective settings against the spec that asked for them.",
    )
    parser.add_argument("--spec", required=True, metavar="PATH", help="the run spec the query was staged from")
    parser.add_argument(
        "--run-id",
        required=True,
        metavar="ID",
        help="the run, which is also the application's name — so this is what says the one read is the one started",
    )
    parser.add_argument(
        "--rest", required=True, metavar="URL", help="the driver's UI endpoint, e.g. http://localhost:14040"
    )
    parser.add_argument(
        "--pods",
        required=True,
        metavar="PATH",
        help="the run's pods as JSON, from `kubectl get pods -l <selector> -o json`",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_id = str(args.run_id)
    try:
        pods = _read_pods(Path(str(args.pods)))
        drift = verify(load_run_spec(Path(str(args.spec))), run_id, fetch_json(str(args.rest)), pods)
    except ValueError as error:
        # Not a verdict: the run could not be judged at all, which a caller
        # answers by looking again rather than by refusing the run.
        print(error, file=sys.stderr)
        return UNVERIFIED_EXIT
    for drifted in drift:
        print(drifted)
    if drift:
        return DRIFT_EXIT
    # On stderr, because the drift lines are this command's answer and a caller
    # that captures them should not have to filter this out of them.
    print(f"verified: {run_id} is running the settings its spec asked for", file=sys.stderr)
    return 0


def _read_pods(path: Path) -> object:
    """The pod list off a file, or a refusal naming it.

    A file the driver could not write is the same kind of answer as an endpoint
    that did not respond — worth another look rather than a run refused — so it
    raises the same way every other unreadable document does.
    """
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ValueError(f"could not read the pod list at {path}: {error}") from error


if __name__ == "__main__":
    raise SystemExit(main())
