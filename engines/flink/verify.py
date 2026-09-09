# SPDX-License-Identifier: Apache-2.0
"""What a running job actually honours, read back and compared with the spec.

Staging waits for the job to reach RUNNING, which says the operator started
something. It does not say that what it started is the run the spec describes:
Flink drops a configuration key it does not know, a connector ignores a hint it
does not implement, and every vertex is sized by whatever configuration reached
it — none of which fails a submission. A run measured under settings nobody
chose is worse than a run that never started, because it publishes a number
attributed to the wrong knobs.

So the settings that decide what a result means — the commit cadence, the
exactness promise, how many readers and writers, how large the fleet — are read
off the JobManager's REST endpoint and compared here. `verify` is a function of
the spec and four JSON documents, so every drift it can report is checked
against recorded answers instead of a cluster.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from engines.flink.knobs import SOURCE_TABLE, Knobs, read
from ingest_bench.readings import (
    DRIFT_EXIT,
    UNVERIFIED_EXIT,
    document,
    documents,
    fetch_json,
    field,
    int_field,
    line,
    str_field,
)
from ingest_bench.specs.model import RunSpec, load_run_spec

# The two resources whose paths hold no job id. The other two are built per
# job, in the functions that read them, so each path is written once and the
# request and the name in an error message cannot disagree.
JOBS_OVERVIEW = "/jobs/overview"
CLUSTER_OVERVIEW = "/overview"

RUNNING = "RUNNING"

# What a job the cluster has never heard of is reported as. A name rather than
# an empty state, because the line is read by a person deciding whether to
# restage or to look at the operator's log.
NOT_FOUND = "not found"

# The endpoint's spelling of the mode `render_conf` submits as `EXACTLY_ONCE`.
EXACTLY_ONCE = "exactly_once"

# The three operators a run is sized by, as the job graph names them. The
# source is a prefix: Flink chains a SQL source with whatever follows it and
# suffixes the operator with its transformation id, so the name grows to the
# right of the table's own. The other two are substrings of the chain they sit
# in for the same reason.
_SOURCE_PREFIX = f"Source: {SOURCE_TABLE}"
_WRITER = "IcebergStreamWriter"
_COMMITTER = "IcebergFilesCommitter"

# Iceberg's sink serialises its commits through one committer whatever the
# writers do, so a committer at any other parallelism is not the sink this
# benchmark measures.
COMMITTER_PARALLELISM = 1

# The two settings a run can move out from under its own knobs: `render_conf`
# applies `extra_flink_conf` last, so a value written there is what the job was
# submitted with.
_INTERVAL_KEY = "execution.checkpointing.interval"
_MIN_PAUSE_KEY = "execution.checkpointing.min-pause"

# Flink's duration grammar, as `TimeUtils.parseDuration` reads it: an integer,
# optional whitespace, and a unit label that is milliseconds when there is
# none. Only the four labels a commit cadence is written in are accepted — a
# duration this cannot read is refused rather than compared against the wrong
# scale.
_DURATION_RE = re.compile(r"^(\d+)\s*([a-z]*)$")
_UNIT_MS = {"": 1, "ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000}


def duration_ms(value: str, where: str) -> int:
    """A Flink duration setting in milliseconds, or a refusal naming ``where``."""
    match = _DURATION_RE.match(value.strip().lower())
    if match is None or match.group(2) not in _UNIT_MS:
        raise ValueError(
            f"{where} is {value!r}, which is not a duration this can read: write a whole number of "
            "milliseconds, or one suffixed ms, s, m or h"
        )
    return int(match.group(1)) * _UNIT_MS[match.group(2)]


# ---------------------------------------------------------------------------
# The readings
# ---------------------------------------------------------------------------


def _effective(knobs: Knobs, key: str, knob: str) -> str:
    """The value the job was submitted with for ``key``.

    An override in `extra_flink_conf` is applied after the knob it displaces,
    so it is what the engine was told — and reporting the knob instead would
    be drift on a run whose author chose the override.
    """
    if key in knobs.extra_flink_conf:
        return knobs.extra_flink_conf[key]
    return knob


def _job(run_id: str, overview: object) -> tuple[str, str]:
    """The run's job state, and the id the other readings are made against.

    A job the cluster restarted keeps the run's name, and the jobmanager
    archives the attempt it gave up on beside the one it is running — so a
    name is not unique and the live attempt is the one a reading of the graph
    can be attributed to. The id is empty for every state but RUNNING, which
    is the only one anything further is read under.
    """
    jobs = documents(field(document(overview, JOBS_OVERVIEW), "jobs", JOBS_OVERVIEW), f"{JOBS_OVERVIEW}.jobs")
    named = [job for job in jobs if str_field(job, "name", JOBS_OVERVIEW) == run_id]
    if not named:
        return NOT_FOUND, ""
    for job in named:
        if str_field(job, "state", JOBS_OVERVIEW) == RUNNING:
            return RUNNING, str_field(job, "jid", JOBS_OVERVIEW)
    return str_field(named[0], "state", JOBS_OVERVIEW), ""


def _checkpoint_drift(knobs: Knobs, fetch: Callable[[str], object], jid: str) -> list[str]:
    where = f"/jobs/{jid}/checkpoints/config"
    config = document(fetch(where), where)
    durations: tuple[tuple[str, str, str, str], ...] = (
        ("checkpoint interval ms", _INTERVAL_KEY, knobs.checkpoint_interval, "interval"),
        ("checkpoint min pause ms", _MIN_PAUSE_KEY, knobs.min_pause, "min_pause"),
    )
    lines: list[str] = []
    for what, key, knob, reported in durations:
        expected = duration_ms(_effective(knobs, key, knob), key)
        actual = int_field(config, reported, where)
        if expected != actual:
            lines.append(line(what, expected, actual))
    # Against the constant and not against the submitted configuration:
    # exactly once is the promise duplication is scored against rather than a
    # knob, so a run that relaxed it through `extra_flink_conf` is precisely
    # what this line exists to catch.
    mode = str_field(config, "mode", where)
    if mode != EXACTLY_ONCE:
        lines.append(line("checkpoint mode", EXACTLY_ONCE, mode))
    return lines


def _vertex_drift(knobs: Knobs, fetch: Callable[[str], object], jid: str) -> list[str]:
    """Every vertex whose parallelism is not the one the spec sizes its role at.

    The writers' number is the sink hint the SQL carries, which is computed
    from the knobs — so unlike the checkpoint settings it is the knobs, and
    not the submitted configuration, that says what the engine was told.
    """
    where = f"/jobs/{jid}"
    vertices = documents(field(document(fetch(where), where), "vertices", where), f"{where}.vertices")
    roles: tuple[tuple[str, Callable[[str], bool], int], ...] = (
        ("source", lambda name: name.startswith(_SOURCE_PREFIX), knobs.parallelism_default()),
        ("writer", lambda name: _WRITER in name, knobs.slots_total()),
        ("committer", lambda name: _COMMITTER in name, COMMITTER_PARALLELISM),
    )
    lines: list[str] = []
    for label, matches, expected in roles:
        matched = [vertex for vertex in vertices if matches(str_field(vertex, "name", where))]
        if not matched:
            # A graph missing a role is not a graph the spec describes, and
            # the parallelism it would have been sized at cannot be read from
            # a vertex that is not there.
            lines.append(line(f"{label} vertices", "at least 1", 0))
            continue
        for vertex in matched:
            actual = int_field(vertex, "parallelism", where)
            if actual != expected:
                lines.append(line(f"{label} vertex parallelism", expected, actual))
    return lines


def _fleet_drift(knobs: Knobs, fetch: Callable[[str], object]) -> list[str]:
    """The registered taskmanagers, against the fleet the run asked for.

    The count has no configuration key to be overridden through: the operator
    sizes the containers from the same knob, so the knob is the whole of what
    the spec asked for here.
    """
    overview = document(fetch(CLUSTER_OVERVIEW), CLUSTER_OVERVIEW)
    taskmanagers = int_field(overview, "taskmanagers", CLUSTER_OVERVIEW)
    if taskmanagers == knobs.taskmanagers:
        return []
    return [line("taskmanagers", knobs.taskmanagers, taskmanagers)]


def verify(spec: RunSpec, run_id: str, fetch: Callable[[str], object]) -> list[str]:
    """One line per setting the running job does not honour; empty when it does.

    ``fetch`` answers a REST path with the parsed document, which is what
    keeps this a function of recorded JSON. A document that cannot be read as
    the one it should be raises rather than returning a clean verdict.
    """
    knobs = read(spec.engine_block)
    state, jid = _job(run_id, fetch(JOBS_OVERVIEW))
    if state != RUNNING:
        # The only reading worth making about a job that is not running. The
        # vertices of a failed one report the parallelism it had, which says
        # nothing about the run being staged.
        return [line("job state", RUNNING, state)]
    return [
        *_checkpoint_drift(knobs, fetch, jid),
        *_vertex_drift(knobs, fetch, jid),
        *_fleet_drift(knobs, fetch),
    ]


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="verify-flink",
        description="Check a running Flink job's effective settings against the spec that asked for them.",
    )
    parser.add_argument("--spec", required=True, metavar="PATH", help="the run spec the job was staged from")
    parser.add_argument(
        "--run-id",
        required=True,
        metavar="ID",
        help="the run, which is also the job's name — so this is what says the job read is the job just started",
    )
    parser.add_argument(
        "--rest", required=True, metavar="URL", help="the jobmanager's REST endpoint, e.g. http://localhost:8081"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_id = str(args.run_id)
    try:
        drift = verify(load_run_spec(Path(str(args.spec))), run_id, fetch_json(str(args.rest)))
    except ValueError as error:
        # Not a verdict: the run could not be judged at all, which a caller
        # answers by looking again rather than by refusing the run.
        print(error, file=sys.stderr)
        return UNVERIFIED_EXIT
    for drifted in drift:
        print(drifted)
    if drift:
        return DRIFT_EXIT
    # On stderr, because the drift lines are this command's answer and a
    # caller that captures them should not have to filter this out of them.
    print(f"verified: {run_id} is running the settings its spec asked for", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
