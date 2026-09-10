# SPDX-License-Identifier: Apache-2.0
"""Check Flink configuration drift against recorded JobManager responses."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from engines.flink import verify as verify_module
from engines.flink.verify import duration_ms, verify
from ingest_bench.readings import DRIFT_EXIT, UNVERIFIED_EXIT
from ingest_bench.specs import engines
from ingest_bench.specs.model import RunSpec, load_run_spec

ROOT = Path(__file__).resolve().parents[1]

# The shipped smoke spec is the run under test: two taskmanagers of four slots
# with four readers, committing every 10 s after a 2 s pause. So the graph it
# asks for is four readers, eight writers and one committer.
SPEC_FILE = ROOT / "runs" / "smoke-flink.yaml"

RUN_ID = "smoke-flink-20260908T120000Z"
JID = "3a5f1c0e9b7d4a2f8c6e1b0d9a7f5c3e"

SOURCE_VERTEX = "Source: kafka_source[1] -> ConstraintEnforcer[2]"
WRITER_VERTEX = "IcebergStreamWriter"
COMMITTER_VERTEX = "IcebergFilesCommitter -> IcebergSink"


def _spec() -> RunSpec:
    return load_run_spec(SPEC_FILE)


def _vertex(name: str, parallelism: int) -> dict[str, object]:
    return {"id": f"v-{name[:8]}", "name": name, "parallelism": parallelism, "maxParallelism": 32, "status": "RUNNING"}


def _answers(
    *,
    state: str = "RUNNING",
    name: str = RUN_ID,
    interval: int = 10_000,
    min_pause: int = 2_000,
    mode: str = "exactly_once",
    source: int = 4,
    writer: int = 8,
    committer: int = 1,
    taskmanagers: int = 2,
    vertices: list[dict[str, object]] | None = None,
    others: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """Build recorded endpoint responses with one reading changed. Preserve unrelated
    fields to catch parsing by position instead of by name.
    """
    graph = (
        vertices
        if vertices is not None
        else [
            _vertex(SOURCE_VERTEX, source),
            _vertex(WRITER_VERTEX, writer),
            _vertex(COMMITTER_VERTEX, committer),
        ]
    )
    return {
        "/jobs/overview": {
            "jobs": [
                *(others if others is not None else []),
                {
                    "jid": JID,
                    "name": name,
                    "state": state,
                    "start-time": 1_757_337_600_000,
                    "end-time": -1,
                    "tasks": {"total": 3, "running": 3},
                },
            ]
        },
        f"/jobs/{JID}/checkpoints/config": {
            "mode": mode,
            "interval": interval,
            "timeout": 600_000,
            "min_pause": min_pause,
            "max_concurrent": 1,
            "unaligned_checkpoints": False,
            "tolerable_failed_checkpoints": 0,
        },
        f"/jobs/{JID}": {"jid": JID, "name": name, "state": state, "vertices": graph},
        "/overview": {
            "taskmanagers": taskmanagers,
            "slots-total": 8,
            "slots-available": 0,
            "jobs-running": 1,
            "flink-version": "1.20.1",
        },
    }


def _fetch(answers: dict[str, object]) -> Callable[[str], object]:
    def fetch(path: str) -> object:
        if path not in answers:
            raise AssertionError(f"verify asked for {path}, which is not one of the recorded {sorted(answers)}")
        return answers[path]

    return fetch


def test_an_engine_that_honours_the_spec_reports_nothing() -> None:
    assert verify(_spec(), RUN_ID, _fetch(_answers())) == []


def test_the_registry_finds_this_module_beside_the_knobs() -> None:
    assert engines.verify_for("flink") is verify_module
    with pytest.raises(ValueError, match="unicorn"):
        engines.verify_for("unicorn")


@pytest.mark.parametrize(
    ("answers", "expected"),
    [
        pytest.param(_answers(state="FAILED"), "job state: spec RUNNING, engine FAILED", id="job-failed"),
        pytest.param(_answers(name="another-run"), "job state: spec RUNNING, engine not found", id="job-absent"),
        pytest.param(
            _answers(interval=5_000), "checkpoint interval ms: spec 10000, engine 5000", id="checkpoint-interval"
        ),
        pytest.param(_answers(min_pause=500), "checkpoint min pause ms: spec 2000, engine 500", id="min-pause"),
        pytest.param(
            _answers(mode="at_least_once"),
            "checkpoint mode: spec exactly_once, engine at_least_once",
            id="checkpoint-mode",
        ),
        pytest.param(_answers(source=2), "source vertex parallelism: spec 4, engine 2", id="source-parallelism"),
        pytest.param(_answers(writer=4), "writer vertex parallelism: spec 8, engine 4", id="writer-parallelism"),
        pytest.param(_answers(committer=2), "committer vertex parallelism: spec 1, engine 2", id="committer"),
        pytest.param(_answers(taskmanagers=1), "taskmanagers: spec 2, engine 1", id="taskmanagers"),
        pytest.param(
            _answers(vertices=[_vertex(SOURCE_VERTEX, 4), _vertex(COMMITTER_VERTEX, 1)]),
            "writer vertices: spec at least 1, engine 0",
            id="writer-absent",
        ),
    ],
)
def test_one_setting_the_engine_dropped_is_one_line(answers: dict[str, object], expected: str) -> None:
    assert verify(_spec(), RUN_ID, _fetch(answers)) == [expected]


def test_a_restarted_job_is_read_off_its_live_attempt() -> None:
    """Job overview includes archived attempts with the same name. Select the live one."""
    earlier = {"jid": "0" * 32, "name": RUN_ID, "state": "FAILED", "start-time": 1, "end-time": 2}
    assert verify(_spec(), RUN_ID, _fetch(_answers(others=[earlier]))) == []


def test_an_override_the_job_was_submitted_with_is_not_drift() -> None:
    """extra_flink_conf overrides knobs and defines the effective value to verify."""
    spec = _spec()
    overridden = replace(
        spec,
        engine_block={**spec.engine_block, "extra_flink_conf": {"execution.checkpointing.min-pause": "9s"}},
    )
    assert verify(overridden, RUN_ID, _fetch(_answers(min_pause=9_000))) == []
    assert verify(overridden, RUN_ID, _fetch(_answers())) == ["checkpoint min pause ms: spec 9000, engine 2000"]


def test_exactly_once_is_not_something_a_spec_can_override() -> None:
    """Exactly-once mode is part of the benchmark contract, not an overridable knob."""
    spec = _spec()
    relaxed = replace(
        spec,
        engine_block={**spec.engine_block, "extra_flink_conf": {"execution.checkpointing.mode": "AT_LEAST_ONCE"}},
    )
    assert verify(relaxed, RUN_ID, _fetch(_answers(mode="at_least_once"))) == [
        "checkpoint mode: spec exactly_once, engine at_least_once"
    ]


@pytest.mark.parametrize(
    ("written", "milliseconds"),
    [("10s", 10_000), ("2 s", 2_000), ("500ms", 500), ("1m", 60_000), ("2h", 7_200_000), ("250", 250)],
)
def test_a_duration_knob_is_read_the_way_flink_reads_it(written: str, milliseconds: int) -> None:
    """Flink interprets a unitless duration as milliseconds."""
    assert duration_ms(written, "spec.flink.checkpoint_interval") == milliseconds


@pytest.mark.parametrize("written", ["10 seconds", "1d", "", "-5s", "1.5s"])
def test_a_duration_this_cannot_read_is_refused_by_name(written: str) -> None:
    with pytest.raises(ValueError, match="checkpoint_interval"):
        duration_ms(written, "spec.flink.checkpoint_interval")


def test_an_answer_that_is_not_the_document_it_should_be_names_the_endpoint() -> None:
    answers = _answers()
    answers[f"/jobs/{JID}"] = {"jid": JID, "name": RUN_ID, "state": "RUNNING"}
    with pytest.raises(ValueError, match=f"/jobs/{JID} answered no 'vertices'"):
        verify(_spec(), RUN_ID, _fetch(answers))


def _reader(answers: dict[str, object]) -> Callable[[str], Callable[[str], object]]:
    def build(base_url: str) -> Callable[[str], object]:
        return _fetch(answers)

    return build


def test_the_console_script_separates_drift_from_not_having_looked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Staging continues on success, rejects drift and retries unreadable endpoints."""
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(SPEC_FILE.read_text())
    arguments = ["--spec", str(spec_path), "--run-id", RUN_ID, "--rest", "http://localhost:18081/"]

    monkeypatch.setattr(verify_module, "fetch_json", _reader(_answers()))
    assert verify_module.main(arguments) == 0

    monkeypatch.setattr(verify_module, "fetch_json", _reader(_answers(taskmanagers=1)))
    assert verify_module.main(arguments) == DRIFT_EXIT
    assert capsys.readouterr().out.splitlines() == ["taskmanagers: spec 2, engine 1"]

    def refuse(base_url: str) -> Callable[[str], object]:
        def fetch(path: str) -> object:
            raise ValueError(f"could not read {base_url.rstrip('/')}{path}: Connection refused")

        return fetch

    monkeypatch.setattr(verify_module, "fetch_json", refuse)
    assert verify_module.main(arguments) == UNVERIFIED_EXIT
    assert "could not read http://localhost:18081/jobs/overview" in capsys.readouterr().err
