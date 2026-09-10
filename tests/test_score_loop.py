# SPDX-License-Identifier: Apache-2.0
import io
import json
from contextlib import suppress
from pathlib import Path
from typing import TextIO

import numpy as np
import pyarrow.parquet as pq
import pytest
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.io import FileIO
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.types import LongType, NestedField

from ingest_bench import uri
from ingest_bench.catalog import open_catalog
from ingest_bench.clock import Clock, now_ms
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.producer import publish_log
from ingest_bench.scorer import cli, score, snapshots
from ingest_bench.table import create
from tests.test_tally import _rows_of

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


class StepClock:
    """Real ordering, fake time: every sleep advances the clock instantly."""

    def __init__(self, start_ms: int) -> None:
        self.t = start_ms

    def now_ms(self) -> int:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += int(seconds * 1000)


@pytest.fixture
def corpus(tmp_path: Path) -> metadata.CorpusMetadata:
    p = preset.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["offered_bytes_per_s=300KB", "duration_s=4", "partition_count=8"],
    )
    generate.generate(p, str(tmp_path / "c"), seed=6, row_block=64)
    return metadata.read(uri.join(str(tmp_path / "c"), preset.corpus_dir_name(p)))


def _props(tmp_path: Path) -> dict[str, str]:
    return {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}


def _finished_producer(
    logs: Path, records: list[generate.BatchRecord], epoch: int, late_batch: int = -1, done: bool = True
) -> None:
    """One shard's publish log: every batch acked, then the done trailer."""
    for record in records:
        late = 9000 if record.batch == late_batch else 50
        publish_log.append(
            logs / "publish_log-0.jsonl",
            publish_log.PublishRecord(
                record.batch,
                epoch + record.offset_ms,
                epoch + record.offset_ms + late,
                epoch + record.offset_ms + late + 250,
                record.rows,
                record.encoded_bytes,
                0,
            ),
        )
    if done:
        publish_log.append_done(logs / "publish_log-0.jsonl", 0, len(records))


def test_scorer_drains_and_validates(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run1", corpus, create.parse_partition("identity(partition_key)"), {})
    # The offer happened just before the engine commits, so commit timestamps follow emit times.
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    # The producer already finished: every batch acked within 300 ms of schedule.
    _finished_producer(logs, records, epoch)

    def engine() -> None:
        # Commit two batches per snapshot, out of order within the pair, like a keyed shuffle would.
        for a, b in ((1, 0), (3, 2)):
            table.append(_rows_of(records[a], corpus))
            table.append(_rows_of(records[b], corpus))

    engine()  # deterministic: all commits exist before the scorer starts; the loop still walks them in order
    clock = StepClock(now_ms())
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run1",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, clock, open(tmp_path / "score.log", "w")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["run_valid"] is True and summary["state"] == "drained" and summary["prefix"] == 3
    fresh = json.loads((tmp_path / "out" / "freshness.json").read_text())
    assert fresh["drained"] and fresh["verdict"] and fresh["window"]["p95_s"] is not None
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["exact"] and exact["rows"] == corpus.row_count
    lines = (tmp_path / "out" / "snapshots.jsonl").read_text().splitlines()
    assert len(lines) == 4 and [json.loads(line)["prefix_after"] for line in lines] == [-1, 1, 1, 3]
    assert "SCORER_DONE run_valid=True" in (tmp_path / "score.log").read_text()


def test_idle_stop_before_drain_is_invalid(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run2", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    table.append(_rows_of(records[0], corpus))  # batch 1 never lands
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run2",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["run_valid"] is False and summary["state"] == "idle_stop" and summary["prefix"] == 0
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["loss_rows"] == sum(r.rows for r in records[1:])


def test_producer_bound_voids(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run3", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch, late_batch=2)
    for r in records:
        table.append(_rows_of(r, corpus))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run3",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["producer_bound"] is True and summary["run_valid"] is False and summary["state"] == "producer_bound"


def _drained_run(tmp_path: Path, corpus: metadata.CorpusMetadata, name: str, late_batch: int = -1) -> Path:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, f"bench.{name}", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch, late_batch=late_batch)
    for record in records:
        table.append(_rows_of(record, corpus))
    out_dir = tmp_path / "out"
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table=f"bench.{name}",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=out_dir,
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    return out_dir


def test_gate_judges_a_run_from_the_artifacts(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    assert cli.gate(["--out", str(tmp_path / "nothing")]) == 5  # no summary is no measurement
    out_dir = _drained_run(tmp_path, corpus, "gate1")
    assert cli.gate(["--out", str(out_dir)]) == 0
    assert score.read_keepup_samples(out_dir / score.KEEPUP_SAMPLES_FILE)[0].backlog_rows == 0


def test_gate_voids_a_producer_bound_run(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    out_dir = _drained_run(tmp_path, corpus, "gate2", late_batch=2)
    assert cli.gate(["--out", str(out_dir)]) == 5


def _table_of(props: dict[str, str], name: str, fields: list[NestedField]) -> Table:
    """A table holding exactly these columns, which `create_table` will not build.

    Every shape the schema check exists to catch is one the harness refuses to
    create, so they are created through the catalog directly.
    """
    catalog = open_catalog(props)
    with suppress(NamespaceAlreadyExistsError):
        catalog.create_namespace("bench", properties={"location": f"{props['warehouse']}/bench"})
    return catalog.create_table(("bench", name), schema=Schema(*fields))


def test_check_table_schema_reads_the_columns_the_table_holds(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    created = create.create_table(props, "bench.shape1", corpus, create.parse_partition("unpartitioned"), {})
    assert snapshots.check_table_schema(created.schema(), corpus) == []

    fields = list(create.iceberg_schema(corpus).fields)
    dropped = _table_of(props, "shape2", [f for f in fields if f.name != "event_type"])
    assert snapshots.check_table_schema(dropped.schema(), corpus) == [
        "the table has no column 'event_type', which the corpus publishes as string"
    ]

    retyped = _table_of(
        props,
        "shape3",
        [f if f.name != "event_time" else NestedField(f.field_id, f.name, LongType(), required=True) for f in fields],
    )
    assert snapshots.check_table_schema(retyped.schema(), corpus) == [
        "column 'event_time' is long in the table and timestamp in the corpus"
    ]

    # An engine free to add a column of its own is still holding the corpus's,
    # and an extra column is under no obligation to be required.
    widened = _table_of(props, "shape4", [*fields, NestedField(900, "ingest_ms", LongType(), required=False)])
    assert snapshots.check_table_schema(widened.schema(), corpus) == []

    optional = _table_of(
        props,
        "shape5",
        [
            f if f.name != "event_type" else NestedField(f.field_id, f.name, f.field_type, required=False)
            for f in fields
        ],
    )
    assert snapshots.check_table_schema(optional.schema(), corpus) == [
        "column 'event_type' is optional, corpus columns are required"
    ]


def test_a_table_missing_a_corpus_column_voids_the_run(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    fields = [f for f in create.iceberg_schema(corpus).fields if f.name != "event_type"]
    table = _table_of(props, "run7", fields)
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    # Every row of the offer is in the table, so nothing but the column set can
    # be what voids this run.
    for record in records:
        table.append(_rows_of(record, corpus).drop_columns(["event_type"]))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run7",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["state"] == "void" and summary["run_valid"] is False
    assert summary["reason"] == (
        "table schema mismatch: the table has no column 'event_type', which the corpus publishes as string"
    )
    # Nothing was tallied: the rows are there, and they describe another table.
    assert summary["committed_rows"] == 0
    assert cli.gate(["--out", str(tmp_path / "out")]) == 5


def test_an_optional_corpus_column_voids_the_run(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    fields = [
        f if f.name != "event_type" else NestedField(f.field_id, f.name, f.field_type, required=False)
        for f in create.iceberg_schema(corpus).fields
    ]
    table = _table_of(props, "run8", fields)
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    # Every row of the offer is in the table and every value is present, so the
    # column's nullability is the only thing left to void this run.
    for record in records:
        table.append(_rows_of(record, corpus))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run8",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["state"] == "void" and summary["run_valid"] is False
    assert summary["reason"] == "table schema mismatch: column 'event_type' is optional, corpus columns are required"
    assert summary["committed_rows"] == 0


def test_score_cli_maps_its_flags(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[score.ScoreArgs] = []

    def capture(args: score.ScoreArgs, clock: Clock, log: TextIO) -> int:
        seen.append(args)
        return 0

    # The CLI reaches the loop through the same module object, so this is the
    # function it will call.
    monkeypatch.setattr(score, "run", capture)
    assert (
        cli.score(
            [
                "--corpus",
                "s3://bench/corpus/events",
                "--table",
                "bench.events",
                "--catalog-prop",
                "type=sql",
                "--catalog-prop",
                "uri=sqlite:///cat.db",
                "--publish-logs",
                "s3://bench/runs/events/producer",
                "--epoch",
                "1700000000.25",
                "--out",
                str(tmp_path / "scores"),
                "--publish-shards",
                "4",
                "--warmup-s",
                "60",
                "--upload-prefix",
                "s3://bench/runs/events/scores",
                "--read-workers",
                "8",
            ]
        )
        == 0
    )
    args = seen[0]
    # --epoch is unix seconds on every command line of the harness, and every
    # figure inside one is milliseconds.
    assert args.epoch_ms == 1_700_000_000_250
    assert args.catalog_props == {"type": "sql", "uri": "sqlite:///cat.db"}
    assert args.expected_publish_shards == 4 and args.warmup_s == 60
    assert args.out_dir == tmp_path / "scores" and args.freshness_bound_s == 180.0
    assert args.upload_prefix == "s3://bench/runs/events/scores"
    assert args.read_workers == 8


def test_the_score_cli_refuses_a_reader_count_below_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A width below one is refused where it is typed rather than mid-run.

    A pool of no threads raises at the first commit the reader reaches, which
    is a fleet staged and an offer begun before anything says the argument was
    what ended the run.
    """

    def unreachable(args: score.ScoreArgs, clock: Clock, log: TextIO) -> int:
        raise AssertionError("the loop must not start on a width it cannot read with")

    monkeypatch.setattr(score, "run", unreachable)
    for width in ("0", "-4"):
        with pytest.raises(SystemExit):
            cli.score(
                [
                    "--corpus",
                    "s3://bench/corpus/events",
                    "--table",
                    "bench.events",
                    "--publish-logs",
                    "s3://bench/runs/events/producer",
                    "--epoch",
                    "1700000000",
                    "--out",
                    str(tmp_path / "scores"),
                    "--read-workers",
                    width,
                ]
            )
        assert "--read-workers" in capsys.readouterr().err


def test_shortened_replay_is_scored_on_what_was_offered(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run4", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    # The producer was given --seconds, so only the first two of the manifest's
    # four batches were ever sent.
    _finished_producer(logs, records[:2], epoch)
    for record in records[:2]:
        table.append(_rows_of(record, corpus))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run4",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["run_valid"] is True and summary["state"] == "drained"
    assert summary["prefix"] == 1 and summary["last_batch"] == 1
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["expected_rows"] == sum(r.rows for r in records[:2]) and exact["expected_rows"] < corpus.row_count
    assert exact["exact"] and exact["loss_rows"] == 0 and exact["scored_batches"] == 2


def test_a_snapshot_arriving_between_polls_is_tallied_once(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run5", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    table.append(_rows_of(records[0], corpus))
    clock = StepClock(now_ms())
    log = io.StringIO()
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run5",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=5.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    state = score._load_inputs(args, clock, log)
    assert score._poll_once(state, clock, log) is True
    assert len(state.seen) == 1 and state.tally.prefix() == 0
    samples = score.read_keepup_samples(args.out_dir / score.KEEPUP_SAMPLES_FILE)
    assert len(samples) == 1 and samples[0].committed_rate is None

    table.append(_rows_of(records[1], corpus))
    clock.sleep(5.0)
    assert score._poll_once(state, clock, log) is True
    assert len(state.seen) == 2 and state.tally.prefix() == 1
    assert state.tally.committed_rows() == records[0].rows + records[1].rows
    samples = score.read_keepup_samples(args.out_dir / score.KEEPUP_SAMPLES_FILE)
    assert len(samples) == 2
    # The whole offer was already published, so only the committed side moved.
    assert samples[1].offered_rate == 0.0 and samples[1].committed_rate == records[1].rows / 5.0

    clock.sleep(5.0)
    assert score._poll_once(state, clock, log) is False
    assert len(state.seen) == 2 and state.tally.committed_rows() == records[0].rows + records[1].rows
    assert len(score.read_keepup_samples(args.out_dir / score.KEEPUP_SAMPLES_FILE)) == 3
    assert len(state.observations) == 2 and [obs.prefix for obs in state.observations] == [0, 1]


def _many_file_commit(tmp_path: Path, corpus: metadata.CorpusMetadata, name: str, files: int) -> Table:
    """A table whose one commit added ``files`` data files, all of one batch.

    That is the shape a wide fleet writing a high-cardinality partition
    produces, and it is built by writing the files and adding them in a single
    commit because what matters here is the count of files one snapshot brings,
    not which writer laid them out.
    """
    props = _props(tmp_path)
    table = create.create_table(props, f"bench.{name}", corpus, create.parse_partition("unpartitioned"), {})
    rows = _rows_of(metadata.read_manifest(corpus.uri)[0], corpus)
    per_file = rows.num_rows // files
    directory = tmp_path / f"{name}-files"
    directory.mkdir()
    written: list[str] = []
    for index in range(files):
        start = index * per_file
        stop = rows.num_rows if index == files - 1 else start + per_file
        path = directory / f"part-{index:05d}.parquet"
        pq.write_table(rows.slice(start, stop - start), path)
        written.append(f"file://{path}")
    table.add_files(written)
    return table


def _many_file_args(
    tmp_path: Path, corpus: metadata.CorpusMetadata, name: str, read_workers: int, poll_interval_s: float = 5.0
) -> score.ScoreArgs:
    logs = tmp_path / f"{name}-logs"
    logs.mkdir()
    _finished_producer(logs, metadata.read_manifest(corpus.uri), now_ms() - 10_000)
    return score.ScoreArgs(
        corpus_uri=corpus.uri,
        table=f"bench.{name}",
        catalog_props=_props(tmp_path),
        publish_logs_uri=str(logs),
        epoch_ms=now_ms() - 10_000,
        out_dir=tmp_path / f"{name}-out",
        poll_interval_s=poll_interval_s,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        read_workers=read_workers,
    )


def test_a_commit_of_many_files_is_tallied_once_at_any_worker_count(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    """One worker or sixteen, the commit tallies to the same figures.

    A batch is judged on a count of its ids and their sum modulo a prime, and
    both are commutative — so the order the reads return in cannot change what
    the tally holds, which is what lets each array be added the moment it
    arrives. The two counts agreeing is that property asserted, and the batch
    reading complete is every file applied exactly once.
    """
    records = metadata.read_manifest(corpus.uri)
    file_count = 64
    figures: list[tuple[int, int, int, int]] = []
    for workers in (1, 16):
        name = f"fanout{workers}"
        _many_file_commit(tmp_path, corpus, name, files=file_count)
        args = _many_file_args(tmp_path, corpus, name, read_workers=workers)
        clock = StepClock(now_ms())
        log = io.StringIO()
        state = score._load_inputs(args, clock, log)
        assert score._poll_once(state, clock, log) is True
        assert state.tally.complete(0), "a file read twice, or not at all"
        # What the poll reports it read, which at either width is every file of
        # the commit: the manifest's own count is the line below.
        assert f"files={file_count}" in log.getvalue().rsplit("POLL t=", 1)[1], log.getvalue()
        line = json.loads((args.out_dir / score.SNAPSHOTS_FILE).read_text().splitlines()[-1])
        figures.append(
            (state.tally.prefix(), state.tally.committed_rows(), int(line["added_files"]), len(state.observations))
        )
    assert figures[0] == figures[1] == (0, records[0].rows, file_count, 1)


def test_a_read_that_fails_inside_the_pool_fails_the_poll_once(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One unreadable file is one failed poll, whatever else the pool had in flight.

    A file the poll did not apply is re-read by the retry, so the failure has
    to belong to the poll rather than to the file: a poll that carried on
    without it would tally the commit short and report the engine as having
    lost the rows.
    """
    _many_file_commit(tmp_path, corpus, "torn", files=64)
    args = _many_file_args(tmp_path, corpus, "torn", read_workers=16)

    def unreadable(path: str, file_format: str) -> np.ndarray:
        raise RuntimeError("the object store stopped answering")

    # The loop reaches it through the same module object, so this is the
    # function it will call.
    monkeypatch.setattr(score, "read_id_column", unreadable)
    clock = StepClock(now_ms())
    log = io.StringIO()
    state = score._load_inputs(args, clock, log)
    assert score._poll_once(state, clock, log) is False
    printed = log.getvalue()
    assert printed.count("POLL_FAILED") == 1, printed
    assert "POLL t=" not in printed, printed
    assert state.read_failures == 1 and state.tally.committed_rows() == 0
    # The commit stays unseen, so the retry reads it whole.
    assert state.seen == set()
    assert score.read_keepup_samples(args.out_dir / score.KEEPUP_SAMPLES_FILE) == []


def test_a_read_phase_past_the_poll_interval_says_so(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A poll longer than its own interval is announced, and a quick one is not.

    Every poll carries what its read cost and how many files it read, and a
    read phase past the interval gets a line of its own — because a reader that
    cannot finish inside its interval is falling behind the table, and the only
    other symptom of it is a verdict voided for staleness, which says nothing
    about which side was slow.
    """
    table = _many_file_commit(tmp_path, corpus, "slow", files=64)
    args = _many_file_args(tmp_path, corpus, "slow", read_workers=1, poll_interval_s=5.0)
    clock = StepClock(now_ms())

    def slowly(document: TableMetadata, snapshot_id: int, io_for_table: FileIO) -> list[snapshots.AddedFile]:
        added = snapshots.added_files(document, snapshot_id, io_for_table)
        # One reader, so the poll pays for each of these files in turn. The
        # clock is charged here rather than inside the reader because a
        # StepClock is not thread-safe and the reads run in the pool.
        clock.sleep(0.1 * len(added))
        return added

    # The loop reaches it through the same module object, so this is the
    # function it will call.
    monkeypatch.setattr(score, "added_files", slowly)
    log = io.StringIO()
    state = score._load_inputs(args, clock, log)
    assert score._poll_once(state, clock, log) is True
    printed = log.getvalue()
    assert "SLOW_POLL poll_ms=6400 files=64 interval_ms=5000" in printed, printed
    assert "poll_ms=6400 files=64" in printed.rsplit("POLL t=", 1)[1], printed

    # The same loop, reading a commit of one file at the speed of the disk.
    monkeypatch.setattr(score, "added_files", snapshots.added_files)
    table.append(_rows_of(metadata.read_manifest(corpus.uri)[1], corpus))
    clock.sleep(5.0)
    assert score._poll_once(state, clock, log) is True
    printed = log.getvalue()
    assert printed.count("SLOW_POLL") == 1, printed
    assert "poll_ms=0 files=1" in printed.rsplit("POLL t=", 1)[1], printed


def test_a_shard_finishing_mid_poll_is_not_scored_over_a_partial_offer(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run6", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    log_path = logs / "publish_log-0.jsonl"
    _finished_producer(logs, records[:3], epoch, done=False)
    for record in records[:3]:
        table.append(_rows_of(record, corpus))
    real_shards_done = publish_log.shards_done

    def finishing(uri_prefix: str) -> set[int]:
        # The shard publishes its last batch and its trailer while this very
        # poll is in flight, which is the race the read order has to survive.
        if not publish_log.shard_done(log_path):
            _finished_producer(logs, records[3:], epoch, done=False)
            publish_log.append_done(log_path, 0, len(records))
        return real_shards_done(uri_prefix)

    # The loop reaches it through the same module object, so this is the
    # function it will call.
    monkeypatch.setattr(publish_log, "shards_done", finishing)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run6",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    # Batch 3 was offered, so the run is still behind: reading the records
    # before the done state would have declared the three-batch list final and
    # scored this as a valid drained run.
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["run_valid"] is False and summary["last_batch"] == 3 and summary["prefix"] == 2
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["scored_batches"] == 4 and exact["loss_rows"] == records[3].rows


def test_the_upload_prefix_mirrors_the_artifacts_the_gate_reads(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    """Both files the gate reads are mirrored every poll, and everything at exit.

    The gate runs beside the scorer rather than inside it, so on a cluster it
    reads these two through the object store — which means a summary written
    only locally is a run nothing can judge until it ends.
    """
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.mirror", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    for record in records:
        table.append(_rows_of(record, corpus))
    out_dir = tmp_path / "out"
    mirror = tmp_path / "mirror"
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.mirror",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=out_dir,
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        upload_prefix=f"file://{mirror}",
    )
    clock = StepClock(now_ms())
    log = io.StringIO()

    state = score._load_inputs(args, clock, log)
    assert score._poll_once(state, clock, log) is True
    assert {path.name for path in mirror.iterdir()} == {score.SUMMARY_FILE, score.KEEPUP_SAMPLES_FILE}

    assert score.run(args, StepClock(now_ms()), log) == 0
    assert {path.name for path in mirror.iterdir()} == {path.name for path in out_dir.iterdir()}
    assert json.loads((mirror / score.SUMMARY_FILE).read_text())["run_valid"] is True


def test_a_failed_scorer_still_publishes_what_it_had(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    """The summary that says the reader is gone is the one a driver has to read.

    A scorer whose catalog stopped answering is the case the mirror exists for:
    without it a driver polling the prefix would keep reading the last summary
    the scorer managed to upload, which said the run was still going.
    """
    props = _props(tmp_path)
    logs = tmp_path / "logs"
    logs.mkdir()
    out_dir = tmp_path / "out"
    mirror = tmp_path / "mirror"

    def unreachable(catalog_props: dict[str, str], table: str) -> Table:
        raise RuntimeError("the catalog stopped answering")

    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.gone",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=now_ms(),
        out_dir=out_dir,
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        upload_prefix=f"file://{mirror}",
    )
    with pytest.MonkeyPatch.context() as patch:
        # The loop reaches it through this module object, so this is the
        # function it will call.
        patch.setattr(score, "load_table", unreachable)
        with pytest.raises(RuntimeError, match="stopped answering"):
            score.run(args, StepClock(now_ms()), io.StringIO())

    assert {path.name for path in mirror.iterdir()} == {path.name for path in out_dir.iterdir()}
    summary = json.loads((mirror / score.SUMMARY_FILE).read_text())
    assert summary["aborted"] is True and summary["reason"] == "scorer_failed: RuntimeError"


def test_a_table_its_engine_has_not_created_yet_is_an_empty_baseline(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    """An engine that creates its table from its first record has none at the start.

    And the scorer starts first, because its first reading is the run's
    baseline — so with `managed_by: engine` the load fails on every poll, the
    launch gives up waiting for that reading, the producer never starts, and the
    engine never sees a record to create the table from. The baseline for an
    absent table is the honest one: zero rows, no snapshots, and keep polling.
    """
    logs = tmp_path / "logs"
    logs.mkdir()
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.never_created",
        catalog_props=_props(tmp_path),
        publish_logs_uri=str(logs),
        epoch_ms=now_ms(),
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        table_managed_by="engine",
    )
    log = tmp_path / "score.log"
    # It ends at the idle stop, having never seen a commit — the engine really
    # did write nothing — but it published a reading on every poll first.
    assert score.run(args, StepClock(now_ms()), open(log, "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["committed_rows"] == 0 and summary["snapshots"] == 0 and summary["prefix"] == -1
    assert summary["aborted"] is True and summary["run_valid"] is False
    printed = log.read_text()
    assert "POLL t=" in printed, printed
    assert "POLL_FAILED" not in printed, printed
    assert "TABLE_ABSENT table=bench.never_created" in printed, printed
    # Announced once and not per poll: it is one state, not one event a second.
    assert printed.count("TABLE_ABSENT") == 1, printed
    samples = score.read_keepup_samples(tmp_path / "out" / score.KEEPUP_SAMPLES_FILE)
    assert samples and all(sample.committed_rows == 0 for sample in samples)


def test_a_harness_managed_table_that_is_absent_is_still_a_failure(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    """Staging created it, so its absence is a fault rather than a phase.

    The bounded retry is what keeps a five-second catalog fault from ending a
    three-hour run; past that the scorer raises, because a run whose table is
    gone is not a run that simply stopped receiving commits.
    """
    logs = tmp_path / "logs"
    logs.mkdir()
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.never_created",
        catalog_props=_props(tmp_path),
        publish_logs_uri=str(logs),
        epoch_ms=now_ms(),
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    log = tmp_path / "score.log"
    with pytest.raises(Exception, match="never_created"):
        score.run(args, StepClock(now_ms()), open(log, "w"))
    printed = log.read_text()
    assert f"POLL_FAILED consecutive={score.MAX_CONSECUTIVE_READ_FAILURES}" in printed, printed


def test_the_schema_is_checked_on_the_first_load_that_succeeds(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    """The check cannot run before the table exists, and must not be skipped either.

    An engine that creates its own table chooses the column set, which is
    exactly the case the check exists for — so it runs on the first load that
    succeeds rather than on the first poll.
    """
    props = _props(tmp_path)
    logs = tmp_path / "logs"
    logs.mkdir()
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.late_shape",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=now_ms(),
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        table_managed_by="engine",
    )
    state = score._load_inputs(args, StepClock(now_ms()), io.StringIO())
    clock = StepClock(now_ms())
    assert score._poll_once(state, clock, io.StringIO()) is False
    assert state.schema_mismatches is None, "there was no table to read a shape off"

    # Now the engine creates one, with a column of its own and one of the
    # corpus's missing.
    _table_of(props, "late_shape", [NestedField(1, "id", LongType(), required=True)])
    assert score._poll_once(state, clock, io.StringIO()) is False
    assert state.schema_mismatches, "the first load that succeeded did not check the shape"


def test_a_run_the_loop_abandoned_is_not_publishable(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    """`aborted` and `run_valid` cannot both be true, and readers check the second.

    An idle stop with every batch landed reads as exact and fresh — the shape
    of it is a shard whose `done` trailer never uploaded — so the two fields
    would contradict each other in the same document.
    """
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.no_trailer", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch, done=False)
    for record in records:
        table.append(_rows_of(record, corpus))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.no_trailer",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["state"] == "idle_stop" and summary["aborted"] is True
    # Every row landed, so the figures beneath it are clean; the run is still
    # not one a result may be published from.
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["exact"] is True
    assert summary["run_valid"] is False
