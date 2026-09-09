import io
import json
from contextlib import suppress
from pathlib import Path
from typing import TextIO

import pytest
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
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
