import json
from pathlib import Path
from typing import TextIO

import pytest

from ingest_bench import uri
from ingest_bench.clock import Clock, now_ms
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.producer import publish_log
from ingest_bench.scorer import cli, score
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


def _finished_producer(logs: Path, records: list[generate.BatchRecord], epoch: int, late_batch: int = -1) -> None:
    """One finished shard's publish log: every batch acked, then the done trailer."""
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


def test_gate_judges_a_leg_from_the_artifacts(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    assert cli.gate(["--out", str(tmp_path / "nothing")]) == 5  # no summary is no measurement
    out_dir = _drained_run(tmp_path, corpus, "gate1")
    assert cli.gate(["--out", str(out_dir)]) == 0
    assert score.read_keepup_samples(out_dir / score.KEEPUP_SAMPLES_FILE)[0].backlog_rows == 0


def test_gate_voids_a_producer_bound_leg(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    out_dir = _drained_run(tmp_path, corpus, "gate2", late_batch=2)
    assert cli.gate(["--out", str(out_dir)]) == 5


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
                "bench.leg",
                "--catalog-prop",
                "type=sql",
                "--catalog-prop",
                "uri=sqlite:///cat.db",
                "--publish-logs",
                "s3://bench/runs/leg/producer",
                "--epoch",
                "1700000000.25",
                "--out",
                str(tmp_path / "scores"),
                "--publish-shards",
                "4",
                "--warmup-s",
                "60",
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
