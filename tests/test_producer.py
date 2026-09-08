import io
import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from ingest_bench import uri
from ingest_bench.corpus import generate, preset
from ingest_bench.producer import pacing, produce, publish_log

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


class FakeClock:
    def __init__(self, start_ms: int) -> None:
        self.t = start_ms
        self.slept: list[float] = []

    def now_ms(self) -> int:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.t += int(seconds * 1000)


class FakeProducer:
    def __init__(self, clock: FakeClock, fail_every: int = 0, full_first: int = 0) -> None:
        self.clock, self.fail_every, self.full_first = clock, fail_every, full_first
        self.sent: list[tuple[str, bytes, bytes | None]] = []
        self.pending: list[Callable[[], None]] = []

    def produce(
        self, topic: str, value: bytes, key: bytes | None, on_delivery: Callable[[object | None, object], None]
    ) -> None:
        if self.full_first > 0:
            self.full_first -= 1
            raise BufferError("Local: Queue full")
        self.sent.append((topic, value, key))
        n = len(self.sent)
        err = RuntimeError("delivery failed") if self.fail_every and n % self.fail_every == 0 else None
        self.pending.append(lambda: on_delivery(err, object()))

    def poll(self, timeout: float) -> int:
        self.clock.t += 1
        done = len(self.pending)
        for cb in self.pending:
            cb()
        self.pending.clear()
        return done

    def flush(self, timeout: float) -> int:
        self.poll(timeout)
        return 0


@pytest.fixture(scope="module")
def corpus_uri(tmp_path_factory: pytest.TempPathFactory) -> str:
    p = preset.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["offered_bytes_per_s=300KB", "duration_s=4", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("c"))
    generate.generate(p, out, seed=11, row_block=64)
    return uri.join(out, preset.corpus_dir_name(p))


def test_scheduling_and_selection(corpus_uri: str) -> None:
    from ingest_bench.corpus import metadata

    records = metadata.read_manifest(corpus_uri)
    assert pacing.scheduled_ms(1_000, 3000, 6) == 1_500
    assert [r.batch for r in pacing.select_batches(records, 1, 2, None)] == [1, 3]
    assert [r.batch for r in pacing.select_batches(records, 0, 1, 2)] == [0, 1]


def test_produce_batch_acks_and_keys() -> None:
    clock = FakeClock(10_000)
    fp = FakeProducer(clock)
    out = produce.produce_batch(fp, "t", [b"a", b"bb"], [b"k1", b"k2"], clock)
    assert out.rows == 2 and out.bytes == 3 and out.errors == 0
    assert out.first_ack_ms <= out.last_ack_ms
    assert [k for _, _, k in fp.sent] == [b"k1", b"k2"]


def test_queue_full_backs_off_and_retries() -> None:
    clock = FakeClock(0)
    fp = FakeProducer(clock, full_first=3)
    out = produce.produce_batch(fp, "t", [b"a"], None, clock)
    assert out.rows == 1 and len(fp.sent) == 1 and clock.slept


def test_run_writes_publish_log_and_fails_on_delivery_error(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(1_700_000_000_000)
    log = io.StringIO()
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="run-x",
        epoch_ms=clock.now_ms() + 2_000,
        speed=2.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column="user_id",
        publish_log_path=tmp_path / "publish_log-0.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
    )
    fp = FakeProducer(clock)
    assert produce.run(args, lambda cfg: fp, clock, log) == 0
    records = publish_log.read(tmp_path / "publish_log-0.jsonl")
    assert [r.batch for r in records] == [0, 1, 2, 3]
    assert [r.scheduled_ms for r in records] == [args.epoch_ms + o for o in (0, 500, 1000, 1500)]
    assert all(r.first_ack_ms >= r.scheduled_ms and r.last_ack_ms >= r.first_ack_ms and r.errors == 0 for r in records)
    assert sum(r.rows for r in records) == len(fp.sent)
    # `user_id`'s vocabulary is the single prefix "u", and a categorical label is the
    # bare vocabulary entry for the ranks it covers, so rank 0 is "u" and every other
    # rank is "u-<n>". Rank 0 is the modal value under the column's Zipf draw, which
    # is why the suffixed form alone does not cover a batch.
    keys_sent = [key for _, _, key in fp.sent]
    assert all(key is not None and key.startswith(b"u") for key in keys_sent)
    assert sum(1 for key in keys_sent if key is not None and key.startswith(b"u-")) > len(keys_sent) // 2
    text = log.getvalue()
    assert "PRODUCE DONE" in text and "PROGRESS" in text
    assert publish_log.behind_ms(records) < 100
    assert publish_log.emit_times(records)[3] == records[3].last_ack_ms

    failing = FakeProducer(clock, fail_every=50)
    log2 = io.StringIO()
    args2 = produce.ProduceArgs(
        **{**args.__dict__, "publish_log_path": tmp_path / "publish_log-1.jsonl", "key_column": None}
    )
    assert produce.run(args2, lambda cfg: failing, clock, log2) == 1
    assert "PRODUCE FAILED" in log2.getvalue()
    assert all(k is None for _, _, k in failing.sent)


def test_key_column_must_have_a_sidecar(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(0)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="x",
        topic="t",
        epoch_ms=0,
        speed=1.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column="country",
        publish_log_path=tmp_path / "p.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
    )
    with pytest.raises(ValueError, match="kafka_key_columns"):
        produce.run(args, lambda cfg: FakeProducer(clock), clock, io.StringIO())


def test_read_all_merges_shards(tmp_path: Path) -> None:
    for shard, batches in ((0, [0, 2]), (1, [1, 3])):
        path = tmp_path / f"publish_log-{shard}.jsonl"
        for b in batches:
            publish_log.append(path, publish_log.PublishRecord(b, b * 1000, b * 1000 + 5, b * 1000 + 50, 10, 100, 0))
    merged = publish_log.read_all(str(tmp_path))
    assert [r.batch for r in merged] == [0, 1, 2, 3]
    publish_log.append(tmp_path / "publish_log-2.jsonl", publish_log.PublishRecord(3, 0, 0, 0, 1, 1, 0))
    with pytest.raises(ValueError, match="duplicate"):
        publish_log.read_all(str(tmp_path))


def test_done_trailer_marks_the_shard_finished(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(1_700_000_000_000)
    log = io.StringIO()
    finished = tmp_path / "publish_log-2.jsonl"
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=clock.now_ms(),
        speed=1.0,
        shard=2,
        shards=4,
        seconds=None,
        key_column=None,
        publish_log_path=finished,
        behind_max_ms=5000,
        upload_prefix=None,
    )
    assert produce.run(args, lambda cfg: FakeProducer(clock), clock, log) == 0
    assert "PRODUCE DONE" in log.getvalue()
    assert json.loads(finished.read_text().splitlines()[-1]) == {"done": True, "shard": 2, "batches": 1}
    assert publish_log.shard_done(finished)
    assert [r.batch for r in publish_log.read(finished)] == [2]
    assert [r.batch for r in publish_log.read_all(str(tmp_path))] == [2]

    stopped = tmp_path / "publish_log-3.jsonl"
    failing = FakeProducer(clock, fail_every=1)
    stopped_args = replace(args, publish_log_path=stopped, shard=3, shards=4)
    assert produce.run(stopped_args, lambda cfg: failing, clock, io.StringIO()) == 1
    assert not publish_log.shard_done(stopped)
    assert not publish_log.shard_done(tmp_path / "publish_log-9.jsonl")


def test_upload_prefix_publishes_the_log(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(1_700_000_000_000)
    uploads = tmp_path / "uploads"
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=clock.now_ms(),
        speed=1000.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column="partition_key",
        publish_log_path=tmp_path / "local" / "publish_log-0.jsonl",
        behind_max_ms=5000,
        upload_prefix=str(uploads),
    )
    assert produce.run(args, lambda cfg: FakeProducer(clock), clock, io.StringIO()) == 0
    uploaded = uploads / "producer" / "publish_log-0.jsonl"
    assert uploaded.read_bytes() == (tmp_path / "local" / "publish_log-0.jsonl").read_bytes()
    assert [r.batch for r in publish_log.read_all(str(uploads / "producer"))] == [0, 1, 2, 3]
    assert publish_log.shard_done(uploaded)
