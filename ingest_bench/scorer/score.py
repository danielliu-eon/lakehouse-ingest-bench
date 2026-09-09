"""The scoring loop: walk a table's commits as they land, and judge the run.

Every figure a run is reported by comes out of one pass over the table's
commits, taken while the run is still going. The loop exists in that shape for
two reasons. A run offers hundreds of gigabytes, so re-reading the table at
each point of interest would cost more than the ingest under test; and the
verdict has to be available before the run is torn down, because a sweep
abandons an undersized fleet rather than paying for its full duration.

So the live estimate and the final verdict are the same computation, and the
verdict exists the moment the table drains. There is no post-drain pass: the
tally, the observations and the keep-up samples the loop has already
accumulated are exactly what the final figures are drawn from.

Every artifact is written as it goes. A run that dies part way through still
says how far it got, and `gate` reads these files while the loop is still
writing them.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TextIO, cast

from ingest_bench import uri
from ingest_bench.clock import Clock
from ingest_bench.corpus import metadata
from ingest_bench.producer import publish_log
from ingest_bench.scorer import freshness
from ingest_bench.scorer.exactness import exactness_result
from ingest_bench.scorer.keepup import KeepupSample, keepup_summary, make_sample
from ingest_bench.scorer.snapshots import (
    added_files,
    check_table_schema,
    load_table,
    read_metadata,
    snapshots_in_order,
)
from ingest_bench.scorer.tally import BatchTally, read_id_column

APPEND = "append"

RUNNING = "running"
DRAINED = "drained"
IDLE_STOP = "idle_stop"
PRODUCER_BOUND = "producer_bound"
VOID = "void"

IDLE_STOP_REASON = "idle_stop_before_drain"
SCHEMA_MISMATCH_REASON = "table schema mismatch"

SNAPSHOTS_FILE = "snapshots.jsonl"
KEEPUP_SAMPLES_FILE = "keepup_samples.jsonl"
SUMMARY_FILE = "summary.json"
FRESHNESS_FILE = "freshness.json"
EXACTNESS_FILE = "exactness.json"
KEEPUP_FILE = "keepup.json"

GRID_MS = 1000

# A catalog, an object store and a publish-log prefix are all remote, and any
# of them can refuse one poll. Retrying a bounded number of times is what keeps
# a five-second network fault from ending a three-hour run; raising after that
# is what keeps a run that has lost its inputs from being scored as one that
# simply stopped receiving commits.
MAX_CONSECUTIVE_READ_FAILURES = 5


@dataclass(frozen=True)
class ScoreArgs:
    """One run's scoring inputs, all of them facts the run was started with."""

    corpus_uri: str
    table: str
    catalog_props: dict[str, str]
    publish_logs_uri: str
    epoch_ms: int
    out_dir: Path
    poll_interval_s: float = 5.0
    idle_stop_s: float = 300.0
    warmup_s: int = 120
    freshness_bound_s: float = 180.0
    speed: float = 1.0
    behind_max_ms: int = 5000
    expected_publish_shards: int = 1
    upload_prefix: str | None = None


@dataclass
class ScoreState:
    """Everything the loop carries between polls.

    The state is accumulated rather than recomputed: the tally holds the rows
    of every commit already read, and `seen` is what keeps a commit from being
    read twice. Nothing here can be rebuilt from the table alone once snapshots
    expire, which is why the artifacts are written as it goes.
    """

    args: ScoreArgs
    corpus: metadata.CorpusMetadata
    tally: BatchTally
    last_new_ms: int
    observations: list[freshness.Observation] = field(default_factory=list)
    samples: list[KeepupSample] = field(default_factory=list)
    seen: set[int] = field(default_factory=set)
    # The data files of the commit currently being read. A poll that fails
    # part-way through a commit is retried, and its rows must not be tallied
    # twice; the set is cleared once the commit is fully applied.
    applied_files: set[str] = field(default_factory=set)
    records: list[publish_log.PublishRecord] = field(default_factory=list)
    # The corpus columns the table does not hold, or None until the table has
    # been loaded once. An engine-created table need not exist when the scorer
    # starts, so the check cannot happen before the first successful load.
    schema_mismatches: list[str] | None = None
    offer_ended: bool = False
    read_failures: int = 0
    state: str = RUNNING
    reason: str | None = None
    aborted: bool = False
    result: freshness.FreshnessResult | None = None
    exactness: dict[str, object] | None = None

    def offered_rows(self) -> int:
        return sum(record.rows for record in self.records)

    def emit_ms(self) -> dict[int, int]:
        return publish_log.emit_times(self.records)

    def last_batch(self) -> int:
        """The batch the prefix has to reach for the run to have drained.

        While the offer is running that is the whole corpus. Once it has ended
        it is the last batch actually published: a corpus replayed with
        `--seconds` never offers the rest, and waiting for them would turn
        every shortened replay into an idle stop.
        """
        if self.offer_ended:
            return max((record.batch for record in self.records), default=-1)
        return self.corpus.batch_count - 1

    def producer_bound(self) -> bool:
        """Whether the offer, rather than the engine, was the bottleneck.

        A run whose producer could not keep to the schedule says nothing about
        how fresh an engine kept the table, so this voids the run instead of
        being reported as engine lag.
        """
        return publish_log.behind_ms(self.records) > self.args.behind_max_ms or any(
            record.errors for record in self.records
        )

    def run_valid(self) -> bool:
        """Whether a result may be published from this run.

        False for a run still going: nothing is valid until the table has
        drained, because the figures a partial run offers are a lower bound on
        its lag and an upper bound on its exactness.
        """
        return (
            not self.schema_mismatches
            and self.result is not None
            and self.result.verdict
            and self.exactness is not None
            and bool(self.exactness["exact"])
            and not self.producer_bound()
        )


def _append_line(path: Path, row: dict[str, object]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()


def _write_json(path: Path, document: dict[str, object]) -> None:
    """Publish one artifact, replacing it whole.

    Written through a temporary and renamed, because `gate` reads these while
    the loop is still writing them: a reader that caught a half-written summary
    would judge the run on a truncated document.
    """
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _mirror(args: ScoreArgs, names: Iterable[str]) -> None:
    """Copy the named artifacts to the upload prefix, if there is one.

    Called after each file is written locally rather than instead of writing
    it: the local copy is what the loop appends to and what a mounted volume
    keeps, and the prefix is how another pod — the gate, or an operator — reads
    a run it is not sharing a filesystem with.
    """
    if args.upload_prefix is None:
        return
    for name in names:
        uri.write_bytes(uri.join(args.upload_prefix, name), (args.out_dir / name).read_bytes())


def _mirror_everything(args: ScoreArgs) -> None:
    """Mirror every artifact the run produced, however the run ended.

    A failed scorer publishes its artifacts too. The summary it leaves says the
    reader is gone, and that is precisely what a driver has to be able to read.
    """
    _mirror(args, sorted(path.name for path in args.out_dir.iterdir() if path.is_file()))


def _as_int(value: object) -> int:
    return int(cast(int, value))


def _as_optional_float(value: object) -> float | None:
    return None if value is None else float(cast(float, value))


def read_keepup_samples(path: Path) -> list[KeepupSample]:
    """The keep-up samples the loop wrote, for a reader judging a run in flight."""
    samples: list[KeepupSample] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = cast(dict[str, object], json.loads(line))
        try:
            samples.append(
                KeepupSample(
                    at_ms=_as_int(raw["at_ms"]),
                    offered_rows=_as_int(raw["offered_rows"]),
                    committed_rows=_as_int(raw["committed_rows"]),
                    backlog_rows=_as_int(raw["backlog_rows"]),
                    offered_rate=_as_optional_float(raw["offered_rate"]),
                    committed_rate=_as_optional_float(raw["committed_rate"]),
                )
            )
        except KeyError as err:
            raise ValueError(f"{path} holds a sample missing key {err.args[0]!r}: {line}") from err
    return samples


def _offer_ended(args: ScoreArgs) -> bool:
    """Whether every producer shard has published everything it selected."""
    done = publish_log.shards_done(args.publish_logs_uri)
    expected = set(range(args.expected_publish_shards))
    surplus = sorted(done - expected)
    if surplus:
        raise ValueError(
            f"publish logs under {args.publish_logs_uri} carry a done marker for shard(s) {surplus}, past the "
            f"{args.expected_publish_shards} this run expects; pass --publish-shards to match the producer"
        )
    return done == expected


def _offer_end_ms(state: ScoreState) -> int | None:
    """When the last row of the offer was acknowledged, or None while it runs."""
    if not state.offer_ended or not state.records:
        return None
    return max(record.last_ack_ms for record in state.records)


def _drained_ms(state: ScoreState) -> int | None:
    """The commit at which the prefix first reached the last batch."""
    last = state.last_batch()
    if last < 0:
        return None
    return next((obs.timestamp_ms for obs in state.observations if obs.prefix >= last), None)


def _keepup(state: ScoreState) -> dict[str, object]:
    return keepup_summary(state.samples, _offer_end_ms(state), _drained_ms(state))


def _live_lag_s(state: ScoreState) -> float | None:
    """The lag as of the last sample, which is what the gate judges a run on.

    Measured the way `lag_series` measures it, so the live figure and the
    published series cannot disagree: from the epoch while no batch is
    complete, and from the newest complete batch's emit time after that.
    """
    if not state.samples:
        return None
    prefix = state.tally.prefix()
    # A prefix whose batch has no publish record yet is a log not uploaded, not
    # a missing input, so the honest answer is that the lag is not measurable.
    reference = state.args.epoch_ms if prefix < 0 else state.emit_ms().get(prefix)
    return None if reference is None else (state.samples[-1].at_ms - reference) / 1000


def _write_summary(state: ScoreState) -> None:
    """The one summary writer, used live and at the end.

    Live and final summaries come from this function alone so they cannot
    drift: a driver polling a run in flight reads the same fields, with the
    same meanings, that the finished run publishes.
    """
    result = state.result
    document: dict[str, object] = {
        "run_valid": state.run_valid(),
        "state": state.state,
        "reason": state.reason,
        "aborted": state.aborted,
        "prefix": state.tally.prefix(),
        "last_batch": state.last_batch(),
        "committed_rows": state.tally.committed_rows(),
        "offered_rows": state.offered_rows(),
        "backlog_rows": state.samples[-1].backlog_rows if state.samples else None,
        "lag_s": _live_lag_s(state),
        "snapshots": len(state.seen),
        "offer_ended": state.offer_ended,
        "producer": {
            "behind_ms": publish_log.behind_ms(state.records),
            "errors": sum(record.errors for record in state.records),
        },
        "producer_bound": state.producer_bound(),
        "freshness": None if result is None else asdict(result),
        "exactness": state.exactness,
        "keepup": _keepup(state),
        "clock_skew_suspected": freshness.clock_skew_suspected(state.observations, state.emit_ms()),
        "epoch_ms": state.args.epoch_ms,
        "freshness_bound_s": state.args.freshness_bound_s,
        "warmup_s": state.args.warmup_s,
        "speed": state.args.speed,
        "table": state.args.table,
        "corpus_uri": state.corpus.uri,
        "corpus_hash": state.corpus.corpus_hash,
    }
    _write_json(state.args.out_dir / SUMMARY_FILE, document)


def _load_inputs(args: ScoreArgs, clock: Clock, log: TextIO) -> ScoreState:
    corpus = metadata.read(args.corpus_uri)
    tally = BatchTally(metadata.read_manifest(args.corpus_uri), corpus.p)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    # The append-only artifacts are truncated here rather than appended to: a
    # second scorer pointed at the same directory would otherwise publish one
    # file describing two runs.
    for name in (SNAPSHOTS_FILE, KEEPUP_SAMPLES_FILE):
        (args.out_dir / name).write_text("", encoding="utf-8")
    print(
        f"SCORER_START table={args.table} batches={corpus.batch_count} rows={corpus.row_count} "
        f"epoch_ms={args.epoch_ms}",
        file=log,
        flush=True,
    )
    return ScoreState(args=args, corpus=corpus, tally=tally, last_new_ms=clock.now_ms())


def _read_inputs(state: ScoreState, clock: Clock) -> bool:
    """Read both sides of the run and apply every commit not yet seen.

    Only appends feed the tally. A rewrite re-adds rows the tally already holds
    — the same ids in new files — so counting it would read a compaction as the
    engine duplicating rows. Every commit is recorded in `snapshots.jsonl`
    whatever its operation, so what the table did stays visible.
    """
    # The done state is read before the records, and that order is what makes
    # the record list trustworthy: a shard appends its trailer after its last
    # record, so a trailer already present when the records are read guarantees
    # those records are complete. Read the other way round, a shard finishing
    # between the two reads would have a record list missing its last batch
    # declared final, and the run would be scored over a partial offer.
    offer_ended = _offer_ended(state.args)
    state.records = publish_log.read_all(state.args.publish_logs_uri)
    # Assigned only once the records it describes are in hand, so a failed read
    # cannot leave a finished offer paired with the previous poll's records.
    state.offer_ended = offer_ended
    table = load_table(state.args.catalog_props, state.args.table)
    if state.schema_mismatches is None:
        state.schema_mismatches = check_table_schema(table.schema(), state.corpus)
    if state.schema_mismatches:
        # Nothing is tallied from a table of the wrong shape. The rows it does
        # hold would produce figures about a different table than the corpus
        # describes, and publishing them is what the void exists to prevent.
        return False
    document = read_metadata(table)
    seen_new = False
    for info in snapshots_in_order(document):
        if info.snapshot_id in state.seen:
            continue
        first_seen_ms = clock.now_ms()
        files = added_files(document, info.snapshot_id, table.io)
        if info.operation == APPEND:
            for data_file in files:
                if data_file.path in state.applied_files:
                    continue
                state.tally.add_ids(read_id_column(data_file.path, data_file.file_format))
                state.applied_files.add(data_file.path)
            # Appended before the artifact line, so a failed write is retried
            # against a duplicate observation rather than a duplicate line: the
            # repeated observation is the same step of the same function and
            # changes no figure drawn from it.
            state.observations.append(freshness.Observation(info.timestamp_ms, first_seen_ms, state.tally.prefix()))
        _append_line(
            state.args.out_dir / SNAPSHOTS_FILE,
            {
                "snapshot_id": info.snapshot_id,
                "parent_id": info.parent_id,
                "operation": info.operation,
                "timestamp_ms": info.timestamp_ms,
                "first_seen_ms": first_seen_ms,
                "added_files": len(files),
                "added_rows": sum(data_file.record_count for data_file in files),
                "prefix_after": state.tally.prefix(),
            },
        )
        state.seen.add(info.snapshot_id)
        state.applied_files.clear()
        seen_new = True
    return seen_new


def _poll_once(state: ScoreState, clock: Clock, log: TextIO) -> bool:
    """One poll, returning whether it saw a commit it had not seen before.

    A poll that could not read its inputs samples nothing and rewrites no
    summary: it has no new information, and a keep-up sample invented from the
    last one would hide the gap from the gate, which reads an empty window as a
    reader that stopped rather than as an empty backlog.
    """
    try:
        seen_new = _read_inputs(state, clock)
    except Exception as error:
        state.read_failures += 1
        print(
            f"POLL_FAILED consecutive={state.read_failures} error={type(error).__name__}: {error}",
            file=log,
            flush=True,
        )
        if state.read_failures >= MAX_CONSECUTIVE_READ_FAILURES:
            raise
        return False
    state.read_failures = 0
    at_ms = clock.now_ms()
    sample = make_sample(
        at_ms,
        state.offered_rows(),
        state.tally.committed_rows(),
        state.samples[-1] if state.samples else None,
    )
    state.samples.append(sample)
    _append_line(state.args.out_dir / KEEPUP_SAMPLES_FILE, cast(dict[str, object], asdict(sample)))
    _write_summary(state)
    # Only these two, and every poll: they are what `gate` judges a run in
    # flight on, and the rest of the artifacts exist only once it has ended.
    _mirror(state.args, (KEEPUP_SAMPLES_FILE, SUMMARY_FILE))
    print(
        f"POLL t={(at_ms - state.args.epoch_ms) / 1000:.1f} prefix={state.tally.prefix()}/{state.last_batch()} "
        f"committed={sample.committed_rows} offered={sample.offered_rows} backlog={sample.backlog_rows} "
        f"snapshots={len(state.seen)}",
        file=log,
        flush=True,
    )
    return seen_new


def _finalize(state: ScoreState, ending: str, clock: Clock, log: TextIO) -> int:
    """Score the run from what the loop accumulated, and publish every artifact.

    Both endings are scored the same way. A run that stopped while it was still
    behind is not withheld from judgement: its freshness quantiles over what it
    did commit, and the rows it never received, are the measurement — the run
    is invalid, and the artifacts say why rather than being absent.
    """
    emit_ms = state.emit_ms()
    end_ms = clock.now_ms()
    result = freshness.freshness_result(
        state.observations,
        emit_ms,
        epoch_ms=state.args.epoch_ms,
        end_ms=end_ms,
        last_batch=state.last_batch(),
        warmup_s=state.args.warmup_s,
        bound_s=state.args.freshness_bound_s,
        grid_ms=GRID_MS,
    )
    state.result = result
    # Only the batches the publish logs say were sent are judged: a replay over
    # a prefix of the corpus never offered the rest, and scoring them would
    # report rows nobody sent as rows the engine lost.
    state.exactness = exactness_result(state.tally, offered_batches={record.batch for record in state.records})
    if ending == VOID:
        # A void outranks a bound producer: the table is not the one the corpus
        # describes, so no figure taken from either side describes anything.
        state.state = VOID
        state.reason = f"{SCHEMA_MISMATCH_REASON}: {'; '.join(state.schema_mismatches or [])}"
    else:
        # A bound producer names the fault whichever way the run ended: the
        # offer, not the engine, is what the figures describe.
        state.state = PRODUCER_BOUND if state.producer_bound() else ending
        if ending == IDLE_STOP:
            state.reason = IDLE_STOP_REASON
            # No verdict was reached, so the gate has nothing to judge the fleet on.
            state.aborted = True
    document: dict[str, object] = dict(asdict(result))
    # Both time bases are published so another bound can be evaluated offline
    # from one run's artifacts: the table's own commit timestamps, and the wall
    # time at which the scorer first saw each commit.
    document["lag_series"] = {
        name: freshness.lag_series(state.observations, emit_ms, state.args.epoch_ms, end_ms, GRID_MS, name)
        for name in (freshness.TIMESTAMP_CLOCK, freshness.FIRST_SEEN_CLOCK)
    }
    _write_json(state.args.out_dir / FRESHNESS_FILE, document)
    _write_json(state.args.out_dir / EXACTNESS_FILE, state.exactness)
    _write_json(state.args.out_dir / KEEPUP_FILE, _keepup(state))
    _write_summary(state)
    print(f"SCORER_DONE run_valid={state.run_valid()} state={state.state}", file=log, flush=True)
    return 0 if ending == DRAINED else 2


def run(args: ScoreArgs, clock: Clock, log: TextIO) -> int:
    """Score one run, returning 0 once it drained and 2 if it did not.

    The exit code says whether the loop reached a verdict, not what the verdict
    was: a drained run that lost rows still returns 0, with `run_valid` false
    in `summary.json`. Two endings return 2 because no verdict was reachable —
    an idle stop, where the table stopped receiving commits with rows still
    outstanding, and a void, where the table does not hold the columns the
    corpus published. Neither leaves anything worth paying for the fleet for.
    """
    state = _load_inputs(args, clock, log)
    try:
        while True:
            if _poll_once(state, clock, log):
                state.last_new_ms = clock.now_ms()
            if state.schema_mismatches:
                return _finalize(state, VOID, clock, log)
            if state.offer_ended and state.tally.prefix() == state.last_batch():
                return _finalize(state, DRAINED, clock, log)
            if clock.now_ms() - state.last_new_ms >= args.idle_stop_s * 1000:
                return _finalize(state, IDLE_STOP, clock, log)
            clock.sleep(args.poll_interval_s)
    except Exception as error:
        # The last summary on disk has to say the reader is gone: a gate that
        # read a stale running summary would report a dead run as passing.
        state.aborted = True
        state.reason = f"scorer_failed: {type(error).__name__}"
        _write_summary(state)
        print(f"SCORER_FAILED error={type(error).__name__}: {error}", file=log, flush=True)
        raise
    finally:
        # Both endings, and a failure that is about to be re-raised: a run whose
        # artifacts never left the pod is a run nobody can read. A failure here
        # is left to propagate — an upload that did not happen is the one thing
        # a caller of this must not be told went fine.
        _mirror_everything(args)
