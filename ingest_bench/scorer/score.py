# SPDX-License-Identifier: Apache-2.0
"""Score a live run by processing each table commit once.

Accumulate the tally, observations, and keep-up samples for both live and
final metrics. Reuse them at drain instead of rescanning the table. Write
artifacts incrementally for the gate and to preserve partial-run evidence.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TextIO, cast

from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.table import Table

from ingest_bench import uri
from ingest_bench.clock import Clock
from ingest_bench.corpus import metadata
from ingest_bench.producer import publish_log
from ingest_bench.scorer import freshness
from ingest_bench.scorer.exactness import exactness_result
from ingest_bench.scorer.keepup import KeepupSample, keepup_summary, make_sample
from ingest_bench.scorer.snapshots import (
    AddedFile,
    added_files,
    check_table_schema,
    load_table,
    read_metadata,
    snapshots_in_order,
)
from ingest_bench.scorer.tally import BatchTally, read_id_column
from ingest_bench.specs.model import ENGINE_OWNED, HARNESS

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

# Retry transient input failures with a bound. Persistent failures must
# abort scoring instead of resembling an idle engine.
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
    # Overlap ID-column reads to hide request latency. Limit concurrent arrays
    # to this width and release each after tallying.
    read_workers: int = 32
    # An engine-owned table may not exist until the first record arrives.
    table_managed_by: str = HARNESS


@dataclass
class ScoreState:
    """Accumulated tally, observations, and status shared between polls.

    Track seen snapshots and applied files to make retries safe. Persist artifacts
    incrementally because expired snapshots may prevent later reconstruction.
    """

    args: ScoreArgs
    corpus: metadata.CorpusMetadata
    tally: BatchTally
    last_new_ms: int
    observations: list[freshness.Observation] = field(default_factory=list)
    samples: list[KeepupSample] = field(default_factory=list)
    seen: set[int] = field(default_factory=set)
    # Track applied files so retrying a partial commit cannot count them twice.
    applied_files: set[str] = field(default_factory=set)
    records: list[publish_log.PublishRecord] = field(default_factory=list)
    # Defer schema validation until the first successful table load.
    schema_mismatches: list[str] | None = None
    # Track table absence to avoid repeating the same log message every poll.
    table_absent: bool = False
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
        """Return the final corpus batch, or final published batch after offer completion."""
        if self.offer_ended:
            return max((record.batch for record in self.records), default=-1)
        return self.corpus.batch_count - 1

    def producer_bound(self) -> bool:
        """Return whether producer lag or delivery failures invalidate the offer."""
        return publish_log.behind_ms(self.records) > self.args.behind_max_ms or any(
            record.errors for record in self.records
        )

    def run_valid(self) -> bool:
        """Require successful freshness and exactness results without aborts,
        schema mismatches, or producer bottlenecks.
        """
        return (
            not self.aborted
            and not self.schema_mismatches
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
    """Replace an artifact atomically so live readers cannot see partial JSON."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _mirror(args: ScoreArgs, names: Iterable[str]) -> None:
    """Upload named local artifacts when an upload prefix is configured."""
    if args.upload_prefix is None:
        return
    for name in names:
        uri.write_bytes(uri.join(args.upload_prefix, name), (args.out_dir / name).read_bytes())


def _mirror_everything(args: ScoreArgs) -> None:
    """Upload all available artifacts, including after scorer failure."""
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
    """Measure live lag from the latest sample and covered batch emit time.

    Use the run epoch until the first batch is covered, matching ``lag_series``.
    """
    if not state.samples:
        return None
    prefix = state.tally.prefix()
    # An unuploaded publish record makes lag unknown until a later poll.
    reference = state.args.epoch_ms if prefix < 0 else state.emit_ms().get(prefix)
    return None if reference is None else (state.samples[-1].at_ms - reference) / 1000


def _producer_reason(state: ScoreState) -> str:
    """Which half of `producer_bound` was true, with the figure behind it."""
    faults: list[str] = []
    behind_ms = publish_log.behind_ms(state.records)
    if behind_ms > state.args.behind_max_ms:
        faults.append(
            f"a batch was acknowledged {behind_ms} ms after it was due, over behind_max_ms {state.args.behind_max_ms}"
        )
    errors = sum(record.errors for record in state.records)
    if errors:
        faults.append(f"{errors} deliveries errored")
    return f"{PRODUCER_BOUND}: " + "; ".join(faults)


def _exactness_reason(exactness: dict[str, object]) -> str:
    """Summarize nonzero loss, duplication, and corruption counts."""
    faults = ("loss_rows", "duplicate_rows", "corrupt_batches")
    return "exactness: " + ", ".join(f"{name}={exactness[name]}" for name in faults if exactness[name])


def _freshness_reason(result: freshness.FreshnessResult) -> str:
    """Describe the first failed freshness condition."""
    if not result.drained:
        return "freshness: the table never drained"
    if result.missing_emit_prefixes:
        return (
            f"freshness: missing_emit_prefixes={len(result.missing_emit_prefixes)}, "
            f"first={result.missing_emit_prefixes[0]}"
        )
    p95_s, max_s = result.window["p95_s"], result.window["max_s"]
    if p95_s is not None and p95_s > result.bound_s:
        return f"freshness: window p95 {p95_s:.2f} s exceeds bound {result.bound_s:.2f} s"
    if max_s is not None and max_s > result.max_bound_s:
        return f"freshness: window max {max_s:.2f} s exceeds max bound {result.max_bound_s:.2f} s"
    return "freshness: the measurement window holds no lag sample to judge the bound on"


def _invalid_reason(state: ScoreState, result: freshness.FreshnessResult, exactness: dict[str, object]) -> str:
    """Choose the primary metric failure: producer, exactness, then freshness.

    Abort and schema failures are handled before this function.
    """
    if state.producer_bound():
        return _producer_reason(state)
    if not exactness["exact"]:
        return _exactness_reason(exactness)
    return _freshness_reason(result)


def _write_summary(state: ScoreState) -> None:
    """Write the shared live and final summary format."""
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
    # Start fresh artifacts so another scorer cannot append unrelated history.
    for name in (SNAPSHOTS_FILE, KEEPUP_SAMPLES_FILE):
        (args.out_dir / name).write_text("", encoding="utf-8")
    print(
        f"SCORER_START table={args.table} batches={corpus.batch_count} rows={corpus.row_count} "
        f"epoch_ms={args.epoch_ms}",
        file=log,
        flush=True,
    )
    return ScoreState(args=args, corpus=corpus, tally=tally, last_new_ms=clock.now_ms())


def _load_table(state: ScoreState, log: TextIO) -> Table | None:
    """Load the table, tolerating absence only for engine-owned tables.

    Such engines may create the namespace and table after the first record.
    Treat absence as an empty baseline so launch can start the producer. Missing
    harness-owned tables remain errors.
    """
    try:
        table = load_table(state.args.catalog_props, state.args.table)
    except (NoSuchTableError, NoSuchNamespaceError):
        if state.args.table_managed_by != ENGINE_OWNED:
            raise
        if not state.table_absent:
            state.table_absent = True
            print(
                f"TABLE_ABSENT table={state.args.table} managed_by={ENGINE_OWNED}: reading it as empty until "
                "the engine creates it",
                file=log,
                flush=True,
            )
        return None
    if state.table_absent:
        state.table_absent = False
        print(f"TABLE_PRESENT table={state.args.table}", file=log, flush=True)
    return table


@dataclass(frozen=True)
class PollRead:
    """Whether a poll saw new snapshots and how many data files it read."""

    seen_new: bool
    files: int


def _apply_added_files(state: ScoreState, files: list[AddedFile]) -> int:
    """Read new ID columns concurrently and apply them to the tally serially.

    Counts and modular sums are order-independent. Track files only after their
    IDs are tallied so failed polls retry only unapplied files.
    """
    pending = [file for file in files if file.path not in state.applied_files]
    if not pending:
        return 0
    pool = ThreadPoolExecutor(max_workers=min(state.args.read_workers, len(pending)))
    try:
        reads = {pool.submit(read_id_column, file.path, file.file_format): file.path for file in pending}
        for read in as_completed(reads):
            state.tally.add_ids(read.result())
            # Remove consumed futures to release their arrays. `as_completed` retains
            # its own snapshot of the pending futures.
            state.applied_files.add(reads.pop(read))
    finally:
        # Cancel queued work on failure and finish running reads before retrying.
        pool.shutdown(wait=True, cancel_futures=True)
    return len(pending)


def _read_inputs(state: ScoreState, clock: Clock, log: TextIO) -> PollRead:
    """Read producer logs and unseen table commits.

    Tally only append snapshots to avoid counting compaction rewrites as
    duplicates. Record every operation in ``snapshots.jsonl``.
    """
    # Read completion before records: a visible trailer guarantees the following
    # record read includes the final batch. The opposite order could mark a
    # partial record list complete.
    offer_ended = _offer_ended(state.args)
    state.records = publish_log.read_all(state.args.publish_logs_uri)
    # Update completion only after the matching records have been read.
    state.offer_ended = offer_ended
    table = _load_table(state, log)
    if table is None:
        # The empty-table sample provides the baseline launch waits for.
        return PollRead(seen_new=False, files=0)
    if state.schema_mismatches is None:
        state.schema_mismatches = check_table_schema(table.schema(), state.corpus)
    if state.schema_mismatches:
        # Do not publish measurements for a table that violates the corpus schema.
        return PollRead(seen_new=False, files=0)
    document = read_metadata(table)
    seen_new = False
    read_files = 0
    for info in snapshots_in_order(document):
        if info.snapshot_id in state.seen:
            continue
        first_seen_ms = clock.now_ms()
        files = added_files(document, info.snapshot_id, table.io)
        if info.operation == APPEND:
            read_files += _apply_added_files(state, files)
            # Record the observation first: retrying a failed artifact write may repeat
            # an idempotent observation, but must not duplicate an artifact line.
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
                "added_bytes": sum(data_file.size_bytes for data_file in files),
                "prefix_after": state.tally.prefix(),
            },
        )
        state.seen.add(info.snapshot_id)
        state.applied_files.clear()
        seen_new = True
    return PollRead(seen_new=seen_new, files=read_files)


def _poll_once(state: ScoreState, clock: Clock, log: TextIO) -> bool:
    """Poll inputs and return whether new snapshots were observed.

    On read failure, leave samples and summary unchanged so the gate can detect
    staleness. Log read duration and file count to diagnose slow polls.
    """
    started_ms = clock.now_ms()
    try:
        read = _read_inputs(state, clock, log)
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
    poll_ms = at_ms - started_ms
    interval_ms = round(state.args.poll_interval_s * 1000)
    sample = make_sample(
        at_ms,
        state.offered_rows(),
        state.tally.committed_rows(),
        state.samples[-1] if state.samples else None,
    )
    state.samples.append(sample)
    _append_line(state.args.out_dir / KEEPUP_SAMPLES_FILE, cast(dict[str, object], asdict(sample)))
    _write_summary(state)
    # Refresh the live gate inputs on every successful poll.
    _mirror(state.args, (KEEPUP_SAMPLES_FILE, SUMMARY_FILE))
    if poll_ms > interval_ms:
        print(
            f"SLOW_POLL poll_ms={poll_ms} files={read.files} interval_ms={interval_ms}",
            file=log,
            flush=True,
        )
    print(
        f"POLL t={(at_ms - state.args.epoch_ms) / 1000:.1f} prefix={state.tally.prefix()}/{state.last_batch()} "
        f"committed={sample.committed_rows} offered={sample.offered_rows} backlog={sample.backlog_rows} "
        f"snapshots={len(state.seen)} poll_ms={poll_ms} files={read.files}",
        file=log,
        flush=True,
    )
    return read.seen_new


def _finalize(state: ScoreState, ending: str, clock: Clock, log: TextIO) -> int:
    """Write final metrics from accumulated state, including for incomplete runs."""
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
    # Exclude unoffered batches from shortened replays.
    exactness = exactness_result(state.tally, offered_batches={record.batch for record in state.records})
    state.exactness = exactness
    if ending == VOID:
        # Schema failure takes precedence over a producer bottleneck.
        state.state = VOID
        state.reason = f"{SCHEMA_MISMATCH_REASON}: {'; '.join(state.schema_mismatches or [])}"
    else:
        # Report producer bottlenecks regardless of how the run ended.
        state.state = PRODUCER_BOUND if state.producer_bound() else ending
        if ending == IDLE_STOP:
            state.reason = IDLE_STOP_REASON
            # No verdict was reached, so the gate has nothing to judge the fleet on.
            state.aborted = True
    # Surface metric failures in the summary as well as detailed artifacts.
    if state.reason is None and not state.run_valid():
        state.reason = _invalid_reason(state, result, exactness)
    document: dict[str, object] = dict(asdict(result))
    # Publish both commit and observation clocks for offline re-evaluation.
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
    """Score until drain, idle stop, or schema failure.

    Return 0 for drain and 2 for idle or schema failure. A drained run can still
    fail metric checks; read ``run_valid`` in the summary for that verdict.
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
        # Persist reader failure so the gate cannot accept the previous running state.
        state.aborted = True
        state.reason = f"scorer_failed: {type(error).__name__}"
        _write_summary(state)
        print(f"SCORER_FAILED error={type(error).__name__}: {error}", file=log, flush=True)
        raise
    finally:
        # Upload artifacts on every exit path. Propagate upload failures.
        _mirror_everything(args)
