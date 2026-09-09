"""Command lines for scoring a run and for judging one while it goes.

`score` is the measurement and `gate` is the decision drawn from it, and they
are separate commands because they run on different rhythms: one process scores
a run from its first commit to its last, while a driver asks the gate every
minute or so whether the run is still worth paying for. The gate therefore
reads the scorer's artifacts rather than the table — the scorer has already
paid for that read, and two readers of one table would disagree about when a
commit became visible.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from ingest_bench.catalog import load_catalog_props, table_identifier
from ingest_bench.clock import SystemClock
from ingest_bench.scorer import score as score_loop
from ingest_bench.scorer.gate import PASS, UNDERSIZED, VOID, gate_verdict
from ingest_bench.table.cli import add_catalog_arguments

# A verdict is an exit code so a shell driver can branch on it without parsing
# output. They are distinct and non-adjacent to keep an undersized fleet from
# being read as a scorer that failed.
EXIT_CODES = {PASS: 0, UNDERSIZED: 3, VOID: 5}

DEFAULT_ADAPTATION_S = 120
DEFAULT_FLOOR_WINDOW_S = 60


def build_score_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="score",
        description="Score a run as it goes: freshness, exactness and keep-up from one pass over the commits.",
    )
    add_catalog_arguments(parser)
    parser.add_argument(
        "--corpus",
        required=True,
        metavar="URI",
        help="the corpus the table is being fed from; its manifest is the ground truth the table is scored against",
    )
    parser.add_argument(
        "--publish-logs",
        required=True,
        metavar="URI",
        help="the prefix the producer shards upload their publish logs under",
    )
    parser.add_argument(
        "--epoch",
        required=True,
        type=float,
        metavar="UNIX_SECONDS",
        help="the run's time origin, the same one every producer shard was given",
    )
    parser.add_argument("--out", required=True, metavar="DIR", help="the directory the artifacts are written to")
    parser.add_argument(
        "--upload-prefix",
        metavar="URI",
        help="copy the artifacts under this prefix as they are written, for a reader that does not share this "
        "process's filesystem",
    )
    parser.add_argument("--poll-interval-s", type=float, default=5.0, help="how often the table is read")
    parser.add_argument(
        "--idle-stop-s",
        type=float,
        default=300.0,
        help="give up after this long with no new commit and rows still outstanding",
    )
    parser.add_argument(
        "--warmup-s",
        type=int,
        default=120,
        help="exclude this long after the epoch from the freshness window; a fleet meeting its first rows is "
        "provisioning rather than lagging, and the bound is a claim about steady state",
    )
    parser.add_argument("--freshness-bound-s", type=float, default=180.0, help="the p95 lag the run is judged against")
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="the replay speed the producers were given, recorded with the score",
    )
    parser.add_argument(
        "--publish-shards",
        type=int,
        default=1,
        help="how many producer shards the offer is split across; the offer is over once every one of them has "
        "marked its log done",
    )
    parser.add_argument(
        "--behind-max-ms",
        type=int,
        default=5000,
        help="a producer that fell this far behind its schedule voids the run",
    )
    return parser


def build_gate_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gate",
        description="Judge a run in flight from the scorer's artifacts: PASS, UNDERSIZED or VOID.",
    )
    parser.add_argument("--out", required=True, metavar="DIR", help="the scorer's artifact directory")
    parser.add_argument(
        "--adaptation-s",
        type=int,
        default=DEFAULT_ADAPTATION_S,
        help="nothing is judged undersized until this long after the epoch",
    )
    parser.add_argument(
        "--window-s",
        type=int,
        default=DEFAULT_FLOOR_WINDOW_S,
        help="the width of each backlog-floor window; the floor rising over three of them is a fleet falling behind",
    )
    return parser


def score(argv: Sequence[str] | None = None) -> int:
    parser = build_score_parser()
    args = parser.parse_args(argv)
    table = str(args.table)
    try:
        # A malformed --table is an argument error rather than a catalog one, so
        # it is resolved here instead of surfacing hours into a run.
        table_identifier(table)
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    return score_loop.run(
        score_loop.ScoreArgs(
            corpus_uri=str(args.corpus),
            table=table,
            catalog_props=props,
            publish_logs_uri=str(args.publish_logs),
            epoch_ms=round(float(args.epoch) * 1000),
            out_dir=Path(str(args.out)),
            poll_interval_s=float(args.poll_interval_s),
            idle_stop_s=float(args.idle_stop_s),
            warmup_s=int(args.warmup_s),
            freshness_bound_s=float(args.freshness_bound_s),
            speed=float(args.speed),
            behind_max_ms=int(args.behind_max_ms),
            expected_publish_shards=int(args.publish_shards),
            upload_prefix=None if args.upload_prefix is None else str(args.upload_prefix),
        ),
        SystemClock(),
        sys.stdout,
    )


def _report(verdict: str, reason: str) -> int:
    print(f"{verdict} {reason}")
    return EXIT_CODES[verdict]


def gate(argv: Sequence[str] | None = None) -> int:
    args = build_gate_parser().parse_args(argv)
    out_dir = Path(str(args.out))
    summary_path = out_dir / score_loop.SUMMARY_FILE
    # A run whose scorer has published nothing is void rather than a crash
    # here: the gate is polled in a loop, and the absence of a measurement is
    # one of the answers it exists to give.
    if not summary_path.exists():
        return _report(VOID, f"{summary_path} does not exist, so the run has no measurement to judge")
    summary = cast(dict[str, object], json.loads(summary_path.read_text(encoding="utf-8")))
    # A run the loop itself voided is void here too, with the reason it gave.
    # The verdict function judges lag and backlog, and both are figures about a
    # table this run has been found not to have.
    if summary["state"] == score_loop.VOID:
        return _report(VOID, str(summary["reason"]))
    producer = cast(dict[str, object], summary["producer"])
    # A bound producer is a fact about the offer, so it cannot be reported
    # through the reader-aborted reason the verdict function would give it.
    if summary["producer_bound"]:
        return _report(
            VOID,
            f"the producer fell {producer['behind_ms']} ms behind its schedule with {producer['errors']} delivery "
            "errors, so the offer rather than the engine set the rate",
        )
    verdict, reason = gate_verdict(
        {"aborted": summary["aborted"], "lag_s": summary["lag_s"]},
        score_loop.read_keepup_samples(out_dir / score_loop.KEEPUP_SAMPLES_FILE),
        bound_s=float(cast(float, summary["freshness_bound_s"])),
        adaptation_s=int(args.adaptation_s),
        window_s=int(args.window_s),
        now_ms=SystemClock().now_ms(),
        epoch_ms=int(cast(int, summary["epoch_ms"])),
    )
    return _report(verdict, reason)
