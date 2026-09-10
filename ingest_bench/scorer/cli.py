# SPDX-License-Identifier: Apache-2.0
"""Commands for live scoring, early-stop decisions, and file geometry.

The gate reads scorer artifacts to avoid a second table reader. Geometry can
be measured after the run, keeping manifest traversal out of the live loop.
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
from ingest_bench.scorer import geometry
from ingest_bench.scorer import score as score_loop
from ingest_bench.scorer.gate import PASS, UNDERSIZED, VOID, gate_verdict
from ingest_bench.scorer.snapshots import load_table, read_metadata
from ingest_bench.specs.model import DEFAULT_GEOMETRY_OFFSETS_S, ENGINE_OWNED, HARNESS
from ingest_bench.table.cli import add_catalog_arguments

FSSPEC_FILE_IO = "pyiceberg.io.fsspec.FsspecFileIO"

# Distinct verdict exit codes let shell drivers branch without parsing output.
EXIT_CODES = {PASS: 0, UNDERSIZED: 3, VOID: 5}

# Distinguish an empty table from argparse's argument-error exit code.
NO_GEOMETRY = 4

DEFAULT_ADAPTATION_S = 120
DEFAULT_FLOOR_WINDOW_S = 60

# Match the newest backlog window. This allows twelve normal poll intervals
# or twice the configured retry gap before declaring samples stale.
DEFAULT_STALE_AFTER_S = DEFAULT_FLOOR_WINDOW_S


def _read_workers(raw: str) -> int:
    """At least one reader, refused here rather than at the first commit.

    A pool of no threads raises where it is built, which is inside the poll
    loop — so a run would stage its fleet, begin its offer and only then end,
    on its reader's own argument.
    """
    try:
        workers = int(raw)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"--read-workers must be an integer, got {raw!r}") from error
    if workers < 1:
        raise argparse.ArgumentTypeError(
            f"--read-workers is how many data files are read at once, so it must be at least 1, got {workers}"
        )
    return workers


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
    parser.add_argument(
        "--read-workers",
        type=_read_workers,
        default=32,
        help="how many of a commit's data files have their id column read at once; a commit of a "
        "high-cardinality partition is hundreds of small files, and one request at a time is a poll longer "
        "than the interval it is polled on",
    )
    parser.add_argument(
        "--table-managed-by",
        choices=sorted({HARNESS, ENGINE_OWNED}),
        default=HARNESS,
        help=f"who ran the table's DDL, as the run spec's table.managed_by says. Under {ENGINE_OWNED!r} a table "
        "that does not exist yet is read as empty and polling continues, because such an engine creates it from "
        "its first record and this reader starts before the offer does",
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
    parser.add_argument(
        "--stale-after-s",
        type=int,
        default=DEFAULT_STALE_AFTER_S,
        help="a run whose newest keep-up sample is older than this is void: a scorer that was killed rather than "
        "raising leaves a summary that still reads as a healthy run",
    )
    return parser


def score(argv: Sequence[str] | None = None) -> int:
    parser = build_score_parser()
    args = parser.parse_args(argv)
    table = str(args.table)
    try:
        # Validate the identifier before contacting the catalog.
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
            read_workers=int(args.read_workers),
            table_managed_by=str(args.table_managed_by),
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
    # Report missing measurements as VOID so the polling driver can handle them.
    if not summary_path.exists():
        return _report(VOID, f"{summary_path} does not exist, so the run has no measurement to judge")
    summary = cast(dict[str, object], json.loads(summary_path.read_text(encoding="utf-8")))
    # Preserve schema-failure verdicts before judging lag and backlog.
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
        stale_after_s=int(args.stale_after_s),
    )
    return _report(verdict, reason)


def _offsets(raw: str) -> tuple[int, ...]:
    """The geometry ladder, as ``--offsets 600,1200,1800`` gives it."""
    try:
        offsets = tuple(int(part) for part in raw.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"--offsets must be comma-separated seconds, got {raw!r}") from error
    if any(offset < 0 for offset in offsets):
        raise argparse.ArgumentTypeError(f"--offsets are seconds from the epoch, so none may be negative: {raw!r}")
    if any(later <= earlier for earlier, later in zip(offsets, offsets[1:], strict=False)):
        raise argparse.ArgumentTypeError(
            f"--offsets must ascend, since each rung reports the commits since the one before it: {raw!r}"
        )
    return offsets


def build_file_sizes_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="file-sizes",
        description="Report the table's file geometry at points along the run and as the run left it.",
    )
    add_catalog_arguments(parser, table_required=False)
    parser.add_argument(
        "--metadata",
        metavar="URI_OR_PATH",
        help="a metadata document to read the table out of, instead of --table. This is what a teardown copied, "
        "so the geometry of a finished run can be read once its catalog and its cluster are gone. The catalog "
        "properties are still read, for the object-store settings among them",
    )
    parser.add_argument(
        "--epoch",
        required=True,
        type=float,
        metavar="UNIX_SECONDS",
        help="the run's time origin, the same one the scorer was given; the offsets are measured from it",
    )
    parser.add_argument(
        "--offsets",
        type=_offsets,
        default=DEFAULT_GEOMETRY_OFFSETS_S,
        metavar="SECONDS,…",
        help="the points after the epoch the table is measured at. Every run reports the same ladder so two "
        "runs' geometry columns line up",
    )
    parser.add_argument("--out", required=True, metavar="DIR", help="the directory geometry.json is written to")
    return parser


def file_sizes(argv: Sequence[str] | None = None) -> int:
    parser = build_file_sizes_parser()
    args = parser.parse_args(argv)
    metadata_location = None if args.metadata is None else str(args.metadata)
    table = None if args.table is None else str(args.table)
    # Require one metadata source: copied and live documents may differ.
    if (metadata_location is None) == (table is None):
        parser.error("give exactly one of --metadata (a copied document) and --table (through a catalog)")
    try:
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
        if table is not None:
            table_identifier(table)
    except ValueError as error:
        parser.error(str(error))
    # Prefer fsspec for operator-side profile support, including credential_process
    # and SSO. Honor an explicit IO implementation.
    props.setdefault("py-io-impl", FSSPEC_FILE_IO)
    if metadata_location is not None:
        document, io = geometry.open_metadata_document(metadata_location, props)
    else:
        loaded = load_table(props, cast(str, table))
        document, io = read_metadata(loaded), loaded.io
    report = geometry.geometry_report(document, io, round(float(args.epoch) * 1000), tuple(args.offsets))
    out_dir = Path(str(args.out))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / geometry.GEOMETRY_FILE
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    final = cast(dict[str, object] | None, report["final"])
    # The empty document is still published: `collect` records that geometry
    # was measured, and a missing file would read as a step never run.
    if final is None:
        print(f"the table holds no commit, so it has no geometry; wrote {path}", file=sys.stderr)
        return NO_GEOMETRY
    live = cast(dict[str, object], final["live"])
    quantiles = cast(dict[str, float | None], live["size_quantiles"])
    print(
        f"GEOMETRY out={path} files={live['files']} rows={live['rows']} bytes={live['bytes']} "
        f"p50_bytes={quantiles['p50']} small_file_share_32mib={live['small_file_share_32mib']}"
    )
    return 0
