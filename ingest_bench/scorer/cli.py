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
        raise argparse.ArgumentTypeError(f"--read-workers must be at least 1, got {workers}")
    return workers


def build_score_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="score",
        description="Measure freshness, exactness, and keep-up as table commits arrive.",
    )
    add_catalog_arguments(parser)
    parser.add_argument(
        "--corpus",
        required=True,
        metavar="URI",
        help="source corpus whose manifest defines the expected rows",
    )
    parser.add_argument(
        "--publish-logs",
        required=True,
        metavar="URI",
        help="URI prefix containing all producer shards' publish logs",
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
        help="upload artifacts to this URI prefix as they are written",
    )
    parser.add_argument("--poll-interval-s", type=float, default=5.0, help="table polling interval in seconds")
    parser.add_argument(
        "--idle-stop-s",
        type=float,
        default=300.0,
        help="stop after this many seconds without a new commit while rows are still missing",
    )
    parser.add_argument(
        "--warmup-s",
        type=int,
        default=120,
        help="seconds after the epoch to exclude from the freshness window used for the verdict",
    )
    parser.add_argument(
        "--freshness-bound-s", type=float, default=180.0, help="maximum allowed p95 freshness lag in seconds"
    )
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
        help="number of producer shards; publishing ends when all shard logs are marked done",
    )
    parser.add_argument(
        "--behind-max-ms",
        type=int,
        default=5000,
        help="maximum producer schedule delay in milliseconds before the run is void",
    )
    parser.add_argument(
        "--read-workers",
        type=_read_workers,
        default=32,
        help="maximum number of data files whose ID columns are read concurrently",
    )
    parser.add_argument(
        "--table-managed-by",
        choices=sorted({HARNESS, ENGINE_OWNED}),
        default=HARNESS,
        help=f"table creator, matching table.managed_by in the run spec. With {ENGINE_OWNED!r}, treat a "
        "missing table as empty and keep polling until the engine creates it",
    )
    return parser


def build_gate_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gate",
        description="Evaluate an active run from scorer artifacts: PASS, UNDERSIZED, or VOID.",
    )
    parser.add_argument("--out", required=True, metavar="DIR", help="the scorer's artifact directory")
    parser.add_argument(
        "--adaptation-s",
        type=int,
        default=DEFAULT_ADAPTATION_S,
        help="seconds after the epoch before a run can be judged UNDERSIZED",
    )
    parser.add_argument(
        "--window-s",
        type=int,
        default=DEFAULT_FLOOR_WINDOW_S,
        help="backlog-floor window in seconds; a rising floor across three windows indicates falling behind",
    )
    parser.add_argument(
        "--stale-after-s",
        type=int,
        default=DEFAULT_STALE_AFTER_S,
        help="maximum age of the latest keep-up sample in seconds before the run is VOID",
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
        description="Report table file sizes and counts at selected times and at the end of the run.",
    )
    add_catalog_arguments(parser, table_required=False)
    parser.add_argument(
        "--metadata",
        metavar="URI_OR_PATH",
        help="saved table metadata to read instead of --table, allowing analysis after teardown. "
        "Catalog properties still supply object-store settings",
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
        help="comma-separated seconds after the epoch at which to measure the table",
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
        print(f"table has no commits; wrote empty file geometry to {path}", file=sys.stderr)
        return NO_GEOMETRY
    live = cast(dict[str, object], final["live"])
    quantiles = cast(dict[str, float | None], live["size_quantiles"])
    print(
        f"GEOMETRY out={path} files={live['files']} rows={live['rows']} bytes={live['bytes']} "
        f"p50_bytes={quantiles['p50']} small_file_share_32mib={live['small_file_share_32mib']}"
    )
    return 0
