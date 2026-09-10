# SPDX-License-Identifier: Apache-2.0
"""Collect run artifacts and render the published results table.

Teardown collects partial results; finish collects again after geometry is
available. Both commands write the same document format.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import cast

from ingest_bench.collect.run_json import RUN_JSON_FILE, SCHEMA_VERSION, build_run_json
from ingest_bench.collect.table import render_results_table
from ingest_bench.specs.model import load_site

DISTRIBUTION = "lakehouse-ingest-bench"

# Default variant for the shipped specs, which distribute writes by hash.
DEFAULT_VARIANT = "hash"

_RESULTS_DATE_FORMAT = "%Y-%m-%d"


def results_name(engine: str, corpus: str, variant: str, collected_at: datetime) -> str:
    """Build a result filename from date, engine, corpus, and variant."""
    date = collected_at.astimezone(UTC).strftime(_RESULTS_DATE_FORMAT)
    return f"{date}-{engine}-{corpus}-{variant}.json"


def _out_path(out: str | None, run_dir: Path, *, name: str) -> Path:
    """Resolve the output path, filling existing directories with the result name.

    A nonexistent directory must have a trailing separator; other paths are
    used as filenames. Without ``--out``, write in the run directory.
    """
    if out is None:
        return run_dir / RUN_JSON_FILE
    path = Path(out)
    if path.is_dir() or out.endswith("/"):
        return path / name
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="collect",
        description="Assemble a publishable run result with private site details redacted.",
    )
    parser.add_argument("--run-dir", required=True, metavar="DIR", help="the run directory to collect")
    parser.add_argument(
        "--site",
        required=True,
        metavar="PATH",
        help="site configuration used for staging; its storage roots identify paths to redact",
    )
    parser.add_argument(
        "--out",
        metavar="PATH",
        help="output file or directory (default: <run dir>/run.json). For a directory, use the filename "
        "<date>-<engine>-<corpus>-<variant>.json",
    )
    parser.add_argument(
        "--variant",
        default=DEFAULT_VARIANT,
        metavar="NAME",
        help=f"tuning variant recorded in the result and its filename (default: {DEFAULT_VARIANT})",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = Path(str(args.run_dir))
    variant = str(args.variant)
    collected_at = datetime.now(UTC)
    document = build_run_json(
        run_dir,
        load_site(Path(str(args.site))),
        harness_version=version(DISTRIBUTION),
        collected_at=collected_at,
        variant=variant,
    )
    run = cast(dict[str, object], document["run"])
    spec = cast(dict[str, object], run["spec"])
    out_path = _out_path(
        None if args.out is None else str(args.out),
        run_dir,
        name=results_name(str(run["engine"]), str(spec["corpus"]), variant, collected_at),
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    missing = cast(list[str], document["missing"])
    print(f"COLLECTED out={out_path} schema_version={document['schema_version']} missing={len(missing)}")
    # Name missing inputs so publication checks can distinguish missing scores
    # from optional measurements.
    for name in missing:
        print(f"  missing: {name}")
    return 0


# ---------------------------------------------------------------------------
# results-table
# ---------------------------------------------------------------------------


def load_results(results_dir: Path) -> list[tuple[Path, dict[str, object]]]:
    """Load run documents under ``results_dir``; an empty directory yields no rows."""
    documents: list[tuple[Path, dict[str, object]]] = []
    for path in sorted(results_dir.glob("**/*.json")):
        document = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))
        schema_version = document["schema_version"] if "schema_version" in document else None
        if schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"{path}: schema_version {schema_version!r} is not {SCHEMA_VERSION}, so it is not a result document"
            )
        documents.append((path, document))
    return documents


def build_results_table_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="results-table",
        description="Render every published result under a results directory into RESULTS.md.",
    )
    parser.add_argument("results_dir", metavar="DIR", help="the results directory to read, e.g. results/")
    parser.add_argument("--out", required=True, metavar="PATH", help="where to write the rendered table")
    return parser


def results_table_main(argv: Sequence[str] | None = None) -> int:
    args = build_results_table_parser().parse_args(argv)
    results_dir = Path(str(args.results_dir))
    try:
        documents = load_results(results_dir)
        text = render_results_table(documents)
    except ValueError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    out_path = Path(str(args.out))
    out_path.write_text(text, encoding="utf-8")
    print(f"RESULTS_TABLE out={out_path} rows={len(documents)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
