"""The commands that turn a finished run into a publishable result, and that
turn a directory of results into the table a reader compares them on.

`collect` is run once by teardown, so a run has a document even if nothing else
is ever done with it, and again by finish once the geometry has been measured.
Both write the same document; the second one is simply the one with every
input present. `results-table` is run over `results/` itself, by a contributor
publishing a result and by CI checking that nobody forgot to.
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

# The variant a run is published under when nothing else is said. Every shipped
# spec distributes writes by hash, so it is the shape a reader comparing engines
# is looking at unless a result says otherwise.
DEFAULT_VARIANT = "hash"

_RESULTS_DATE_FORMAT = "%Y-%m-%d"


def results_name(engine: str, corpus: str, variant: str, collected_at: datetime) -> str:
    """The file name a published result takes under ``results/<engine>/``.

    Date, engine, corpus and variant, because those four are what distinguish
    two results anyone may publish: the same engine on the same corpus tuned
    differently is a separate variant rather than a second version of one file.
    """
    date = collected_at.astimezone(UTC).strftime(_RESULTS_DATE_FORMAT)
    return f"{date}-{engine}-{corpus}-{variant}.json"


def _out_path(out: str | None, run_dir: Path, *, name: str) -> Path:
    """Where the document is written.

    A directory is filled with the results name and a file path is taken as
    given, so publishing is `--out results/<engine>` and re-collecting in place
    is no `--out` at all. A path that does not exist yet counts as a directory
    only if it says so with a trailing separator: guessing would turn a
    misspelled file name into a directory nobody meant to create.
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
        description="Assemble a run's publishable result document, with the operator's site redacted out of it.",
    )
    parser.add_argument("--run-dir", required=True, metavar="DIR", help="the run directory to collect")
    parser.add_argument(
        "--site",
        required=True,
        metavar="PATH",
        help="the site config the run was staged with; its roots are what the embedded paths are redacted against",
    )
    parser.add_argument(
        "--out",
        metavar="PATH",
        help="where to write the document (default: <run dir>/run.json). A directory is filled with "
        "<date>-<engine>-<corpus>-<variant>.json, which is the name a published result takes",
    )
    parser.add_argument(
        "--variant",
        default=DEFAULT_VARIANT,
        metavar="NAME",
        help=f"the tuning this run stands for, recorded in the document and in its published name "
        f"(default: {DEFAULT_VARIANT})",
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
    # Named rather than counted, because which input is absent decides whether
    # the document is publishable: a run with no geometry is still a result,
    # while one with no scores is not.
    for name in missing:
        print(f"  missing: {name}")
    return 0


# ---------------------------------------------------------------------------
# results-table
# ---------------------------------------------------------------------------


def load_results(results_dir: Path) -> list[tuple[Path, dict[str, object]]]:
    """Every `run.json` under `results_dir`, refusing anything that is not one.

    A directory nobody has published to yet has no JSON files at all, so an
    empty list is a normal answer, not an error — `results/` starts and ends a
    dry spell with its table still valid.
    """
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
