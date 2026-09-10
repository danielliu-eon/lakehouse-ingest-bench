# SPDX-License-Identifier: Apache-2.0
"""The publication rules `results/` is held to, checked in one
pass so a contributor and CI run the identical check `scripts/validate-results.py`
merely invokes.

`collect` already redacts a run directory into a publishable document; this
does not trust that it did. A document that leaked a bucket name, or that was
collected from a corpus nobody shipped, is a mistake in `collect` or in the
spec that produced it — the kind of mistake this has to catch independently of
the code that could have made it, not by re-deriving what `collect` derived
but by re-checking the same public-repo promises from the outside.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from ingest_bench.collect.run_json import SCHEMA_VERSION
from ingest_bench.collect.table import render_results_table
from ingest_bench.corpus.cli import workloads_dir
from ingest_bench.corpus.preset import corpus_hash, load_preset
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED

RESULTS_MD = "RESULTS.md"

# Twelve digits with no digit on either side: an AWS account id, and not the
# millisecond epoch timestamps and byte counts a run.json is full of, which
# run thirteen digits and up.
_TWELVE_DIGIT = re.compile(r"(?<!\d)\d{12}(?!\d)")

# A path `collect`'s redaction left alone because it sat under none of the
# site's roots. That is documented behaviour for a path collect never saw
# reason to touch, and exactly the leak this check exists to catch before the
# path reaches a public repository.
_CLOUD_URI = re.compile(r'(?:s3|gs)://[^\s"]+')


def _uri_failures(path: Path, text: str) -> list[str]:
    return [
        f"{path}: uri: an unredacted URI ({match.group()!r}) was not rewritten to a placeholder root"
        for match in _CLOUD_URI.finditer(text)
    ]


def _account_id_failures(path: Path, value: object) -> list[str]:
    """Every 12-digit number inside a JSON *string* of the parsed document —
    a key or a value, never a number.

    An account id can only leak inside a string: an ARN, a bucket name, a
    path. A byte total or a millisecond timestamp is a JSON number that can
    just as easily land on twelve digits by coincidence — an hour at
    100 MB/s is close to 3.6·10^11 bytes — and scanning the raw file text
    cannot tell the two apart. Walking the parsed value can.
    """
    if isinstance(value, str):
        return [
            f"{path}: account_id: a 12-digit number ({match.group()}) appears in a string"
            for match in _TWELVE_DIGIT.finditer(value)
        ]
    if isinstance(value, dict):
        failures = [
            f"{path}: account_id: a 12-digit number ({match.group()}) appears in a key"
            for key in value
            for match in _TWELVE_DIGIT.finditer(key)
        ]
        for inner in value.values():
            failures.extend(_account_id_failures(path, inner))
        return failures
    if isinstance(value, list):
        return [failure for inner in value for failure in _account_id_failures(path, inner)]
    return []


def _fleet_failures(fleet: list[dict[str, object]]) -> list[str]:
    if not fleet:
        return ["fleet: run.fleet is empty"]
    failures = []
    for role in fleet:
        name = role["role"]
        # The sentinel as well as the empty string: an engine whose knobs named
        # no machine type reports a word, and a rule that read only falsiness
        # would wave that through while failing the engine that reported "".
        if not role["machine_type"] or role["machine_type"] == MACHINE_TYPE_UNSPECIFIED:
            failures.append(f"fleet: role {name!r} has no machine_type")
        for field in ("vcpu", "gib"):
            value = role[field]
            if not (isinstance(value, int | float) and value > 0):
                failures.append(f"fleet: role {name!r} has a non-positive {field} ({value!r})")
    return failures


def _corpus_failures(run: dict[str, object], shipped_presets: set[str], workloads: Path) -> list[str]:
    spec = cast(dict[str, object], run["spec"])
    name = str(spec["corpus"])
    if name not in shipped_presets:
        return [f"corpus: {name!r} is not a shipped preset (workloads/presets/*.yaml)"]
    corpus_hash_value = run["corpus_hash"]
    if corpus_hash_value is None:
        return ["corpus_hash: run.corpus_hash is null — the result was collected before it was scored"]
    expected = corpus_hash(load_preset(name, workloads_dir=workloads))
    if corpus_hash_value != expected:
        return [
            f"corpus_hash: {corpus_hash_value!r} does not match the shipped preset's {expected!r} "
            "— the corpus was generated from an overridden preset"
        ]
    return []


def _document_failures(document: dict[str, object], shipped_presets: set[str], workloads: Path) -> list[str]:
    """Every rule beyond `schema_version` that a document must satisfy."""
    run = cast(dict[str, object], document["run"])
    spec = cast(dict[str, object], run["spec"])
    failures: list[str] = []

    producer = cast(dict[str, object], spec["producer"]) if "producer" in spec else {}
    if "seconds" in producer:
        failures.append(
            f"producer.seconds: spec.producer.seconds is set ({producer['seconds']!r}) — a shortened offer "
            "is not publishable"
        )

    failures.extend(_corpus_failures(run, shipped_presets, workloads))

    derived = cast(dict[str, object], document["derived"])
    if derived["keepup"] is None:
        failures.append("derived.keepup: is null")
    if cast(dict[str, object], derived["producer"])["producer_bound"] is None:
        failures.append("derived.producer.producer_bound: is null")

    data = cast(dict[str, object], document["data"])
    if data["summary"] is None:
        failures.append("data.summary: is null — a published result must include the scorer's summary")

    failures.extend(_fleet_failures(cast(list[dict[str, object]], run["fleet"])))

    site_pricing = run["site_pricing"]
    if not isinstance(site_pricing, dict) or "vcpu_hour_usd" not in site_pricing or "gib_hour_usd" not in site_pricing:
        failures.append("site_pricing: run.site_pricing is missing or incomplete")

    return failures


def validate(results_dir: Path, *, workloads: Path) -> list[str]:
    """Every failure found under `results_dir`, as `<path>: <rule>: <detail>` lines.

    An empty `results/` — nothing but `RESULTS.md` and `README.md` — has no
    JSON to check and a two-line table to compare, so it is valid by having
    nothing to fail.
    """
    shipped_presets = {preset_path.stem for preset_path in (workloads / "presets").glob("*.yaml")}
    failures: list[str] = []
    documents: list[tuple[Path, dict[str, object]]] = []
    all_schema_version_2 = True

    for path in sorted(results_dir.glob("**/*.json")):
        text = path.read_text(encoding="utf-8")
        failures.extend(_uri_failures(path, text))
        try:
            document = cast(dict[str, object], json.loads(text))
        except json.JSONDecodeError as err:
            failures.append(f"{path}: json: {err}")
            all_schema_version_2 = False
            continue
        failures.extend(_account_id_failures(path, document))
        schema_version = document["schema_version"] if "schema_version" in document else None
        if schema_version != SCHEMA_VERSION:
            failures.append(f"{path}: schema_version: must be {SCHEMA_VERSION}, got {schema_version!r}")
            all_schema_version_2 = False
            continue
        documents.append((path, document))
        failures.extend(f"{path}: {failure}" for failure in _document_failures(document, shipped_presets, workloads))

    # A directory holding a document that failed even to parse as schema
    # version 2 cannot be rendered at all, so the freshness check below would
    # compare against a table `results-table` could never actually produce —
    # that failure is reported above instead.
    if all_schema_version_2:
        results_md_path = results_dir / RESULTS_MD
        rendered = render_results_table(documents)
        current = results_md_path.read_text(encoding="utf-8") if results_md_path.exists() else None
        if current != rendered:
            failures.append(
                f"{results_md_path}: results_md: does not match a fresh render of {results_dir} — "
                "run results-table again"
            )

    return failures


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="validate-results",
        description="Check every published result under results/ against the public-repo publication rules.",
    )
    parser.add_argument(
        "results_dir",
        nargs="?",
        default="results",
        metavar="DIR",
        help="the results directory to check (default: results/)",
    )
    parser.add_argument(
        "--workloads",
        metavar="DIR",
        help="where the shipped presets live (default: the workloads/ directory shipped beside the package)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    results_dir = Path(str(args.results_dir))
    workloads = workloads_dir(None if args.workloads is None else str(args.workloads))
    failures = validate(results_dir, workloads=workloads)
    for line in failures:
        print(line)
    if failures:
        print(f"{len(failures)} failure(s)", file=sys.stderr)
        return 1
    print(f"OK: {results_dir} holds only publishable results")
    return 0
