# SPDX-License-Identifier: Apache-2.0
"""Validate publication requirements for every result under ``results/``.

Check for leaked identifiers, unsupported corpora, missing disclosures,
reused resources, and a stale results table independently of collection.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path

from ingest_bench.collect.schema import FleetRole, ResultDocument, Run, parse_result
from ingest_bench.collect.table import render_results_table
from ingest_bench.corpus.cli import workloads_dir
from ingest_bench.corpus.preset import corpus_hash, load_preset
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED, PLACEHOLDER

RESULTS_MD = "RESULTS.md"

# Scan only strings; numeric byte counts and timestamps can have 12 digits.
_TWELVE_DIGIT = re.compile(r"(?<!\d)\d{12}(?!\d)")

# Catch storage URIs outside the configured roots that collection could not redact.
_CLOUD_URI = re.compile(r'(?:s3|gs)://[^\s"]+')


def _uri_failures(path: Path, text: str) -> list[str]:
    return [
        f"{path}: uri: an unredacted URI ({match.group()!r}) was not rewritten to a placeholder root"
        for match in _CLOUD_URI.finditer(text)
    ]


def _account_id_failures(path: Path, value: object) -> list[str]:
    """Find 12-digit identifiers in JSON string values and keys.

    Skip numeric values: byte counts and timestamps can also have 12 digits.
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


def _fleet_failures(fleet: list[FleetRole]) -> list[str]:
    if not fleet:
        return ["fleet: run.fleet is empty"]
    failures = []
    for role in fleet:
        name = role["role"]
        # Reject empty, unspecified, and unreplaced example machine types.
        machine_type = role["machine_type"]
        if not machine_type or machine_type == MACHINE_TYPE_UNSPECIFIED or machine_type.startswith(PLACEHOLDER):
            failures.append(f"fleet: role {name!r} has no machine_type ({machine_type!r})")
        for field, value in (("vcpu", role["vcpu"]), ("gib", role["gib"])):
            if value <= 0:
                failures.append(f"fleet: role {name!r} has a non-positive {field} ({value!r})")
    return failures


def _corpus_failures(run: Run, shipped_presets: set[str], workloads: Path) -> list[str]:
    spec = run["spec"]
    name = spec["corpus"]
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


def _document_failures(document: ResultDocument, shipped_presets: set[str], workloads: Path) -> list[str]:
    """Every rule beyond `schema_version` that a document must satisfy."""
    run = document["run"]
    spec = run["spec"]
    failures: list[str] = []

    producer = spec.get("producer", {})
    # A null `seconds` value denotes a full offer, even when the key is present.
    if "seconds" in producer and producer["seconds"] is not None:
        failures.append(
            f"producer.seconds: spec.producer.seconds is set ({producer['seconds']!r}) — a shortened offer "
            "is not publishable"
        )

    failures.extend(_corpus_failures(run, shipped_presets, workloads))

    derived = document["derived"]
    if derived["cost"]["usd_per_hour"] is None:
        failures.append("derived.cost.usd_per_hour: is null — a published result must disclose resource cost")
    if derived["keepup"] is None:
        failures.append("derived.keepup: is null")
    if derived["producer"]["producer_bound"] is None:
        failures.append("derived.producer.producer_bound: is null")

    data = document["data"]
    if data["summary"] is None:
        failures.append("data.summary: is null — a published result must include the scorer's summary")

    failures.extend(_fleet_failures(run["fleet"]))

    site_pricing = run["site_pricing"]
    # Example zero prices are placeholders, not a valid cost disclosure.
    for field, price in (
        ("vcpu_hour_usd", site_pricing["vcpu_hour_usd"]),
        ("gib_hour_usd", site_pricing["gib_hour_usd"]),
    ):
        if price <= 0:
            failures.append(f"site_pricing.{field}: is {price!r}, so the cost column has no price behind it")

    return failures


def _freshness_failures(documents: list[tuple[Path, ResultDocument]]) -> list[str]:
    """Find table and topic names reused across published results."""
    failures: list[str] = []
    for field in ("table", "topic"):
        seen: dict[str, Path] = {}
        for path, document in documents:
            run = document["run"]
            name = run.get("table") if field == "table" else run.get("topic")
            if name is None:
                failures.append(f"{path}: {field}: run.{field} is absent, so its freshness cannot be checked")
                continue
            if name in seen:
                failures.append(f"{path}: {field}: {name!r} is also the {field} of {seen[name]}")
                continue
            seen[name] = path
    return failures


def validate(results_dir: Path, *, workloads: Path) -> list[str]:
    """Return failures as ``<path>: <rule>: <detail>`` lines.

    An empty results directory is valid if its generated table is current.
    """
    shipped_presets = {preset_path.stem for preset_path in (workloads / "presets").glob("*.yaml")}
    failures: list[str] = []
    documents: list[tuple[Path, ResultDocument]] = []
    all_renderable = True

    for path in sorted(results_dir.glob("**/*.json")):
        text = path.read_text(encoding="utf-8")
        failures.extend(_uri_failures(path, text))
        try:
            value = json.loads(text)
        except json.JSONDecodeError as err:
            failures.append(f"{path}: json: {err}")
            all_renderable = False
            continue
        failures.extend(_account_id_failures(path, value))
        try:
            document = parse_result(value)
        except ValueError as err:
            failures.append(f"{path}: {err}")
            all_renderable = False
            continue
        if document["data"]["summary"] is None:
            all_renderable = False
        documents.append((path, document))
        failures.extend(f"{path}: {failure}" for failure in _document_failures(document, shipped_presets, workloads))

    # Skip table comparison if a document cannot be rendered; its parse failure
    # is already reported above.
    failures.extend(_freshness_failures(documents))

    if all_renderable:
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
