"""Stage a run: create the topic and the table, and write down what an engine needs.

Staging is the seam between the harness and whatever is being scored. It ends
with a topic that exists, a table that exists (or the statement to create one)
and a `facts.json` an operator can read to point any engine at both — which is
what makes the external tier possible: nothing after this point is specific to
an engine the harness knows how to run.

The topic is created last among the checks and rolled back on any later
failure. A half-staged run that left a topic behind would fail the next
attempt at the same name for a reason unrelated to what actually went wrong.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

from ingest_bench import kafka_admin, uri
from ingest_bench.corpus import metadata
from ingest_bench.specs import derive as derive_module
from ingest_bench.specs import model
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.engines import knobs_for
from ingest_bench.table.create import create_table, parse_partition
from ingest_bench.table.ddl import spark_sql_ddl

REDACTED = "<redacted>"

# A property whose name contains one of these carries a credential. Matching on
# the name rather than the value is what keeps the run directory publishable:
# `facts.json` is meant to be pasted into an issue or an engine's config, and a
# catalog token is the one thing in it that must not travel.
_SECRET_HINTS = ("token", "credential", "secret", "password")

# Hosts that only ever name a single-broker cluster — a laptop stack or the
# compose service — get a replication factor of 1 because a higher one cannot
# be satisfied. Anything else is assumed to be a real cluster of at least
# three brokers. Phase 2 reads the broker count from the cluster instead.
_SINGLE_BROKER_HOSTS = frozenset({"localhost", "127.0.0.1", "kafka"})

# The spec's way of saying the producer sends no message key, so records
# round-robin across partitions instead of following a column's skew.
KEY_NONE = "none"

STAGED = "staged"

# The timeline is the run's audit trail, appended to at every phase transition,
# so its timestamps are seconds-resolution UTC and sort lexicographically.
_TIMELINE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class KafkaAdmin(Protocol):
    """The topic operations staging needs, behind a protocol so it can be faked.

    Staging is otherwise untestable without a broker, and the part worth
    testing — that a failure after creation drops the topic again — is exactly
    the part a live broker makes hard to provoke.
    """

    def exists(self, bootstrap: str, name: str) -> bool: ...

    def create(
        self, bootstrap: str, name: str, partitions: int, replication_factor: int, config: dict[str, str]
    ) -> None: ...

    def delete(self, bootstrap: str, name: str) -> None: ...


class ClusterAdmin:
    """The real topic admin, against the broker the site names."""

    def exists(self, bootstrap: str, name: str) -> bool:
        return kafka_admin.topic_exists(bootstrap, name)

    def create(
        self, bootstrap: str, name: str, partitions: int, replication_factor: int, config: dict[str, str]
    ) -> None:
        kafka_admin.create_topic(bootstrap, name, partitions, replication_factor, config)

    def delete(self, bootstrap: str, name: str) -> None:
        kafka_admin.delete_topic(bootstrap, name)


@dataclass(frozen=True)
class Staged:
    """What staging produced: the spec it read, the run's names, and its facts."""

    spec: model.RunSpec
    derived: Derived
    run_dir: Path
    facts: dict[str, object]


def resolve_corpus_dir(corpus_root: str, name: str) -> str:
    """The directory under ``corpus_root`` holding the corpus called ``name``.

    A corpus directory carries the hash of the preset that produced it, so two
    generations of the same preset coexist under different names. Ambiguity is
    refused rather than resolved by recency: the older of the two is a
    legitimate scoring input, and picking one silently would score a table
    against a corpus nobody chose.
    """
    candidates = [
        d
        for d in uri.listdir(corpus_root)
        if d.startswith(f"{name}-") and uri.exists(uri.join(corpus_root, d, "corpus.json"))
    ]
    matches = [
        d
        for d in candidates
        if cast(dict[str, object], json.loads(uri.read_text(uri.join(corpus_root, d, "corpus.json"))))["name"] == name
    ]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one corpus named {name!r} under {corpus_root}, found {matches}")
    return matches[0]


def redact(props: dict[str, str]) -> dict[str, str]:
    """``props`` with every credential-shaped value replaced."""
    return {
        key: REDACTED if any(hint in key.lower() for hint in _SECRET_HINTS) else value for key, value in props.items()
    }


def replication_factor(bootstrap: str) -> int:
    hosts = {entry.split(":")[0].strip() for entry in bootstrap.split(",")}
    return 1 if hosts <= _SINGLE_BROKER_HOSTS else 3


def harness_table_properties(spec: model.RunSpec) -> dict[str, str]:
    """The properties the harness creates the table with.

    Format version 2 explicitly: an engine writing equality or position
    deletes needs it, and a table created at version 1 would fail the run for
    a reason that has nothing to do with the engine's ingest path. An
    engine-owned table is given the spec's properties untouched, since the
    statement it runs is the one a reader has to be able to check.
    """
    return {**spec.table.properties, "format-version": "2"}


def _facts(spec: model.RunSpec, site: model.SiteConfig, derived: Derived, ddl: str | None) -> dict[str, object]:
    """Everything an engine needs to join the run, in the order it is printed.

    ``run_id`` comes first because the scripts read it off the first line.
    ``epoch`` is null until the run is launched: the time origin is chosen when
    the producer starts, not when the topic is created, so a staged run that
    waits an hour for an operator is not scored from the moment it was staged.
    """
    return {
        "run_id": derived.run_id,
        "bootstrap": site.kafka_bootstrap,
        "topic": derived.topic,
        "corpus_uri": derived.corpus_uri,
        "schema_avsc_uri": uri.join(derived.corpus_uri, "schema.avsc"),
        "catalog_props": redact(site.catalog_props),
        "table": derived.table,
        "partition": spec.table.partition,
        "ddl": ddl,
        "key_column": None if spec.kafka.key == KEY_NONE else spec.kafka.key,
        "epoch": None,
    }


def timeline_line(event: str) -> str:
    return f"{datetime.now(UTC).strftime(_TIMELINE_TIME_FORMAT)} {event}"


def facts_lines(facts: dict[str, object]) -> list[str]:
    """The facts as ``key: value`` lines.

    A value that is not a single-line string is printed as JSON: the DDL spans
    lines and the catalog properties are a mapping, and either would break the
    one-line-per-fact shape the scripts parse.
    """
    lines: list[str] = []
    for key, value in facts.items():
        scalar = isinstance(value, str) and "\n" not in value
        lines.append(f"{key}: {value if scalar else json.dumps(value)}")
    return lines


def stage(
    spec_path: Path,
    site_path: Path,
    runs_dir: Path,
    admin: KafkaAdmin,
    *,
    stamp: str | None = None,
) -> Staged:
    """Create the run's topic and table and write its run directory.

    Every refusal that can be raised without touching the cluster is raised
    first, so the common failures — a misspelled knob, a key column the corpus
    does not carry, an unresolvable corpus — cost nothing to recover from.
    """
    site = model.load_site(site_path)
    spec = model.load_run_spec(spec_path)
    derived = derive_module.derive(
        spec, site, stamp=stamp, corpus_dir=resolve_corpus_dir(site.corpus_root, spec.corpus)
    )
    meta = metadata.read(derived.corpus_uri)
    if spec.kafka.key != KEY_NONE and spec.kafka.key not in meta.key_columns:
        raise ValueError(
            f"spec.kafka.key {spec.kafka.key!r} is not a key column of corpus {meta.name!r}; "
            f"it publishes {list(meta.key_columns)} and accepts {KEY_NONE!r}"
        )
    partition = parse_partition(spec.table.partition)
    knobs = None
    if not spec.is_external():
        knobs = knobs_for(spec.engine)
        knobs.validate(spec.engine_block, spec, meta)

    if admin.exists(site.kafka_bootstrap, derived.topic):
        raise ValueError(f"topic {derived.topic!r} already exists on {site.kafka_bootstrap}; it holds another run")
    admin.create(
        site.kafka_bootstrap,
        derived.topic,
        spec.kafka.partitions,
        replication_factor(site.kafka_bootstrap),
        dict(kafka_admin.DEFAULT_TOPIC_CONFIG),
    )
    try:
        ddl: str | None = None
        if spec.table.managed_by == model.HARNESS:
            create_table(site.catalog_props, derived.table, meta, partition, harness_table_properties(spec))
        else:
            ddl = spark_sql_ddl(meta, derived.table, partition, spec.table.properties)
        facts = _facts(spec, site, derived, ddl)
        run_dir = runs_dir / derived.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        # The spec is copied verbatim rather than re-serialised: it is the
        # published record of what was asked for, and a round trip through the
        # loader would drop its comments and print its defaults as if they had
        # been chosen.
        (run_dir / "spec.yaml").write_text(spec_path.read_text())
        (run_dir / "facts.json").write_text(json.dumps(facts, indent=2) + "\n")
        (run_dir / "timeline.log").write_text(f"{timeline_line(STAGED)}\n")
        if knobs is not None:
            for filename, content in cast(dict[str, str], knobs.render(spec, site, derived, meta)).items():
                (run_dir / filename).write_text(content)
    except BaseException:
        # An interrupt gets the same treatment as an error: the topic is the
        # one thing a failed staging leaves that blocks the next attempt at the
        # same name. A table it may also have created is left alone — a table
        # with no data is inert, and a teardown that drops tables on its own is
        # a worse failure mode than an orphan.
        admin.delete(site.kafka_bootstrap, derived.topic)
        raise
    return Staged(spec=spec, derived=derived, run_dir=run_dir, facts=facts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stage", description="Create a run's topic and table and write its facts.")
    parser.add_argument("--spec", required=True, metavar="PATH", help="the run spec to stage")
    parser.add_argument(
        "--site", required=True, metavar="PATH", help="the site config naming storage, broker and catalog"
    )
    parser.add_argument("--runs-dir", default="./runs", metavar="DIR", help="where the run directory is written")
    parser.add_argument(
        "--stamp",
        metavar="YYYYmmddTHHMMSSZ",
        help="the run's time stamp, for re-staging a run under its original identifier",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    staged = stage(
        Path(str(args.spec)),
        Path(str(args.site)),
        Path(str(args.runs_dir)),
        ClusterAdmin(),
        stamp=None if args.stamp is None else str(args.stamp),
    )
    for line in facts_lines(staged.facts):
        print(line)
    print(f"run_dir: {staged.run_dir}")
    if staged.spec.is_external():
        print("Start your engine now; run launch when it is consuming.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
