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
from ingest_bench.catalog import table_identifier
from ingest_bench.collect.redact import redact_props
from ingest_bench.corpus import metadata
from ingest_bench.schema_registry import register_schema, subject_for
from ingest_bench.specs import derive as derive_module
from ingest_bench.specs import model
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.engines import knobs_for
from ingest_bench.specs.env import resolve_env_placeholders
from ingest_bench.table.create import create_table, parse_partition
from ingest_bench.table.ddl import spark_sql_ddl

# Three replicas is what a run's records are worth: enough that losing one
# broker mid-run does not end it, and no more than the smallest cluster anyone
# runs this against can satisfy. A cluster with fewer brokers than that gets as
# many replicas as it has brokers, since a factor above the broker count is
# refused outright.
MAX_REPLICATION_FACTOR = 3

# The spec's way of saying the producer sends no message key, so records
# round-robin across partitions instead of following a column's skew.
KEY_NONE = "none"

STAGED = "staged"

# The site key a `confluent` run needs, named in the refusal so an operator
# reads which block to add rather than which call failed.
_REGISTRY_AUTH_KEY = "site.kafka.schema_registry.basic_auth_user_info"

# The timeline is the run's audit trail, appended to at every phase transition,
# so its timestamps are seconds-resolution UTC and sort lexicographically.
_TIMELINE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class KafkaAdmin(Protocol):
    """The cluster operations staging needs, behind a protocol so it can be faked.

    Staging is otherwise untestable without a broker, and the parts worth
    testing — that a failure after creation drops the topic again, and that the
    replication factor follows the cluster — are exactly the parts a live
    broker makes hard to provoke.

    Every method takes the client properties to reach the cluster with, rather
    than the admin holding them, because the site that declares them is read
    inside `stage` and a secret they name is resolved at the call itself.
    """

    def exists(self, bootstrap: str, name: str, client: dict[str, str]) -> bool: ...

    def broker_count(self, bootstrap: str, client: dict[str, str]) -> int: ...

    def create(
        self,
        bootstrap: str,
        name: str,
        partitions: int,
        replication_factor: int,
        topic_config: dict[str, str],
        client: dict[str, str],
    ) -> None: ...

    def delete(self, bootstrap: str, name: str, client: dict[str, str]) -> None: ...


class ClusterAdmin:
    """The real cluster admin, against the broker the site names."""

    def exists(self, bootstrap: str, name: str, client: dict[str, str]) -> bool:
        return kafka_admin.topic_exists(bootstrap, name, client)

    def broker_count(self, bootstrap: str, client: dict[str, str]) -> int:
        return kafka_admin.broker_count(bootstrap, client)

    def create(
        self,
        bootstrap: str,
        name: str,
        partitions: int,
        replication_factor: int,
        topic_config: dict[str, str],
        client: dict[str, str],
    ) -> None:
        kafka_admin.create_topic(bootstrap, name, partitions, replication_factor, topic_config, client)

    def delete(self, bootstrap: str, name: str, client: dict[str, str]) -> None:
        kafka_admin.delete_topic(bootstrap, name, client)


@dataclass(frozen=True)
class SchemaRegistration:
    """The schema a `confluent` run's records point at, once it has an id.

    The id is what the producer's header carries and what every reader resolves
    the writer schema by, so it is a fact about the run rather than about the
    registry: one id for the whole run, chosen before the first record.
    """

    url: str
    subject: str
    schema_id: int


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
    """``props`` with every credential-shaped literal value replaced.

    No site is passed, so the URIs among the values survive: `facts.json` is
    what an engine is configured from, and a warehouse property rewritten to a
    placeholder would point it at nothing. The published copy of the same
    properties loses them; see `collect.redact`.
    """
    return redact_props(props, None)


def replication_factor(brokers: int) -> int:
    return min(MAX_REPLICATION_FACTOR, brokers)


def harness_table_properties(spec: model.RunSpec) -> dict[str, str]:
    """The properties the harness creates the table with.

    Format version 2 explicitly: an engine writing equality or position
    deletes needs it, and a table created at version 1 would fail the run for
    a reason that has nothing to do with the engine's ingest path. An
    engine-owned table is given the spec's properties untouched, since the
    statement it runs is the one a reader has to be able to check.
    """
    return {**spec.table.properties, "format-version": "2"}


def _facts(
    spec: model.RunSpec,
    site: model.SiteConfig,
    derived: Derived,
    ddl: str | None,
    registration: SchemaRegistration | None,
) -> dict[str, object]:
    """Everything an engine needs to join the run, in the order it is printed.

    ``run_id`` comes first because the scripts read it off the first line.
    ``epoch`` is null until the run is launched: the time origin is chosen when
    the producer starts, not when the topic is created, so a staged run that
    waits an hour for an operator is not scored from the moment it was staged.

    ``value_encoding`` is stated for every run and the three registry facts
    are null where it is the raw one, rather than being left out: a reader that
    had to tell an absent key from a null one would read a harness too old to
    know the difference as a run that offered raw Avro.
    """
    return {
        "run_id": derived.run_id,
        "bootstrap": site.kafka_bootstrap,
        "topic": derived.topic,
        "corpus_uri": derived.corpus_uri,
        "schema_avsc_uri": uri.join(derived.corpus_uri, "schema.avsc"),
        "value_encoding": spec.kafka.value_encoding,
        "schema_registry_url": None if registration is None else registration.url,
        "schema_subject": None if registration is None else registration.subject,
        "schema_id": None if registration is None else registration.schema_id,
        "catalog_props": redact(site.catalog_props),
        "table": derived.table,
        "partition": spec.table.partition,
        "ddl": ddl,
        "key_column": None if spec.kafka.key == KEY_NONE else spec.kafka.key,
        "epoch": None,
    }


def _registry_for(spec: model.RunSpec, site: model.SiteConfig) -> model.SchemaRegistryConfig | None:
    """The registry this run registers with, or ``None`` for a raw-Avro run.

    A `confluent` run against a site that declares no registry is refused here,
    with the other refusals that cost nothing: the alternative is a topic and a
    table that exist for a run no engine can be pointed at.
    """
    if spec.kafka.value_encoding != model.VALUE_ENCODING_CONFLUENT:
        return None
    if site.schema_registry is None:
        raise ValueError(
            f"spec.kafka.value_encoding is {model.VALUE_ENCODING_CONFLUENT!r}, which registers the corpus's "
            "schema, and the site declares no kafka.schema_registry.url to register it with"
        )
    return site.schema_registry


def _registry_auth(registry: model.SchemaRegistryConfig) -> str | None:
    if registry.basic_auth_user_info is None:
        return None
    return resolve_env_placeholders({_REGISTRY_AUTH_KEY: registry.basic_auth_user_info})[_REGISTRY_AUTH_KEY]


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


def publish_run_dir(run_dir: Path, upload_prefix: str, run_id: str) -> None:
    """Copy the run directory to ``<upload_prefix>/<run_id>/stage/``.

    Staging runs as a Job wherever the broker is only reachable from inside its
    own network, and that pod's filesystem goes with the pod. Publishing the
    directory is what leaves it somewhere the operator's machine can fetch it
    from afterwards.
    """
    for path in sorted(run_dir.iterdir()):
        uri.write_bytes(uri.join(upload_prefix, run_id, "stage", path.name), path.read_bytes())


def stage(
    spec_path: Path,
    site_path: Path,
    runs_dir: Path,
    admin: KafkaAdmin,
    *,
    stamp: str | None = None,
    image_tag: str | None = None,
    upload_prefix: str | None = None,
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
    registry = _registry_for(spec, site)
    knobs = None
    if not spec.is_external():
        knobs = knobs_for(spec.engine)
        knobs.validate(spec.engine_block, spec, meta)
        # Refused here rather than at the render that needs it: by then the
        # topic exists and the table with it, so a forgotten flag would cost a
        # rollback instead of an error message.
        if site.kubernetes is not None and image_tag is None:
            raise ValueError(
                "a managed run on a cluster starts an image, so staging needs --image-tag: "
                "the tag push-images.sh pushed"
            )

    # Resolved here and not at load: the site config, the run's facts and the
    # engine's rendered script all keep the placeholder, and only the calls
    # below ever see the value. Both are resolved before the cluster is
    # touched, so an unset variable is a refusal rather than a half-staged run.
    kafka_client = resolve_env_placeholders(site.kafka_security)
    catalog_props = resolve_env_placeholders(site.catalog_props)
    registry_auth = None if registry is None else _registry_auth(registry)
    if admin.exists(site.kafka_bootstrap, derived.topic, kafka_client):
        raise ValueError(f"topic {derived.topic!r} already exists on {site.kafka_bootstrap}; it holds another run")
    admin.create(
        site.kafka_bootstrap,
        derived.topic,
        spec.kafka.partitions,
        replication_factor(admin.broker_count(site.kafka_bootstrap, kafka_client)),
        dict(kafka_admin.DEFAULT_TOPIC_CONFIG),
        kafka_client,
    )
    try:
        ddl: str | None = None
        if spec.table.managed_by == model.HARNESS:
            # Both locations come from `site.warehouse` and never from the
            # catalog's own `warehouse` property: a Glue Iceberg REST catalog
            # reads an account id there, so a table created without a location
            # would land nowhere a bucket can hold.
            namespace, table_name = table_identifier(derived.table)
            create_table(
                catalog_props,
                derived.table,
                meta,
                partition,
                harness_table_properties(spec),
                location=uri.join(site.warehouse, namespace, table_name),
                namespace_location=uri.join(site.warehouse, namespace),
            )
        else:
            ddl = spark_sql_ddl(meta, derived.table, partition, spec.table.properties)
        registration = None
        if registry is not None:
            # The corpus's own file rather than the schema `corpus.json`
            # embeds: it is the document `schema_avsc_uri` points every reader
            # at, so what is registered is byte for byte what a reader that
            # skipped the registry would use instead.
            subject = subject_for(derived.topic)
            schema_id = register_schema(
                registry.url, registry_auth, subject, uri.read_text(uri.join(derived.corpus_uri, "schema.avsc"))
            )
            registration = SchemaRegistration(url=registry.url, subject=subject, schema_id=schema_id)
        facts = _facts(spec, site, derived, ddl, registration)
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
            rendered = knobs.render(spec, site, derived, meta, image_tag=image_tag)
            for filename, content in cast(dict[str, str], rendered).items():
                (run_dir / filename).write_text(content)
        if upload_prefix is not None:
            publish_run_dir(run_dir, upload_prefix, derived.run_id)
    except BaseException:
        # An interrupt gets the same treatment as an error: the topic is the
        # one thing a failed staging leaves that blocks the next attempt at the
        # same name. A table it may also have created is left alone — a table
        # with no data is inert, and a teardown that drops tables on its own is
        # a worse failure mode than an orphan.
        admin.delete(site.kafka_bootstrap, derived.topic, kafka_client)
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
    parser.add_argument(
        "--image-tag",
        metavar="TAG",
        help="the tag of the images push-images.sh pushed, which a managed engine's cluster documents start. "
        "Required when the site declares a cluster",
    )
    parser.add_argument(
        "--upload-prefix",
        metavar="URI",
        help="publish the run directory under <URI>/<run id>/stage/, for a staging that ran as a Job and whose "
        "filesystem went with its pod",
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
        image_tag=None if args.image_tag is None else str(args.image_tag),
        upload_prefix=None if args.upload_prefix is None else str(args.upload_prefix),
    )
    for line in facts_lines(staged.facts):
        print(line)
    print(f"run_dir: {staged.run_dir}")
    if staged.spec.is_external():
        print("Start your engine now; run launch when it is consuming.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
