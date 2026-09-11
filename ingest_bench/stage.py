# SPDX-License-Identifier: Apache-2.0
"""Create a run's topic and table, then write its engine configuration.

Staging writes ``facts.json`` and either creates the table or supplies its DDL.
External engines can use these facts without a harness-specific integration.
Validate local inputs before creating the topic, and delete the topic if a
later step fails so staging can be retried.
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
from ingest_bench.kafka_auth import refuse_java_oauth
from ingest_bench.schema_registry import register_schema, subject_for
from ingest_bench.specs import derive as derive_module
from ingest_bench.specs import model
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.engines import knobs_for, kubernetes_for
from ingest_bench.specs.env import resolve_env_placeholders
from ingest_bench.specs.kubernetes import object_name
from ingest_bench.table.create import create_table, parse_partition
from ingest_bench.table.ddl import spark_sql_ddl

# Use up to three replicas, capped by the broker count.
MAX_REPLICATION_FACTOR = 3

# No Kafka message key; records are not partitioned by a column value.
KEY_NONE = "none"

STAGED = "staged"

# Use the full configuration path in credential errors.
_REGISTRY_AUTH_KEY = "site.kafka.schema_registry.basic_auth_user_info"

# Use sortable UTC timestamps with seconds precision.
_TIMELINE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class KafkaAdmin(Protocol):
    """Kafka operations required by staging, injectable for tests.

    Methods receive client properties because staging loads the site and resolves
    credentials before calling the admin.
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
    """Kafka admin backed by the configured cluster."""

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
    """Registered schema used by every record in a Confluent-framed run.

    The producer writes this schema ID into each record header.
    """

    url: str
    subject: str
    schema_id: int


@dataclass(frozen=True)
class Staged:
    """Validated spec, derived names, run directory, and generated facts."""

    spec: model.RunSpec
    derived: Derived
    run_dir: Path
    facts: dict[str, object]


def resolve_corpus_dir(corpus_root: str, name: str) -> str:
    """Find the unique corpus named ``name`` under ``corpus_root``.

    Directories include the preset hash, so several versions may coexist.
    Reject ambiguous matches rather than silently choosing a scoring input.
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
    """Redact literal credentials while preserving configuration URIs.

    Engines read these properties from ``facts.json``. Publication also redacts
    site roots; see ``collect.redact``.
    """
    return redact_props(props, None)


def replication_factor(brokers: int) -> int:
    return min(MAX_REPLICATION_FACTOR, brokers)


def harness_table_properties(spec: model.RunSpec) -> dict[str, str]:
    """Return table properties with Iceberg format version 2 enforced.

    Equality and position deletes require version 2. Engine-owned tables receive
    the spec's properties through their DDL instead.
    """
    return {**spec.table.properties, "format-version": "2"}


def _facts(
    spec: model.RunSpec,
    site: model.SiteConfig,
    derived: Derived,
    ddl: str | None,
    registration: SchemaRegistration | None,
) -> dict[str, object]:
    """Build engine configuration facts in their printed order.

    Keep ``run_id`` first for shell readers. Leave ``epoch`` null until launch so
    staging delays do not count toward the run. Always include ``value_encoding``
    and ``compression``; raw Avro runs have null registry fields.
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
        "compression": spec.producer.compression,
        "catalog_props": redact(site.catalog_props),
        "table": derived.table,
        "partition": spec.table.partition,
        "ddl": ddl,
        "key_column": None if spec.kafka.key == KEY_NONE else spec.kafka.key,
        "epoch": None,
    }


def _refuse_an_unaddressable_name(spec: model.RunSpec, derived: Derived) -> None:
    """Reject object names that exceed the engine operator's declared limit.

    Check before creating resources to avoid staging a run that cannot launch.
    """
    limit = kubernetes_for(spec.engine).max_object_name_length
    if limit is None:
        return
    name = object_name(derived.run_id)
    if len(name) <= limit:
        return
    raise ValueError(
        f"spec.name {spec.name!r} is {len(spec.name)} characters, and a {spec.engine!r} run's object name is that "
        f"plus the run's stamp — {len(name)} characters, past the {limit} its operator accepts. "
        f"Name this run in at most {limit - (len(name) - len(spec.name))} characters"
    )


def _registry_for(spec: model.RunSpec, site: model.SiteConfig) -> model.SchemaRegistryConfig | None:
    """Return the registry for Confluent framing, or ``None`` for raw Avro.

    Reject a missing required registry before creating cluster resources.
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
    """Format facts as one ``key: value`` line each.

    Encode mappings, multiline strings, and other non-string values as JSON so
    shell readers can parse one fact per line.
    """
    lines: list[str] = []
    for key, value in facts.items():
        scalar = isinstance(value, str) and "\n" not in value
        lines.append(f"{key}: {value if scalar else json.dumps(value)}")
    return lines


def publish_run_dir(run_dir: Path, upload_prefix: str, run_id: str) -> None:
    """Copy the run directory to ``<upload_prefix>/<run_id>/stage/``.

    This preserves artifacts from staging Jobs after their pods are removed.
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
    """Create the topic and table and write the run directory.

    Validate local inputs before modifying cluster resources.
    """
    site = model.load_site(site_path)
    spec = model.load_run_spec(spec_path)
    if not spec.is_external():
        refuse_java_oauth(site.kafka_security)
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
        _refuse_an_unaddressable_name(spec, derived)
        # Require the image tag before creating resources.
        if site.kubernetes is not None and image_tag is None:
            raise ValueError(
                "a managed run on a cluster starts an image, so staging needs --image-tag: "
                "the tag push-images.sh pushed"
            )

    # Resolve credentials only for client calls; keep placeholders in artifacts.
    # Resolve all references before creating resources so missing variables fail early.
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
            # Use the storage warehouse for explicit locations. Glue REST catalogs use
            # the catalog `warehouse` property for an account ID, not a storage path.
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
            # Register the same schema file that `schema_avsc_uri` gives consumers.
            subject = subject_for(derived.topic)
            schema_id = register_schema(
                registry.url, registry_auth, subject, uri.read_text(uri.join(derived.corpus_uri, "schema.avsc"))
            )
            registration = SchemaRegistration(url=registry.url, subject=subject, schema_id=schema_id)
        facts = _facts(spec, site, derived, ddl, registration)
        run_dir = runs_dir / derived.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        # Preserve comments and omitted defaults in the staged spec.
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
        # Delete the topic on errors and interrupts so staging can be retried.
        # Leave any created table for explicit teardown to avoid unintended deletion.
        admin.delete(site.kafka_bootstrap, derived.topic, kafka_client)
        raise
    return Staged(spec=spec, derived=derived, run_dir=run_dir, facts=facts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stage", description="Prepare a run's topic, table, and configuration files.")
    parser.add_argument("--spec", required=True, metavar="PATH", help="the run spec to stage")
    parser.add_argument(
        "--site", required=True, metavar="PATH", help="the site config naming storage, broker and catalog"
    )
    parser.add_argument("--runs-dir", default="./runs", metavar="DIR", help="where the run directory is written")
    parser.add_argument(
        "--stamp",
        metavar="YYYYmmddTHHMMSSZ",
        help="timestamp to reuse when staging a run with its original ID",
    )
    parser.add_argument(
        "--image-tag",
        metavar="TAG",
        help="image tag published by push-images.sh. Required when the site declares a Kubernetes cluster",
    )
    parser.add_argument(
        "--upload-prefix",
        metavar="URI",
        help="upload the run directory to <URI>/<run id>/stage/ so it survives the staging pod's deletion",
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
