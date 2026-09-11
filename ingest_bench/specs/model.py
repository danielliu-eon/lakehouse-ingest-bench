# SPDX-License-Identifier: Apache-2.0
"""Load and validate run specs and site configuration.

Reject unknown keys so misspelled settings cannot silently use defaults.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml

from ingest_bench.kafka_auth import refuse_mechanism_alias
from ingest_bench.specs import engines
from ingest_bench.specs.env import refuse_literal_secrets

# Allow names shared by Kafka, SQL, and Kubernetes. A 35-character spec name
# plus a 17-character stamp and the longest Job prefix (`drop-topic-`, 11)
# fits Kubernetes' 63-character job-name label.
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{2,34}$")

EXTERNAL = "external"
HARNESS = "harness"
ENGINE_OWNED = "engine"

# Raw Avro is the default; Confluent framing adds a five-byte schema header.
VALUE_ENCODING_AVRO = "avro"
VALUE_ENCODING_CONFLUENT = "confluent"
VALUE_ENCODINGS = frozenset({VALUE_ENCODING_AVRO, VALUE_ENCODING_CONFLUENT})

# Use librdkafka codec names. Compression is a workload setting shared by
# compared runs.
COMPRESSION_DEFAULT = "zstd"
COMPRESSIONS = frozenset({"gzip", "lz4", "none", "snappy", COMPRESSION_DEFAULT})

# Reject compression overrides in client properties so the actual codec
# matches the spec and published facts.
_COMPRESSION_PROP_PREFIX = "compression."

# Default geometry sample offsets, in seconds from run start.
DEFAULT_GEOMETRY_OFFSETS_S: tuple[int, ...] = (600, 1200, 1800, 2700, 3600)

_SPEC_KEYS = frozenset({"name", "engine", "corpus", "table", "kafka", "producer", "scoring", "external", "fleet"})
_SITE_KEYS = frozenset({"corpus_root", "runs_root", "warehouse", "kafka", "catalog", "kubernetes", "pricing"})
_KUBERNETES_KEYS = frozenset(
    {
        "context",
        "namespace",
        "harness_service_account",
        "flink_service_account",
        "spark_service_account",
        "service_account_annotations",
        "registry",
        "aws_region",
        "secret_name",
        "node_selector",
        "tolerations",
    }
)

# Default the Spark service account for older site configs. AWS setup creates
# this same account name.
_SPARK_SERVICE_ACCOUNT = "ingest-bench-spark"

# Share the placeholder prefix with result-publication validation.
PLACEHOLDER = "YOUR_"

# Default to the partition column used by shipped corpora.
_PARTITION_DEFAULT_COLUMN = "partition_key"


# ---------------------------------------------------------------------------
# Reading YAML values
# ---------------------------------------------------------------------------


def _as_mapping(value: object, where: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a mapping, got {type(value).__name__}")
    return {str(key): entry for key, entry in cast(dict[object, object], value).items()}


def _as_str(value: object, where: str) -> str:
    # Require quoted strings: YAML booleans and numbers can change spelling
    # when coerced, producing invalid client properties or version labels.
    if not isinstance(value, str):
        raise ValueError(f"{where} must be a string; quote it in the YAML. Got {value!r}")
    return value


def _as_int(value: object, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} must be an integer, got {value!r}")
    return value


def _as_float(value: object, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{where} must be a number, got {value!r}")
    return float(value)


def _as_string_map(value: object, where: str) -> dict[str, str]:
    return {key: _as_str(entry, f"{where}.{key}") for key, entry in _as_mapping(value, where).items()}


def _as_string_maps(value: object, where: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise ValueError(f"{where} must be a list of mappings, got {value!r}")
    return [_as_string_map(entry, f"{where}[{index}]") for index, entry in enumerate(cast(list[object], value))]


def refuse_compression_props(props: Mapping[str, str], where: str) -> None:
    """Refuse a client property that would choose the wire codec behind the spec."""
    named = sorted(key for key in props if key.startswith(_COMPRESSION_PROP_PREFIX))
    if named:
        raise ValueError(
            f"{where} sets {named}, which would choose the wire codec instead of the run's spec; "
            f"set producer.compression (one of {sorted(COMPRESSIONS)}) and leave the property out"
        )


def _refuse_unknown(block: dict[str, object], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(set(block) - allowed)
    if unknown:
        raise ValueError(f"{where} has unknown keys {unknown}; the ones it takes are {sorted(allowed)}")


def _block(raw: dict[str, object], key: str, where: str) -> dict[str, object]:
    """The optional ``key`` block, empty when the file leaves it out."""
    if key not in raw:
        return {}
    return _as_mapping(raw[key], f"{where}.{key}")


def _required(raw: dict[str, object], key: str, where: str) -> object:
    if key not in raw:
        raise ValueError(f"{where} must set {key}")
    return raw[key]


def _load_yaml(path: Path, where: str) -> dict[str, object]:
    try:
        text = path.read_text()
    except OSError as error:
        raise ValueError(f"could not read {where} {path}: {error}") from error
    return _as_mapping(yaml.safe_load(text), str(path))


# ---------------------------------------------------------------------------
# Run spec
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TableSpec:
    """Table ownership, partitioning, and properties.

    ``managed_by`` selects whether the harness creates the table or supplies DDL
    for the engine to execute.
    """

    managed_by: str
    partition: str
    properties: dict[str, str]


@dataclass(frozen=True)
class KafkaSpec:
    """Topic partition count, message key, and value framing.

    These settings define the workload and must match across compared engines.
    ``avro`` sends corpus bytes directly; ``confluent`` adds a five-byte schema
    registry header.
    """

    partitions: int
    key: str
    value_encoding: str


@dataclass(frozen=True)
class ProducerSpec:
    speed: float
    seconds: int | None
    shards: int
    behind_max_ms: int
    compression: str


@dataclass(frozen=True)
class ScoringSpec:
    """Scoring settings and optional live-gate overrides.

    Absent gate fields use the gate's own defaults.
    """

    freshness_bound_s: float
    warmup_s: int
    geometry_offsets_s: tuple[int, ...]
    gate_adaptation_s: int | None
    gate_window_s: int | None


# Shared sentinel for an undisclosed machine type; publication rejects it.
MACHINE_TYPE_UNSPECIFIED = "unspecified"


@dataclass(frozen=True)
class FleetRole:
    """Compute resources for one fleet role, used to calculate cost."""

    role: str
    count: int
    vcpu: float
    gib: float
    machine_type: str


@dataclass(frozen=True)
class ExternalSpec:
    name: str
    version: str
    notes: str


@dataclass(frozen=True)
class RunSpec:
    name: str
    engine: str
    corpus: str
    table: TableSpec
    kafka: KafkaSpec
    producer: ProducerSpec
    scoring: ScoringSpec
    engine_block: dict[str, object]
    external: ExternalSpec | None
    fleet: tuple[FleetRole, ...]

    def is_external(self) -> bool:
        return self.engine == EXTERNAL


def _table_spec(raw: dict[str, object], partition_default: str) -> TableSpec:
    block = _block(raw, "table", "spec")
    _refuse_unknown(block, frozenset({"managed_by", "partition", "properties"}), "spec.table")
    managed_by = HARNESS if "managed_by" not in block else _as_str(block["managed_by"], "spec.table.managed_by")
    if managed_by not in (HARNESS, ENGINE_OWNED):
        raise ValueError(f"spec.table.managed_by must be {HARNESS!r} or {ENGINE_OWNED!r}, got {managed_by!r}")
    partition = partition_default if "partition" not in block else _as_str(block["partition"], "spec.table.partition")
    properties = {} if "properties" not in block else _as_string_map(block["properties"], "spec.table.properties")
    return TableSpec(managed_by=managed_by, partition=partition, properties=properties)


def _kafka_spec(raw: dict[str, object]) -> KafkaSpec:
    block = _as_mapping(_required(raw, "kafka", "spec"), "spec.kafka")
    _refuse_unknown(block, frozenset({"partitions", "key", "value_encoding"}), "spec.kafka")
    encoding = (
        VALUE_ENCODING_AVRO
        if "value_encoding" not in block
        else _as_str(block["value_encoding"], "spec.kafka.value_encoding")
    )
    if encoding not in VALUE_ENCODINGS:
        raise ValueError(f"spec.kafka.value_encoding must be one of {sorted(VALUE_ENCODINGS)}, got {encoding!r}")
    return KafkaSpec(
        partitions=_as_int(_required(block, "partitions", "spec.kafka"), "spec.kafka.partitions"),
        key=_as_str(_required(block, "key", "spec.kafka"), "spec.kafka.key"),
        value_encoding=encoding,
    )


def _producer_spec(raw: dict[str, object]) -> ProducerSpec:
    block = _block(raw, "producer", "spec")
    _refuse_unknown(block, frozenset({"speed", "seconds", "shards", "behind_max_ms", "compression"}), "spec.producer")
    compression = (
        COMPRESSION_DEFAULT
        if "compression" not in block
        else _as_str(block["compression"], "spec.producer.compression")
    )
    if compression not in COMPRESSIONS:
        raise ValueError(f"spec.producer.compression must be one of {sorted(COMPRESSIONS)}, got {compression!r}")
    producer = ProducerSpec(
        speed=1.0 if "speed" not in block else _as_float(block["speed"], "spec.producer.speed"),
        seconds=None if "seconds" not in block else _as_int(block["seconds"], "spec.producer.seconds"),
        shards=1 if "shards" not in block else _as_int(block["shards"], "spec.producer.shards"),
        behind_max_ms=5000
        if "behind_max_ms" not in block
        else _as_int(block["behind_max_ms"], "spec.producer.behind_max_ms"),
        compression=compression,
    )

    if not math.isfinite(producer.speed) or producer.speed <= 0:
        raise ValueError("spec.producer.speed must be finite and positive")
    for key, value in (("seconds", producer.seconds), ("shards", producer.shards)):
        if value is not None and value <= 0:
            raise ValueError(f"spec.producer.{key} must be positive")
    if producer.behind_max_ms < 0:
        raise ValueError("spec.producer.behind_max_ms must be nonnegative")
    return producer


def _scoring_spec(raw: dict[str, object]) -> ScoringSpec:
    block = _block(raw, "scoring", "spec")
    _refuse_unknown(
        block,
        frozenset({"freshness_bound_s", "warmup_s", "geometry_offsets_s", "gate_adaptation_s", "gate_window_s"}),
        "spec.scoring",
    )
    if "geometry_offsets_s" not in block:
        offsets = DEFAULT_GEOMETRY_OFFSETS_S
    else:
        raw_offsets = block["geometry_offsets_s"]
        if not isinstance(raw_offsets, list):
            raise ValueError(f"spec.scoring.geometry_offsets_s must be a list of seconds, got {raw_offsets!r}")
        offsets = tuple(
            _as_int(offset, f"spec.scoring.geometry_offsets_s[{index}]")
            for index, offset in enumerate(cast(list[object], raw_offsets))
        )
    scoring = ScoringSpec(
        freshness_bound_s=180.0
        if "freshness_bound_s" not in block
        else _as_float(block["freshness_bound_s"], "spec.scoring.freshness_bound_s"),
        warmup_s=120 if "warmup_s" not in block else _as_int(block["warmup_s"], "spec.scoring.warmup_s"),
        geometry_offsets_s=offsets,
        gate_adaptation_s=None
        if "gate_adaptation_s" not in block
        else _as_int(block["gate_adaptation_s"], "spec.scoring.gate_adaptation_s"),
        gate_window_s=None
        if "gate_window_s" not in block
        else _as_int(block["gate_window_s"], "spec.scoring.gate_window_s"),
    )

    if not math.isfinite(scoring.freshness_bound_s) or scoring.freshness_bound_s <= 0:
        raise ValueError("spec.scoring.freshness_bound_s must be finite and positive")
    for key, value in (("warmup_s", scoring.warmup_s), ("gate_adaptation_s", scoring.gate_adaptation_s)):
        if value is not None and value < 0:
            raise ValueError(f"spec.scoring.{key} must be nonnegative")
    if scoring.gate_window_s is not None and scoring.gate_window_s <= 0:
        raise ValueError("spec.scoring.gate_window_s must be positive")
    if any(offset < 0 for offset in offsets):
        raise ValueError("spec.scoring.geometry_offsets_s must be nonnegative")
    if any(right <= left for left, right in zip(offsets, offsets[1:], strict=False)):
        raise ValueError("spec.scoring.geometry_offsets_s must strictly ascend")
    return scoring


def _external_spec(raw: dict[str, object]) -> ExternalSpec:
    block = _as_mapping(_required(raw, "external", "spec"), "spec.external")
    _refuse_unknown(block, frozenset({"name", "version", "notes"}), "spec.external")
    return ExternalSpec(
        name=_as_str(_required(block, "name", "spec.external"), "spec.external.name"),
        version=_as_str(_required(block, "version", "spec.external"), "spec.external.version"),
        notes=_as_str(_required(block, "notes", "spec.external"), "spec.external.notes"),
    )


def _fleet(raw: dict[str, object]) -> tuple[FleetRole, ...]:
    entries = _required(raw, "fleet", "spec")
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"spec.fleet must be a non-empty list of roles, got {entries!r}")
    roles: list[FleetRole] = []
    for index, entry in enumerate(cast(list[object], entries)):
        where = f"spec.fleet[{index}]"
        block = _as_mapping(entry, where)
        _refuse_unknown(block, frozenset({"role", "count", "vcpu", "gib", "machine_type"}), where)
        roles.append(
            FleetRole(
                role=_as_str(_required(block, "role", where), f"{where}.role"),
                count=_as_int(_required(block, "count", where), f"{where}.count"),
                vcpu=_as_float(_required(block, "vcpu", where), f"{where}.vcpu"),
                gib=_as_float(_required(block, "gib", where), f"{where}.gib"),
                machine_type=_as_str(_required(block, "machine_type", where), f"{where}.machine_type"),
            )
        )
    return tuple(roles)


def load_run_spec(path: Path) -> RunSpec:
    """Load a run spec and apply defaults.

    Leave the engine block for its own validator so reading specs does not require
    engine-specific dependencies.
    """
    raw = _load_yaml(path, "run spec")
    engine = _as_str(_required(raw, "engine", "spec"), "spec.engine")
    managed = engine in engines.MANAGED
    if not managed and engine != EXTERNAL:
        raise ValueError(
            f"spec.engine {engine!r} is neither {EXTERNAL!r} nor a registered managed engine "
            f"({sorted(engines.MANAGED)})"
        )
    _refuse_unknown(raw, _SPEC_KEYS | ({engine} if managed else frozenset()), "spec")

    name = _as_str(_required(raw, "name", "spec"), "spec.name")
    if NAME_RE.match(name) is None:
        raise ValueError(
            f"spec.name names a topic, a table and a Kubernetes object, so it must match {NAME_RE.pattern}; "
            f"got {name!r}"
        )
    corpus = _as_str(_required(raw, "corpus", "spec"), "spec.corpus")
    kafka = _kafka_spec(raw)
    table = _table_spec(raw, partition_default=f"identity({_PARTITION_DEFAULT_COLUMN})")

    if engine == EXTERNAL:
        external = _external_spec(raw)
        fleet = _fleet(raw)
        engine_block: dict[str, object] = {}
    else:
        if "external" in raw or "fleet" in raw:
            raise ValueError(f"spec.engine {engine!r} is managed here, so it declares no external or fleet block")
        external = None
        fleet = ()
        engine_block = _as_mapping(_required(raw, engine, "spec"), f"spec.{engine}")

    return RunSpec(
        name=name,
        engine=engine,
        corpus=corpus,
        table=table,
        kafka=kafka,
        producer=_producer_spec(raw),
        scoring=_scoring_spec(raw),
        engine_block=engine_block,
        external=external,
        fleet=fleet,
    )


# ---------------------------------------------------------------------------
# Site config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KubernetesConfig:
    """Cluster placement, identities, and environment for run workloads.

    Namespaces and service accounts belong to the site so runs can reuse their
    configured cloud identities. ``aws_region`` supplies the pod region when set.
    ``secret_name`` names a Secret whose keys become environment variables in
    harness and engine pods, resolving ``${env:NAME}`` references at runtime.
    """

    context: str
    namespace: str
    harness_service_account: str
    flink_service_account: str
    spark_service_account: str
    service_account_annotations: dict[str, str]
    registry: str
    aws_region: str | None
    secret_name: str | None
    node_selector: dict[str, str]
    tolerations: list[dict[str, str]]


@dataclass(frozen=True)
class SchemaRegistryConfig:
    """Confluent-compatible registry URL and optional basic-auth configuration.

    Preserve ``${env:NAME}`` references until the registration request needs them.
    """

    url: str
    basic_auth_user_info: str | None


@dataclass(frozen=True)
class SiteConfig:
    """Operator-specific storage, Kafka, catalog, cluster, and pricing settings."""

    corpus_root: str
    runs_root: str
    warehouse: str
    kafka_bootstrap: str
    kafka_security: dict[str, str]
    schema_registry: SchemaRegistryConfig | None
    catalog_props: dict[str, str]
    kubernetes: KubernetesConfig | None
    pricing_vcpu_hour_usd: float
    pricing_gib_hour_usd: float
    kafka_deployment: str | None = None


def _refuse_placeholders(value: object, where: str) -> None:
    """Reject unreplaced example placeholders with their configuration paths."""
    if isinstance(value, str):
        if PLACEHOLDER in value:
            raise ValueError(f"{where} still holds the {PLACEHOLDER} placeholder: {value!r}")
        return
    if isinstance(value, dict):
        for key, entry in cast(dict[object, object], value).items():
            _refuse_placeholders(key, f"{where} key {key!r}")
            _refuse_placeholders(entry, f"{where}.{key}")
        return
    if isinstance(value, list):
        for index, entry in enumerate(cast(list[object], value)):
            _refuse_placeholders(entry, f"{where}[{index}]")


def _kubernetes_config(raw: dict[str, object]) -> KubernetesConfig | None:
    """Read the Kubernetes block; return ``None`` when absent or empty."""
    where = "site.kubernetes"
    block = _block(raw, "kubernetes", "site")
    if not block:
        return None
    _refuse_unknown(block, _KUBERNETES_KEYS, where)
    # Reject empty regions before they reach pod configuration.
    aws_region: str | None = None
    if "aws_region" in block:
        aws_region = _as_str(block["aws_region"], f"{where}.aws_region")
        if not aws_region:
            raise ValueError(f"{where}.aws_region is empty; leave the key out where there is no AWS region")
    # Reject empty Secret names before Kubernetes resource validation.
    secret_name: str | None = None
    if "secret_name" in block:
        secret_name = _as_str(block["secret_name"], f"{where}.secret_name")
        if not secret_name:
            raise ValueError(f"{where}.secret_name is empty; leave the key out where no property names a variable")
    return KubernetesConfig(
        context=_as_str(_required(block, "context", where), f"{where}.context"),
        namespace=_as_str(_required(block, "namespace", where), f"{where}.namespace"),
        harness_service_account=_as_str(
            _required(block, "harness_service_account", where), f"{where}.harness_service_account"
        ),
        flink_service_account=_as_str(
            _required(block, "flink_service_account", where), f"{where}.flink_service_account"
        ),
        spark_service_account=_SPARK_SERVICE_ACCOUNT
        if "spark_service_account" not in block
        else _as_str(block["spark_service_account"], f"{where}.spark_service_account"),
        service_account_annotations={}
        if "service_account_annotations" not in block
        else _as_string_map(block["service_account_annotations"], f"{where}.service_account_annotations"),
        registry=_as_str(_required(block, "registry", where), f"{where}.registry"),
        aws_region=aws_region,
        secret_name=secret_name,
        node_selector={}
        if "node_selector" not in block
        else _as_string_map(block["node_selector"], f"{where}.node_selector"),
        tolerations=[] if "tolerations" not in block else _as_string_maps(block["tolerations"], f"{where}.tolerations"),
    )


def _schema_registry_config(kafka: dict[str, object]) -> SchemaRegistryConfig | None:
    """Read the registry block; return ``None`` when absent or empty.

    Staging rejects Confluent-framed runs if no registry is configured.
    """
    where = "site.kafka.schema_registry"
    block = _block(kafka, "schema_registry", "site.kafka")
    if not block:
        return None
    _refuse_unknown(block, frozenset({"url", "basic_auth_user_info"}), where)
    user_info: str | None = None
    if "basic_auth_user_info" in block:
        user_info = _as_str(block["basic_auth_user_info"], f"{where}.basic_auth_user_info")
        if not user_info:
            raise ValueError(f"{where}.basic_auth_user_info is empty; leave the key out where the registry is open")
    return SchemaRegistryConfig(
        url=_as_str(_required(block, "url", where), f"{where}.url"), basic_auth_user_info=user_info
    )


def load_site(path: Path) -> SiteConfig:
    raw = _load_yaml(path, "site config")
    _refuse_placeholders(raw, "site")
    _refuse_unknown(raw, _SITE_KEYS, "site")

    kafka = _as_mapping(_required(raw, "kafka", "site"), "site.kafka")
    _refuse_unknown(kafka, frozenset({"deployment", "bootstrap_servers", "security", "schema_registry"}), "site.kafka")
    deployment: str | None = None
    if "deployment" in kafka:
        deployment = _as_str(kafka["deployment"], "site.kafka.deployment")
        if deployment not in ("managed", "in-cluster", "external"):
            raise ValueError("site.kafka.deployment must be managed, in-cluster, or external")
    security = {} if "security" not in kafka else _as_string_map(kafka["security"], "site.kafka.security")
    # Allow arbitrary client properties, except conflicting harness settings.
    refuse_mechanism_alias(security, "site.kafka.security")
    refuse_compression_props(security, "site.kafka.security")
    catalog = _as_mapping(_required(raw, "catalog", "site"), "site.catalog")
    _refuse_unknown(catalog, frozenset({"props"}), "site.catalog")
    pricing = _as_mapping(_required(raw, "pricing", "site"), "site.pricing")
    _refuse_unknown(pricing, frozenset({"vcpu_hour_usd", "gib_hour_usd"}), "site.pricing")

    catalog_props = _as_string_map(_required(catalog, "props", "site.catalog"), "site.catalog.props")
    kubernetes = _kubernetes_config(raw)
    registry = _schema_registry_config(kafka)
    # Cluster properties are rendered into ConfigMaps and uploaded to storage,
    # so require credential references. Local sites may use literal defaults.
    if kubernetes is not None:
        refuse_literal_secrets(security, "site.kafka.security")
        refuse_literal_secrets(catalog_props, "site.catalog.props")
        if registry is not None and registry.basic_auth_user_info is not None:
            refuse_literal_secrets(
                {"basic_auth_user_info": registry.basic_auth_user_info}, "site.kafka.schema_registry"
            )

    return SiteConfig(
        corpus_root=_as_str(_required(raw, "corpus_root", "site"), "site.corpus_root"),
        runs_root=_as_str(_required(raw, "runs_root", "site"), "site.runs_root"),
        warehouse=_as_str(_required(raw, "warehouse", "site"), "site.warehouse"),
        kafka_bootstrap=_as_str(_required(kafka, "bootstrap_servers", "site.kafka"), "site.kafka.bootstrap_servers"),
        kafka_security=security,
        kafka_deployment=deployment,
        schema_registry=registry,
        catalog_props=catalog_props,
        kubernetes=kubernetes,
        pricing_vcpu_hour_usd=_as_float(
            _required(pricing, "vcpu_hour_usd", "site.pricing"), "site.pricing.vcpu_hour_usd"
        ),
        pricing_gib_hour_usd=_as_float(_required(pricing, "gib_hour_usd", "site.pricing"), "site.pricing.gib_hour_usd"),
    )
