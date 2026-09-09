"""The run spec and the site config, as read off disk.

Both loaders refuse what they do not recognise: an unknown key is a typo, and
a typo in a spec is silent. A misspelled ``producer.shards`` would run one
shard, publish a spec claiming several, and the two would never disagree
loudly. Refusing costs an operator one error message and buys the guarantee
that the published spec is the run that happened.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml

from ingest_bench.specs import engines

# A run's name reaches a Kafka topic, an Iceberg table name and a Kubernetes
# object name, so it is restricted to what all three accept.
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{2,60}$")

EXTERNAL = "external"
HARNESS = "harness"
ENGINE_OWNED = "engine"

# The wire format a record's value carries. `avro` is the default because it is
# what the corpus publishes and what nothing has to be told; `confluent` is the
# same Avro binary behind the five-byte header of the Confluent wire format.
VALUE_ENCODING_AVRO = "avro"
VALUE_ENCODING_CONFLUENT = "confluent"
VALUE_ENCODINGS = frozenset({VALUE_ENCODING_AVRO, VALUE_ENCODING_CONFLUENT})

# The offsets, in seconds from the run's start, at which the scorer measures
# the table's file geometry. Every run reports the same ladder unless it says
# otherwise, so two runs' geometry columns line up.
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
        "node_selector",
        "tolerations",
    }
)

# The identity a Spark run's driver and executors run as. Defaulted rather than
# required, unlike the two beside it: a site written before Spark could be
# staged on a cluster names two accounts and not three, and `deploy/aws/setup.sh`
# creates this one under exactly this name.
_SPARK_SERVICE_ACCOUNT = "ingest-bench-spark"

_PLACEHOLDER = "YOUR_"

# The corpus's partition column, which every shipped schema carries under this
# name. It is the default partition source because a run that says nothing
# about layout should still be partitioned the way the corpus was designed to
# be read.
_PARTITION_DEFAULT_COLUMN = "partition_key"


# ---------------------------------------------------------------------------
# Reading YAML values
# ---------------------------------------------------------------------------


def _as_mapping(value: object, where: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a mapping, got {type(value).__name__}")
    return {str(key): entry for key, entry in cast(dict[object, object], value).items()}


def _as_str(value: object, where: str) -> str:
    # A YAML scalar is not coerced: `version: 0.0` reads as a float whose text
    # is not what was written, and `path-style-access: true` reads as a bool
    # whose `str()` is `True`, which no catalog accepts. Quoting is the fix,
    # and saying so is more useful than passing the wrong text along.
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
    """How the table under test comes into being, and how it is laid out.

    ``managed_by`` decides who runs the DDL: the harness creates the table so
    the run's properties are the ones the writer sees, and an engine that
    insists on creating its own is given the equivalent statement instead.
    """

    managed_by: str
    partition: str
    properties: dict[str, str]


@dataclass(frozen=True)
class KafkaSpec:
    """The topic's shape, which column becomes the message key, and the wire format.

    The key decides how records distribute across partitions, so it is part of
    the workload rather than of the engine: two engines are comparable only
    when they consumed the same skew.

    ``value_encoding`` is part of the workload for the same reason. It says
    what a value's bytes are: ``avro`` is the corpus's Avro binary as it
    stands, and ``confluent`` is the same bytes behind a five-byte header
    naming a registered schema — which is the only shape some engines read.
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


@dataclass(frozen=True)
class ScoringSpec:
    """What the scorer measures against, and what the in-flight gate allows.

    The two gate fields are absent unless a run says otherwise, so the gate
    keeps its own defaults rather than having them restated here — a default
    written down twice drifts, and the copy nobody rereads is the one that
    decides whether a run was abandoned.
    """

    freshness_bound_s: float
    warmup_s: int
    geometry_offsets_s: tuple[int, ...]
    gate_adaptation_s: int | None
    gate_window_s: int | None


@dataclass(frozen=True)
class FleetRole:
    """One role in the compute an engine was given, for the cost column.

    An external engine reports its own fleet because the harness never sees
    it; a managed one is sized by its knobs and the roles are derived.
    """

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
    _refuse_unknown(block, frozenset({"speed", "seconds", "shards", "behind_max_ms"}), "spec.producer")
    return ProducerSpec(
        speed=1.0 if "speed" not in block else _as_float(block["speed"], "spec.producer.speed"),
        seconds=None if "seconds" not in block else _as_int(block["seconds"], "spec.producer.seconds"),
        shards=1 if "shards" not in block else _as_int(block["shards"], "spec.producer.shards"),
        behind_max_ms=5000
        if "behind_max_ms" not in block
        else _as_int(block["behind_max_ms"], "spec.producer.behind_max_ms"),
    )


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
    return ScoringSpec(
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
    """The spec at ``path``, with its defaults filled in, or a refusal to read it.

    The engine block is carried through unread: its knobs belong to the engine
    that declares them, and validating them here would need the engine's own
    module — which a machine that only reads specs need not have installed.
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
    """The cluster a run's workloads are submitted to, and how they are placed on it.

    One namespace and one service account per role per site, not per run: a
    cloud grants an identity to a (namespace, service account) pair, and it is
    granted once by whoever stood the cluster up — so a run that invented its
    own namespace would have no credentials in it.

    ``aws_region`` is absent on a cluster that is not on AWS. Where it is set it
    reaches every pod as ``AWS_REGION``, which is what an AWS SDK reads when
    nothing else names a region for it.
    """

    context: str
    namespace: str
    harness_service_account: str
    flink_service_account: str
    spark_service_account: str
    service_account_annotations: dict[str, str]
    registry: str
    aws_region: str | None
    node_selector: dict[str, str]
    tolerations: list[dict[str, str]]


@dataclass(frozen=True)
class SchemaRegistryConfig:
    """The Confluent-API schema registry a `confluent` run registers with.

    Bring your own: the harness makes one POST against whatever this names, so
    a hosted registry, a self-managed one and the local stack's are the same
    thing to it. ``basic_auth_user_info`` is the ``user:password`` a hosted one
    authenticates with, and it stays as the site wrote it — a ``${env:NAME}``
    reference is resolved at the call that needs it, never at load.
    """

    url: str
    basic_auth_user_info: str | None


@dataclass(frozen=True)
class SiteConfig:
    """Where a run's storage, broker and catalog are, and what compute costs.

    Everything here is local to one operator, which is why it is not part of
    the spec: a published result carries the spec verbatim, and this file's
    bucket names and credentials never leave the machine that staged the run.
    """

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


def _refuse_placeholders(value: object, where: str) -> None:
    """Refuse the example file's placeholders wherever they survived a copy.

    An unedited placeholder otherwise reaches a bucket or a broker as a
    hostname, and the failure surfaces as a name-resolution error from inside
    whichever tool used it first rather than from the file that carries it.
    """
    if isinstance(value, str):
        if _PLACEHOLDER in value:
            raise ValueError(f"{where} still holds the {_PLACEHOLDER} placeholder: {value!r}")
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
    """The cluster block, or ``None`` where there is no cluster.

    An empty block is that answer rather than a missing one: a local run has a
    site config like any other, and it says so by declaring no cluster instead
    of by leaving a reader to guess whether the key was forgotten.
    """
    where = "site.kubernetes"
    block = _block(raw, "kubernetes", "site")
    if not block:
        return None
    _refuse_unknown(block, _KUBERNETES_KEYS, where)
    # An absent region is the answer for a cluster that is not on AWS. An empty
    # one is no answer at all: it would reach a pod as an `AWS_REGION` that no
    # SDK can resolve, which surfaces as a signing failure far from this file.
    aws_region: str | None = None
    if "aws_region" in block:
        aws_region = _as_str(block["aws_region"], f"{where}.aws_region")
        if not aws_region:
            raise ValueError(f"{where}.aws_region is empty; leave the key out where there is no AWS region")
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
        node_selector={}
        if "node_selector" not in block
        else _as_string_map(block["node_selector"], f"{where}.node_selector"),
        tolerations=[] if "tolerations" not in block else _as_string_maps(block["tolerations"], f"{where}.tolerations"),
    )


def _schema_registry_config(kafka: dict[str, object]) -> SchemaRegistryConfig | None:
    """The registry block, or ``None`` where the site declares none.

    Absent is the answer for a site whose runs are all raw Avro. A `confluent`
    run against such a site is refused at staging by name, which is a better
    error than a registration against an empty URL.
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
    """The site config at ``path``, or a refusal to read it."""
    raw = _load_yaml(path, "site config")
    _refuse_placeholders(raw, "site")
    _refuse_unknown(raw, _SITE_KEYS, "site")

    kafka = _as_mapping(_required(raw, "kafka", "site"), "site.kafka")
    _refuse_unknown(kafka, frozenset({"bootstrap_servers", "security", "schema_registry"}), "site.kafka")
    catalog = _as_mapping(_required(raw, "catalog", "site"), "site.catalog")
    _refuse_unknown(catalog, frozenset({"props"}), "site.catalog")
    pricing = _as_mapping(_required(raw, "pricing", "site"), "site.pricing")
    _refuse_unknown(pricing, frozenset({"vcpu_hour_usd", "gib_hour_usd"}), "site.pricing")

    return SiteConfig(
        corpus_root=_as_str(_required(raw, "corpus_root", "site"), "site.corpus_root"),
        runs_root=_as_str(_required(raw, "runs_root", "site"), "site.runs_root"),
        warehouse=_as_str(_required(raw, "warehouse", "site"), "site.warehouse"),
        kafka_bootstrap=_as_str(_required(kafka, "bootstrap_servers", "site.kafka"), "site.kafka.bootstrap_servers"),
        kafka_security={} if "security" not in kafka else _as_string_map(kafka["security"], "site.kafka.security"),
        schema_registry=_schema_registry_config(kafka),
        catalog_props=_as_string_map(_required(catalog, "props", "site.catalog"), "site.catalog.props"),
        kubernetes=_kubernetes_config(raw),
        pricing_vcpu_hour_usd=_as_float(
            _required(pricing, "vcpu_hour_usd", "site.pricing"), "site.pricing.vcpu_hour_usd"
        ),
        pricing_gib_hour_usd=_as_float(_required(pricing, "gib_hour_usd", "site.pricing"), "site.pricing.gib_hour_usd"),
    )
