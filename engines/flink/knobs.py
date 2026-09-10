# SPDX-License-Identifier: Apache-2.0
"""Validate Flink knobs and render the files needed for a run.

Render SQL, configuration, local fleet sizing, and Kubernetes manifests without
contacting a cluster. The engine uses released Flink connectors throughout.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import cast

import yaml

from engines.flink.script import join_statements
from ingest_bench import uri
from ingest_bench.catalog import table_identifier
from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.kafka_auth import MECHANISM_KEY, REGION_KEY
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.kubernetes import NAME, EngineKubernetes, object_name
from ingest_bench.specs.model import (
    VALUE_ENCODING_AVRO,
    VALUE_ENCODING_CONFLUENT,
    KubernetesConfig,
    RunSpec,
    SiteConfig,
)

# Names are local to the submitted script, so they need no run-specific suffix.
SOURCE_TABLE = "kafka_source"
CATALOG_NAME = "ice"

# Rendered files; the deployment and ConfigMap are only needed on Kubernetes.
SQL_FILE = "job.sql"
CONF_FILE = "flink-conf.yaml"
ENV_FILE = "flink.env"
FLINKDEPLOYMENT_FILE = "flinkdeployment.yaml"
CONFIGMAP_FILE = "flink-job-configmap.yaml"

NONE = "none"
HASH = "hash"
RANGE = "range"
DISTRIBUTION_MODES = frozenset({NONE, HASH, RANGE})

# Keep this repository name aligned with push-images.sh and AWS setup.sh.
IMAGE_REPOSITORY = "lakehouse-ingest-bench/flink"

# Pinned Flink version in the operator's label and Python driver jar formats.
_FLINK_VERSION_LABEL = "v1_20"
_PYFLINK_JAR = "local:///opt/flink/opt/flink-python-1.20.1.jar"
_PYTHON_DRIVER = "org.apache.flink.client.python.PythonDriver"
_JOB_SCRIPT = "/opt/bench/engines/flink/job.py"

# Mount beside the submitter; /run is reserved for container runtime files.
_RUN_MOUNT = "/opt/bench/run"
_JOB_VOLUME = "job"

# The operator merges this container name into its Flink container. Any other
# name would create a sidecar.
_FLINK_CONTAINER = "flink-main-container"

# The pinned PyFlink image requires amd64. Override conflicting site selectors.
_ARCH_PIN = {"kubernetes.io/arch": "amd64"}

REST = "rest"

# Operator-generated service names and pod labels, used to locate the run.
# Read image provenance from the JobManager, whose image is the engine image
# in every submission mode. Verification reads fleet settings from the job.
KUBERNETES = EngineKubernetes(
    kind="flinkdeployment",
    running_state="RUNNING",
    # Terminal jobs cannot become ready, even when they completed successfully.
    failed_states=("FAILED", "CANCELED", "FINISHED"),
    state_jsonpath="{.status.jobStatus.state}",
    # Rejected deployments may have no jobStatus. Treat reconciliation errors as
    # terminal only when the lifecycle is FAILED; other states may recover.
    error_jsonpath="{.status.error}",
    lifecycle_jsonpath="{.status.lifecycleState}",
    rest_service_suffix="-rest",
    rest_port=8081,
    log_target=f"deploy/{NAME}",
    provenance_selector=f"app={NAME},component=jobmanager",
    pods_selector="",
    document_file=FLINKDEPLOYMENT_FILE,
    configmap_file=CONFIGMAP_FILE,
    # The operator derives a Service name from this label and enforces a
    # 45-character DNS-1035 limit before creating resources.
    max_object_name_length=45,
)

# Reject unknown keys so misspelled tuning options cannot be silently ignored.
KNOBS: dict[str, type] = {
    "taskmanagers": int,
    "slots": int,
    "tm_cpu": float,
    "tm_mem_mb": int,
    "jm_cpu": float,
    "jm_mem_mb": int,
    "checkpoint_interval": str,
    "min_pause": str,
    "unaligned_checkpoints": bool,
    "source_parallelism": int,
    "max_parallelism": int,
    "distribution_mode": str,
    "machine_type": str,
    "extra_flink_conf": dict,
}

# Require explicit choices for the fleet size, memory, commit cadence, and
# write distribution: these are the benchmark's main comparison dimensions.
REQUIRED_KNOBS = frozenset({"taskmanagers", "slots", "tm_cpu", "tm_mem_mb", "checkpoint_interval", "distribution_mode"})

# Max parallelism is stored in checkpoints and cannot increase on restore.
# Leave room to expand the fleet without discarding state.
_MAX_PARALLELISM_FACTOR = 4

# Use a zoneless TIMESTAMP to match Iceberg, with millisecond precision to
# match the corpus. TIMESTAMP(3) also works with avro-confluent, whose legacy
# timestamp mapping cannot plan higher precision.
_DDL_TYPES = {
    "long": "BIGINT",
    "string": "STRING",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP(3)",
    "binary": "BYTES",
}

# Map supported wire encodings to source formats; reject others before staging.
_SOURCE_FORMATS = {
    VALUE_ENCODING_AVRO: "avro",
    VALUE_ENCODING_CONFLUENT: "avro-confluent",
}

# The non-legacy mapping preserves zoneless TIMESTAMP semantics without
# changing the encoded long. This option belongs to avro only; the confluent
# format rejects it.
_AVRO_OPTIONS: tuple[tuple[str, str], ...] = (("avro.timestamp_mapping.legacy", "false"),)

# Static options by encoding. Confluent registry options come from the site.
_FORMAT_OPTIONS: dict[str, tuple[tuple[str, str], ...]] = {
    VALUE_ENCODING_AVRO: _AVRO_OPTIONS,
    VALUE_ENCODING_CONFLUENT: (),
}

# Confluent decoding requires the registry to resolve schema IDs. Set the
# credentials source with user info or the format ignores those credentials.
_REGISTRY_URL_KEY = "avro-confluent.url"
_REGISTRY_USER_INFO_SOURCE = ("avro-confluent.basic-auth.credentials-source", "USER_INFO")
_REGISTRY_USER_INFO_KEY = "avro-confluent.basic-auth.user-info"

# Translate the harness's OAUTHBEARER + aws.region signal to Java MSK IAM
# authentication. The login module signs tokens using the pod's credentials.
_OAUTHBEARER = "OAUTHBEARER"
_MSK_IAM_PROPS: tuple[tuple[str, str], ...] = (
    ("security.protocol", "SASL_SSL"),
    (MECHANISM_KEY, "AWS_MSK_IAM"),
    ("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;"),
    ("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler"),
)

# Replace these keys to avoid duplicate options. Pass the region through
# AWS_REGION, since aws.region is a harness key, not a Kafka client option.
_MSK_IAM_REPLACED = frozenset({key for key, _ in _MSK_IAM_PROPS} | {REGION_KEY})

# Rename the differing PyIceberg S3 key; pass other properties through.
_CATALOG_PROP_RENAMES = {"s3.region": "client.region"}

# Select FileIO from the warehouse scheme. The default Hadoop fallback lacks
# the required storage configuration.
_FILE_IO_BY_SCHEME = {
    "s3://": "org.apache.iceberg.aws.s3.S3FileIO",
    "gs://": "org.apache.iceberg.gcp.gcs.GCSFileIO",
}

# Flink requires type=iceberg and catalog-type=rest; PyIceberg uses type=rest.
_PYICEBERG_TYPE = "type"

# Exclude properties already emitted by the catalog clause.
_STATED_CATALOG_PROPS = frozenset({_PYICEBERG_TYPE, "uri", "warehouse"})


# ---------------------------------------------------------------------------
# Reading the block
# ---------------------------------------------------------------------------


def _int_at(value: object, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} must be an integer, got {value!r}")
    return value


def _float_at(value: object, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{where} must be a number, got {value!r}")
    return float(value)


def _str_at(value: object, where: str) -> str:
    # Require strings for durations; an integer YAML scalar is not a Flink duration.
    if not isinstance(value, str):
        raise ValueError(f"{where} must be a string; quote it in the YAML. Got {value!r}")
    return value


def _bool_at(value: object, where: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{where} must be true or false, got {value!r}")
    return value


def _conf_at(value: object, where: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a mapping of Flink setting to value, got {value!r}")
    return {str(key): _str_at(entry, f"{where}.{key}") for key, entry in cast(dict[object, object], value).items()}


@dataclass(frozen=True)
class Knobs:
    """Flink sizing and tuning with defaults applied."""

    taskmanagers: int
    slots: int
    tm_cpu: float
    tm_mem_mb: int
    jm_cpu: float
    jm_mem_mb: int
    checkpoint_interval: str
    min_pause: str
    unaligned_checkpoints: bool
    source_parallelism: int
    max_parallelism: int
    distribution_mode: str
    machine_type: str | None
    extra_flink_conf: dict[str, str]

    def slots_total(self) -> int:
        """Return the total task slots available to writers."""
        return self.taskmanagers * self.slots

    def parallelism_default(self) -> int:
        """Return the default parallelism inherited by Kafka readers.

        The pinned connector has no source parallelism option. Set the job default
        to the reader count and use a sink hint to give writers the full fleet.
        """
        return self.source_parallelism


# Keep accepted keys aligned with the parsed dataclass so none are dropped.
if frozenset(KNOBS) != {field.name for field in fields(Knobs)}:
    raise ValueError(f"KNOBS declares {sorted(KNOBS)} and Knobs holds {sorted(field.name for field in fields(Knobs))}")


def read(block: dict[str, object]) -> Knobs:
    """Validate the knob block and apply explicit defaults."""
    unknown = sorted(set(block) - set(KNOBS))
    if unknown:
        raise ValueError(f"spec.flink has unknown keys {unknown}; supported keys are {sorted(KNOBS)}")
    missing = sorted(REQUIRED_KNOBS - set(block))
    if missing:
        raise ValueError(f"spec.flink must set {missing}")

    taskmanagers = _int_at(block["taskmanagers"], "spec.flink.taskmanagers")
    slots = _int_at(block["slots"], "spec.flink.slots")
    source_parallelism = (
        1
        if "source_parallelism" not in block
        else _int_at(block["source_parallelism"], "spec.flink.source_parallelism")
    )
    max_parallelism = (
        _MAX_PARALLELISM_FACTOR * taskmanagers * slots
        if "max_parallelism" not in block
        else _int_at(block["max_parallelism"], "spec.flink.max_parallelism")
    )
    # Parallelism and container counts must be positive to run a job.
    for name, count in (
        ("taskmanagers", taskmanagers),
        ("slots", slots),
        ("source_parallelism", source_parallelism),
        ("max_parallelism", max_parallelism),
    ):
        if count < 1:
            raise ValueError(f"spec.flink.{name} must be at least 1, got {count}")

    distribution_mode = _str_at(block["distribution_mode"], "spec.flink.distribution_mode")
    if distribution_mode not in DISTRIBUTION_MODES:
        raise ValueError(
            f"spec.flink.distribution_mode must be one of {sorted(DISTRIBUTION_MODES)}, got {distribution_mode!r}"
        )

    return Knobs(
        taskmanagers=taskmanagers,
        slots=slots,
        tm_cpu=_float_at(block["tm_cpu"], "spec.flink.tm_cpu"),
        tm_mem_mb=_int_at(block["tm_mem_mb"], "spec.flink.tm_mem_mb"),
        jm_cpu=1.0 if "jm_cpu" not in block else _float_at(block["jm_cpu"], "spec.flink.jm_cpu"),
        jm_mem_mb=2048 if "jm_mem_mb" not in block else _int_at(block["jm_mem_mb"], "spec.flink.jm_mem_mb"),
        checkpoint_interval=_str_at(block["checkpoint_interval"], "spec.flink.checkpoint_interval"),
        min_pause="0s" if "min_pause" not in block else _str_at(block["min_pause"], "spec.flink.min_pause"),
        unaligned_checkpoints=(
            False
            if "unaligned_checkpoints" not in block
            else _bool_at(block["unaligned_checkpoints"], "spec.flink.unaligned_checkpoints")
        ),
        source_parallelism=source_parallelism,
        max_parallelism=max_parallelism,
        distribution_mode=distribution_mode,
        machine_type=None if "machine_type" not in block else _str_at(block["machine_type"], "spec.flink.machine_type"),
        extra_flink_conf=(
            {}
            if "extra_flink_conf" not in block
            else _conf_at(block["extra_flink_conf"], "spec.flink.extra_flink_conf")
        ),
    )


def validate(block: dict[str, object], spec: RunSpec, meta: CorpusMetadata) -> None:
    """Validate fleet sizing and the run's wire encoding before staging.

    ``meta`` is unused but retained for the shared engine validator interface.
    Reject encodings without a matching Flink source format.
    """
    knobs = read(block)
    # Validate the encoding now; render its format at submission time.
    _source_format(spec)
    if knobs.source_parallelism > spec.kafka.partitions:
        raise ValueError(
            f"spec.flink.source_parallelism {knobs.source_parallelism} exceeds the topic's "
            f"{spec.kafka.partitions} partitions; each source reader needs a partition"
        )
    if knobs.slots_total() < knobs.source_parallelism:
        raise ValueError(
            f"spec.flink.source_parallelism {knobs.source_parallelism} exceeds available task slots: "
            f"{knobs.taskmanagers} taskmanagers with {knobs.slots} slots each provide {knobs.slots_total()}"
        )
    if knobs.max_parallelism < knobs.slots_total():
        raise ValueError(
            f"spec.flink.max_parallelism {knobs.max_parallelism} is below the fleet's {knobs.slots_total()} "
            "task slots, so the job could never occupy them"
        )


# ---------------------------------------------------------------------------
# Rendering SQL
# ---------------------------------------------------------------------------


def flink_ddl_type(iceberg_type: str) -> str:
    """Return the Flink SQL type for a corpus type."""
    if iceberg_type not in _DDL_TYPES:
        raise ValueError(f"type {iceberg_type!r} has no Flink SQL name here; the ones read are {sorted(_DDL_TYPES)}")
    return _DDL_TYPES[iceberg_type]


def _literal(value: str) -> str:
    """Quote ``value`` as a SQL string literal, doubling embedded quotes."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _with_clause(options: list[tuple[str, str]]) -> str:
    return ",\n".join(f"  {_literal(key)} = {_literal(value)}" for key, value in options)


def _column_ddl(name: str, meta: CorpusMetadata) -> str:
    """Declare a required source column matching the corpus's Avro encoding.

    Nullable Avro fields add a union branch index to the wire representation.
    NOT NULL keeps the DDL-derived reader schema compatible with the corpus.
    """
    published = meta.iceberg_types[name]
    if published not in _DDL_TYPES:
        raise ValueError(f"corpus column {name!r} publishes type {published!r}, which has no Flink SQL name")
    return f"  {name} {_DDL_TYPES[published]} NOT NULL"


def _is_msk_iam(security: dict[str, str]) -> bool:
    """Return whether Kafka properties request MSK IAM authentication."""
    return MECHANISM_KEY in security and security[MECHANISM_KEY] == _OAUTHBEARER and REGION_KEY in security


def _kafka_options(security: dict[str, str]) -> list[tuple[str, str]]:
    """Translate Kafka properties to source options, including MSK IAM settings.

    Preserve unrelated TLS and client options during authentication translation.
    """
    if not _is_msk_iam(security):
        return [(f"properties.{key}", value) for key, value in security.items()]
    carried = [(key, value) for key, value in security.items() if key not in _MSK_IAM_REPLACED]
    return [(f"properties.{key}", value) for key, value in (*_MSK_IAM_PROPS, *carried)]


def _source_format(spec: RunSpec) -> str:
    """Return the source format for the encoding, or reject an unsupported value."""
    if spec.kafka.value_encoding not in _SOURCE_FORMATS:
        raise ValueError(
            f"spec.kafka.value_encoding is {spec.kafka.value_encoding!r} and a Flink source reads "
            f"{sorted(_SOURCE_FORMATS)}"
        )
    return _SOURCE_FORMATS[spec.kafka.value_encoding]


def _format_options(spec: RunSpec, site: SiteConfig) -> list[tuple[str, str]]:
    """Return the validated source format and its decoding options."""
    encoding = spec.kafka.value_encoding
    options: list[tuple[str, str]] = [("format", _source_format(spec)), *_FORMAT_OPTIONS[encoding]]
    if encoding != VALUE_ENCODING_CONFLUENT:
        return options
    registry = site.schema_registry
    if registry is None:
        raise ValueError(
            f"spec.kafka.value_encoding {VALUE_ENCODING_CONFLUENT!r} requires kafka.schema_registry.url "
            "in the site configuration to resolve schema IDs"
        )
    options.append((_REGISTRY_URL_KEY, registry.url))
    # Keep environment placeholders intact for substitution inside the container.
    if registry.basic_auth_user_info is not None:
        options.append(_REGISTRY_USER_INFO_SOURCE)
        options.append((_REGISTRY_USER_INFO_KEY, registry.basic_auth_user_info))
    return options


def _source_ddl(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    columns = ",\n".join(_column_ddl(name, meta) for name in meta.field_names())
    options: list[tuple[str, str]] = [
        ("connector", "kafka"),
        ("topic", derived.topic),
        ("properties.bootstrap.servers", site.kafka_bootstrap),
        # Use the run ID to identify consumer groups left by abandoned runs.
        ("properties.group.id", derived.run_id),
        # Read from the beginning because records may arrive before the engine starts.
        ("scan.startup.mode", "earliest-offset"),
        *_kafka_options(site.kafka_security),
        *_format_options(spec, site),
    ]
    return f"CREATE TABLE {SOURCE_TABLE} (\n{columns}\n) WITH (\n{_with_clause(options)}\n)"


def _catalog_key(key: str) -> str:
    """Translate a PyIceberg property name to its Iceberg Java equivalent."""
    return _CATALOG_PROP_RENAMES[key] if key in _CATALOG_PROP_RENAMES else key


def _required_prop(props: dict[str, str], key: str) -> str:
    if key not in props:
        raise ValueError(f"site.catalog.props must set {key!r}: a Flink run addresses its table through it")
    return props[key]


def _file_io_for(warehouse: str) -> str | None:
    for scheme, implementation in _FILE_IO_BY_SCHEME.items():
        if warehouse.startswith(scheme):
            return implementation
    return None


def _catalog_ddl(site: SiteConfig) -> str:
    props = site.catalog_props
    if _PYICEBERG_TYPE in props and props[_PYICEBERG_TYPE] != REST:
        raise ValueError(
            f"Flink requires an Iceberg REST catalog; site.catalog.props specifies type {props[_PYICEBERG_TYPE]!r}"
        )
    options: list[tuple[str, str]] = [
        ("type", "iceberg"),
        ("catalog-type", REST),
        ("uri", _required_prop(props, "uri")),
        # A Glue REST warehouse identifies an account, not a storage location.
        ("warehouse", _required_prop(props, "warehouse")),
    ]
    # Sort properties for deterministic output and readable diffs.
    options += [(_catalog_key(key), props[key]) for key in sorted(set(props) - _STATED_CATALOG_PROPS)]
    # Use the storage URI; the catalog's warehouse property may be an account ID.
    file_io = _file_io_for(site.warehouse)
    if file_io is not None:
        options.append(("io-impl", file_io))
    return f"CREATE CATALOG {CATALOG_NAME} WITH (\n{_with_clause(options)}\n)"


def _insert(derived: Derived, meta: CorpusMetadata, knobs: Knobs) -> str:
    namespace, table = table_identifier(derived.table)
    hints: list[tuple[str, str]] = [("distribution-mode", knobs.distribution_mode)]
    if knobs.slots_total() != knobs.parallelism_default():
        # Override the reader default so writers use all fleet slots.
        hints.append(("write-parallelism", str(knobs.slots_total())))
    rendered = ", ".join(f"{_literal(key)} = {_literal(value)}" for key, value in hints)
    columns = ", ".join(meta.field_names())
    return (
        f"INSERT INTO {CATALOG_NAME}.`{namespace}`.`{table}` /*+ OPTIONS({rendered}) */\n"
        f"SELECT {columns} FROM {SOURCE_TABLE}"
    )


def render_sql(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """Render the source, catalog, and INSERT statements."""
    knobs = read(spec.engine_block)
    return join_statements((_source_ddl(spec, site, derived, meta), _catalog_ddl(site), _insert(derived, meta, knobs)))


# ---------------------------------------------------------------------------
# Rendering configuration
# ---------------------------------------------------------------------------


def _flag(value: bool) -> str:
    return "true" if value else "false"


def render_conf(spec: RunSpec, derived: Derived) -> dict[str, str]:
    """Render Flink job settings.

    Fleet sizing also appears in ``flink.env`` because Compose starts containers
    before the job is submitted.
    """
    knobs = read(spec.engine_block)
    conf = {
        "execution.checkpointing.interval": knobs.checkpoint_interval,
        "execution.checkpointing.min-pause": knobs.min_pause,
        "execution.checkpointing.unaligned.enabled": _flag(knobs.unaligned_checkpoints),
        # Exactly-once mode is required by the benchmark's duplication metric.
        "execution.checkpointing.mode": "EXACTLY_ONCE",
        "parallelism.default": str(knobs.parallelism_default()),
        "pipeline.max-parallelism": str(knobs.max_parallelism),
        "taskmanager.numberOfTaskSlots": str(knobs.slots),
        "taskmanager.memory.process.size": f"{knobs.tm_mem_mb}m",
        "jobmanager.memory.process.size": f"{knobs.jm_mem_mb}m",
        # Use the run ID to identify the job in listings.
        "pipeline.name": derived.run_id,
    }
    # Apply explicit overrides last.
    conf.update(knobs.extra_flink_conf)
    return conf


def _conf_yaml(spec: RunSpec, derived: Derived) -> str:
    """Serialize the submitter's settings as YAML."""
    return yaml.safe_dump(render_conf(spec, derived), sort_keys=True, default_flow_style=False)


# ---------------------------------------------------------------------------
# Rendering the Kubernetes documents
# ---------------------------------------------------------------------------


def _cluster(site: SiteConfig) -> KubernetesConfig:
    if site.kubernetes is None:
        raise ValueError("Flink on Kubernetes requires site.kubernetes; no cluster is configured")
    return site.kubernetes


def kubernetes_name(run_id: str) -> str:
    """Convert the run ID using the shared Kubernetes naming rules."""
    return object_name(run_id)


def configmap_name(derived: Derived) -> str:
    """Return the name of the ConfigMap containing the run's files."""
    return f"{kubernetes_name(derived.run_id)}-flink-job"


def render_flinkdeployment(
    spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata, image_tag: str
) -> str:
    """Render the run's FlinkDeployment.

    ``meta`` is unused but retained for the shared Kubernetes renderer interface.
    """
    knobs = read(spec.engine_block)
    cluster = _cluster(site)
    conf = {
        # Keep checkpoints under the run for cleanup; allow extra_flink_conf to
        # override this default.
        "state.checkpoints.dir": uri.join(site.runs_root, derived.run_id, "checkpoints"),
        **render_conf(spec, derived),
    }
    container: dict[str, object] = {
        "name": _FLINK_CONTAINER,
        "volumeMounts": [{"name": _JOB_VOLUME, "mountPath": _RUN_MOUNT, "readOnly": True}],
    }
    if cluster.aws_region is not None:
        # Java SDKs read AWS_REGION; botocore reads AWS_DEFAULT_REGION. Supply both
        # for broker authentication and S3 access.
        container["env"] = [
            {"name": name, "value": cluster.aws_region} for name in ("AWS_REGION", "AWS_DEFAULT_REGION")
        ]
    if cluster.secret_name is not None:
        # Resolve rendered environment references from the Secret. ConfigMaps and
        # archived run files retain only the references.
        container["envFrom"] = [{"secretRef": {"name": cluster.secret_name}}]
    pod_spec: dict[str, object] = {
        "nodeSelector": {**cluster.node_selector, **_ARCH_PIN},
        "tolerations": cluster.tolerations,
        "volumes": [{"name": _JOB_VOLUME, "configMap": {"name": configmap_name(derived)}}],
        "containers": [container],
    }
    document: dict[str, object] = {
        "apiVersion": "flink.apache.org/v1beta1",
        "kind": "FlinkDeployment",
        "metadata": {"name": kubernetes_name(derived.run_id), "namespace": cluster.namespace},
        "spec": {
            "image": f"{cluster.registry}/{IMAGE_REPOSITORY}:{image_tag}",
            "flinkVersion": _FLINK_VERSION_LABEL,
            # Standalone mode honors taskmanagers; native mode derives the fleet from
            # job parallelism.
            "mode": "standalone",
            "serviceAccount": cluster.flink_service_account,
            "flinkConfiguration": conf,
            # CRD fields override flinkConfiguration, so read these values from the
            # effective configuration to preserve extra_flink_conf overrides. Replica
            # count, slots, and CPU have no competing settings here.
            "jobManager": {"resource": {"memory": conf["jobmanager.memory.process.size"], "cpu": knobs.jm_cpu}},
            "taskManager": {
                "resource": {"memory": conf["taskmanager.memory.process.size"], "cpu": knobs.tm_cpu},
                "replicas": knobs.taskmanagers,
            },
            "job": {
                # Use PyFlink's bundled driver; each run supplies a script, not a custom jar.
                "jarURI": _PYFLINK_JAR,
                "entryClass": _PYTHON_DRIVER,
                "args": [
                    "-py",
                    _JOB_SCRIPT,
                    "--sql",
                    f"{_RUN_MOUNT}/{SQL_FILE}",
                    "--conf",
                    f"{_RUN_MOUNT}/{CONF_FILE}",
                ],
                "parallelism": int(conf["parallelism.default"]),
                # Runs are scored once, so object updates do not restore prior state.
                "upgradeMode": "stateless",
                "state": "running",
            },
            "podTemplate": {"apiVersion": "v1", "kind": "Pod", "spec": pod_spec},
        },
    }
    return yaml.safe_dump(document, sort_keys=False)


def render_job_configmap(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """Render the ConfigMap containing SQL and Flink settings."""
    cluster = _cluster(site)
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": configmap_name(derived), "namespace": cluster.namespace},
        "data": {SQL_FILE: render_sql(spec, site, derived, meta), CONF_FILE: _conf_yaml(spec, derived)},
    }
    return yaml.safe_dump(document, sort_keys=False)


# ---------------------------------------------------------------------------
# The run directory
# ---------------------------------------------------------------------------


def render(
    spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata, *, image_tag: str | None = None
) -> dict[str, str]:
    """Return rendered run files keyed by filename.

    Kubernetes sites also receive deployment manifests and require an image tag.
    The local stack builds and submits its own image.
    """
    knobs = read(spec.engine_block)
    files = {
        SQL_FILE: render_sql(spec, site, derived, meta),
        CONF_FILE: _conf_yaml(spec, derived),
        # Compose needs fleet sizing before submission, so provide it as environment.
        ENV_FILE: (
            f"TASKMANAGERS={knobs.taskmanagers}\n"
            f"SLOTS={knobs.slots}\n"
            f"TM_MEM_MB={knobs.tm_mem_mb}\n"
            f"JM_MEM_MB={knobs.jm_mem_mb}\n"
        ),
    }
    if site.kubernetes is None:
        return files
    if image_tag is None:
        raise ValueError("render requires image_tag for Kubernetes runs; use the tag of the published image")
    files[FLINKDEPLOYMENT_FILE] = render_flinkdeployment(spec, site, derived, meta, image_tag)
    files[CONFIGMAP_FILE] = render_job_configmap(spec, site, derived, meta)
    return files
