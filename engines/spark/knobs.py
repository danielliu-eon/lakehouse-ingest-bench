# SPDX-License-Identifier: Apache-2.0
"""Validate Spark knobs and render the files needed for a run.

Render properties, the Avro reader schema, the job document, local fleet sizing,
and Kubernetes manifests without contacting a cluster. The engine uses released
Spark connectors throughout.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, fields
from typing import cast

import yaml

from engines.spark.stream_to_iceberg import JOB_DOCUMENT, READER_SCHEMA, RUN_DIR, VALUE_EXPRESSIONS
from ingest_bench import uri
from ingest_bench.catalog import table_identifier
from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.kafka_auth import MECHANISM_KEY, REGION_KEY
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.env import PLACEHOLDER_FORM, has_placeholder
from ingest_bench.specs.kubernetes import NAME, EngineKubernetes, object_name
from ingest_bench.specs.model import KubernetesConfig, RunSpec, SiteConfig

# This catalog name is local to the rendered files.
CATALOG_NAME = "ice"

# Derive job filenames from the reader's paths. Keep submission filenames
# aligned with Compose.
CONF_FILE = "spark-defaults.conf"
ENV_FILE = "job.env"
SCHEMA_FILE = READER_SCHEMA.name
JOB_FILE = JOB_DOCUMENT.name

# Kubernetes-only manifests; Compose submits the local job directly.
SPARKAPPLICATION_FILE = "sparkapplication.yaml"
CONFIGMAP_FILE = "spark-job-configmap.yaml"

# Environment variable names consumed by Compose's submission command.
LOCAL_CORES_VAR = "LOCAL_CORES"
DRIVER_MEM_VAR = "DRIVER_MEM_MB"

NONE = "none"
HASH = "hash"
RANGE = "range"
DISTRIBUTION_MODES = frozenset({NONE, HASH, RANGE})

REST = "rest"

# Keep the image repository aligned with push-images.sh and AWS setup.sh,
# and the Spark version aligned with the Dockerfile.
IMAGE_REPOSITORY = "lakehouse-ingest-bench/spark"
SPARK_VERSION = "3.5.9"

# local:// points to the script bundled in the image; the operator need not
# stage a separate file.
_JOB_SCRIPT = "local:///opt/bench/engines/spark/stream_to_iceberg.py"

# Use the job's mount path so it can find the rendered files.
_RUN_MOUNT = str(RUN_DIR)
_JOB_VOLUME = "job"

# Operator-generated names and labels used to locate the driver UI and pods.
# Select the whole fleet to verify executor count and Guaranteed QoS.
KUBERNETES = EngineKubernetes(
    kind="sparkapplication",
    running_state="RUNNING",
    # These terminal states cannot become ready, including a streaming job that
    # completed before measurement began.
    failed_states=("FAILED", "SUBMISSION_FAILED", "FAILING", "COMPLETED", "SUCCEEDING"),
    state_jsonpath="{.status.applicationState.state}",
    # The same state field covers submission and application failures; the
    # error field provides the operator's explanation.
    error_jsonpath="{.status.applicationState.errorMessage}",
    lifecycle_jsonpath="{.status.applicationState.state}",
    rest_service_suffix="-ui-svc",
    rest_port=4040,
    log_target=f"pod/{NAME}-driver",
    provenance_selector=f"spark-role=driver,sparkoperator.k8s.io/app-name={NAME}",
    pods_selector=f"sparkoperator.k8s.io/app-name={NAME}",
    document_file=SPARKAPPLICATION_FILE,
    configmap_file=CONFIGMAP_FILE,
)

# ProcessingTime requires a count, space, and full unit word. Validate before
# staging so abbreviations such as 10s or malformed 10seconds fail early.
_TRIGGER_RE = re.compile(r"^\d+\s+(millisecond|milliseconds|second|seconds|minute|minutes|hour|hours)$")

# Bind the catalog name to Iceberg's SparkCatalog implementation.
_SPARK_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"

# Enable Iceberg SQL extensions and sink options.
_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

# The local timestamp annotation preserves the encoded long while making
# from_avro return TimestampNTZ, matching Iceberg's zoneless timestamp.
_TIMESTAMP_MILLIS = "timestamp-millis"
_LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis"

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
# AWS_REGION rather than the harness-only aws.region key.
_MSK_IAM_REPLACED = frozenset({key for key, _ in _MSK_IAM_PROPS} | {REGION_KEY})

# Rename the differing PyIceberg S3 key; pass other properties through.
_CATALOG_PROP_RENAMES = {"s3.region": "client.region"}

# Select FileIO from the warehouse scheme; the Hadoop fallback is not
# configured for catalog reads.
_FILE_IO_BY_SCHEME = {
    "s3://": "org.apache.iceberg.aws.s3.S3FileIO",
    "gs://": "org.apache.iceberg.gcp.gcs.GCSFileIO",
}

# Spark's catalog type selects the backend for the catalog class.
_PYICEBERG_TYPE = "type"

# Exclude properties already emitted by the catalog block.
_STATED_CATALOG_PROPS = frozenset({_PYICEBERG_TYPE, "uri", "warehouse"})

# Stock Spark registers Hadoop S3 storage under the s3a scheme.
_S3_SCHEME = "s3://"
_S3A_SCHEME = "s3a://"

# Translate local Iceberg storage properties to Hadoop S3A settings.
# Kubernetes runs use pod identity instead of static keys.
_S3A_FROM_CATALOG_PROP = (
    ("s3.endpoint", "spark.hadoop.fs.s3a.endpoint"),
    ("s3.path-style-access", "spark.hadoop.fs.s3a.path.style.access"),
    ("s3.access-key-id", "spark.hadoop.fs.s3a.access.key"),
    ("s3.secret-access-key", "spark.hadoop.fs.s3a.secret.key"),
)

# Reject unknown keys so misspelled tuning options cannot be silently ignored.
KNOBS: dict[str, type] = {
    "executors": int,
    "executor_cores": int,
    "executor_mem_mb": int,
    "driver_cores": int,
    "driver_mem_mb": int,
    "trigger_interval": str,
    "max_offsets_per_trigger": int,
    "distribution_mode": str,
    "fanout": bool,
    "machine_type": str,
    "extra_spark_conf": dict,
}

# Require explicit fleet sizing, memory, commit cadence, and write distribution.
REQUIRED_KNOBS = frozenset({"executors", "executor_cores", "executor_mem_mb", "trigger_interval", "distribution_mode"})


# ---------------------------------------------------------------------------
# Reading the block
# ---------------------------------------------------------------------------


def _int_at(value: object, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} must be an integer, got {value!r}")
    return value


def _str_at(value: object, where: str) -> str:
    # Require a string interval; an integer YAML scalar is not a Spark interval.
    if not isinstance(value, str):
        raise ValueError(f"{where} must be a string; quote it in the YAML. Got {value!r}")
    return value


def _bool_at(value: object, where: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{where} must be true or false, got {value!r}")
    return value


def _conf_at(value: object, where: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a mapping of Spark setting to value, got {value!r}")
    return {str(key): _str_at(entry, f"{where}.{key}") for key, entry in cast(dict[object, object], value).items()}


@dataclass(frozen=True)
class Knobs:
    """Spark sizing and tuning with defaults applied."""

    executors: int
    executor_cores: int
    executor_mem_mb: int
    driver_cores: int
    driver_mem_mb: int
    trigger_interval: str
    max_offsets_per_trigger: int | None
    distribution_mode: str
    fanout: bool
    machine_type: str | None
    extra_spark_conf: dict[str, str]

    def cores_total(self) -> int:
        """Return the total executor cores available to writers."""
        return self.executors * self.executor_cores


# Keep accepted keys aligned with the parsed dataclass so none are dropped.
if frozenset(KNOBS) != {field.name for field in fields(Knobs)}:
    raise ValueError(f"KNOBS declares {sorted(KNOBS)} and Knobs holds {sorted(field.name for field in fields(Knobs))}")


def read(block: dict[str, object]) -> Knobs:
    """Validate the knob block and apply explicit defaults."""
    unknown = sorted(set(block) - set(KNOBS))
    if unknown:
        raise ValueError(f"spec.spark has unknown keys {unknown}; the ones it takes are {sorted(KNOBS)}")
    missing = sorted(REQUIRED_KNOBS - set(block))
    if missing:
        raise ValueError(f"spec.spark must set {missing}")

    executors = _int_at(block["executors"], "spec.spark.executors")
    executor_cores = _int_at(block["executor_cores"], "spec.spark.executor_cores")
    driver_cores = 1 if "driver_cores" not in block else _int_at(block["driver_cores"], "spec.spark.driver_cores")
    # Core and container counts must be positive to run a job.
    for name, count in (("executors", executors), ("executor_cores", executor_cores), ("driver_cores", driver_cores)):
        if count < 1:
            raise ValueError(f"spec.spark.{name} must be at least 1, got {count}")

    trigger_interval = _str_at(block["trigger_interval"], "spec.spark.trigger_interval")
    if _TRIGGER_RE.match(trigger_interval) is None:
        raise ValueError(
            f"spec.spark.trigger_interval must be a Spark interval like '10 seconds' matching "
            f"{_TRIGGER_RE.pattern}, got {trigger_interval!r}"
        )

    distribution_mode = _str_at(block["distribution_mode"], "spec.spark.distribution_mode")
    if distribution_mode not in DISTRIBUTION_MODES:
        raise ValueError(
            f"spec.spark.distribution_mode must be one of {sorted(DISTRIBUTION_MODES)}, got {distribution_mode!r}"
        )

    # None permits all available offsets; a zero limit would allow no records.
    max_offsets_per_trigger: int | None = None
    if "max_offsets_per_trigger" in block:
        max_offsets_per_trigger = _int_at(block["max_offsets_per_trigger"], "spec.spark.max_offsets_per_trigger")
        if max_offsets_per_trigger < 1:
            raise ValueError(f"spec.spark.max_offsets_per_trigger must be at least 1, got {max_offsets_per_trigger}")

    return Knobs(
        executors=executors,
        executor_cores=executor_cores,
        executor_mem_mb=_int_at(block["executor_mem_mb"], "spec.spark.executor_mem_mb"),
        driver_cores=driver_cores,
        driver_mem_mb=2048
        if "driver_mem_mb" not in block
        else _int_at(block["driver_mem_mb"], "spec.spark.driver_mem_mb"),
        trigger_interval=trigger_interval,
        max_offsets_per_trigger=max_offsets_per_trigger,
        distribution_mode=distribution_mode,
        fanout=False if "fanout" not in block else _bool_at(block["fanout"], "spec.spark.fanout"),
        machine_type=None if "machine_type" not in block else _str_at(block["machine_type"], "spec.spark.machine_type"),
        extra_spark_conf=(
            {}
            if "extra_spark_conf" not in block
            else _conf_at(block["extra_spark_conf"], "spec.spark.extra_spark_conf")
        ),
    )


def validate(block: dict[str, object], spec: RunSpec, meta: CorpusMetadata) -> None:
    """Validate fleet sizing and the run's wire encoding before staging.

    ``meta`` is unused but retained for the shared engine validator interface.
    Spark allows more cores than topic partitions; excess reader capacity is idle.
    """
    read(block)
    if spec.kafka.value_encoding not in VALUE_EXPRESSIONS:
        raise ValueError(
            f"spec.kafka.value_encoding is {spec.kafka.value_encoding!r} and a Spark run decodes "
            f"{sorted(VALUE_EXPRESSIONS)}"
        )


# ---------------------------------------------------------------------------
# Rendering the properties
# ---------------------------------------------------------------------------


def _catalog_key(key: str) -> str:
    """Translate a PyIceberg property name to its Iceberg Java equivalent."""
    return _CATALOG_PROP_RENAMES[key] if key in _CATALOG_PROP_RENAMES else key


def _required_prop(props: dict[str, str], key: str, why: str) -> str:
    if key not in props:
        raise ValueError(f"site.catalog.props must set {key!r}: {why}")
    return props[key]


def _file_io_for(warehouse: str) -> str | None:
    for scheme, implementation in _FILE_IO_BY_SCHEME.items():
        if warehouse.startswith(scheme):
            return implementation
    return None


def _catalog_conf(site: SiteConfig) -> dict[str, str]:
    """Render catalog settings under spark.sql.catalog.<name>."""
    props = site.catalog_props
    if _PYICEBERG_TYPE in props and props[_PYICEBERG_TYPE] != REST:
        raise ValueError(
            f"a Spark run reads its table through an Iceberg REST catalog, and site.catalog.props names catalog "
            f"type {props[_PYICEBERG_TYPE]!r}"
        )
    prefix = f"spark.sql.catalog.{CATALOG_NAME}"
    conf = {
        prefix: _SPARK_CATALOG_CLASS,
        f"{prefix}.{_PYICEBERG_TYPE}": REST,
        f"{prefix}.uri": _required_prop(props, "uri", "a Spark run addresses its table through it"),
        # A Glue REST warehouse identifies an account, not a storage location.
        f"{prefix}.warehouse": _required_prop(props, "warehouse", "the catalog resolves a table under it"),
    }
    # Use the storage URI; the catalog's warehouse property may be an account ID.
    file_io = _file_io_for(site.warehouse)
    if file_io is not None:
        conf[f"{prefix}.io-impl"] = file_io
    # Sort properties for deterministic output and readable diffs.
    for key in sorted(set(props) - _STATED_CATALOG_PROPS):
        conf[f"{prefix}.{_catalog_key(key)}"] = props[key]
    return conf


def checkpoint_uri(site: SiteConfig, derived: Derived) -> str:
    """Return the per-run checkpoint URI, using s3a:// for Hadoop S3 storage."""
    location = uri.join(site.runs_root, derived.run_id, "checkpoints")
    if location.startswith(_S3_SCHEME):
        return _S3A_SCHEME + location[len(_S3_SCHEME) :]
    return location


def _local_store_conf(site: SiteConfig, derived: Derived) -> dict[str, str]:
    """Derive local Hadoop S3A settings from Iceberg storage properties.

    Checkpoints use Hadoop while table files use Iceberg FileIO. Reuse the site's
    properties to configure both consistently; non-S3 checkpoints need no S3A keys.
    """
    if not checkpoint_uri(site, derived).startswith(_S3A_SCHEME):
        return {}
    return {
        setting: _required_prop(site.catalog_props, prop, "a run with no cluster reaches its checkpoints through it")
        for prop, setting in _S3A_FROM_CATALOG_PROP
    }


def render_conf(spec: RunSpec, site: SiteConfig, derived: Derived) -> dict[str, str]:
    """Render the Spark settings submitted with the job."""
    knobs = read(spec.engine_block)
    conf = {
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        **_catalog_conf(site),
        "spark.executor.cores": str(knobs.executor_cores),
        "spark.executor.memory": f"{knobs.executor_mem_mb}m",
        "spark.driver.cores": str(knobs.driver_cores),
        "spark.driver.memory": f"{knobs.driver_mem_mb}m",
        # Use one shuffle partition per executor core to avoid Spark's default
        # 200-way shuffle and the resulting small files on smaller fleets.
        "spark.sql.shuffle.partitions": str(knobs.cores_total()),
        # Use the run ID to identify the application in listings.
        "spark.app.name": derived.run_id,
    }
    if site.kubernetes is None:
        conf.update(_local_store_conf(site, derived))
    # Apply explicit overrides last.
    conf.update(knobs.extra_spark_conf)
    # Spark reads these settings directly and cannot resolve environment
    # placeholders. Kafka options in job.json are resolved by the job instead.
    referenced = sorted(key for key, value in conf.items() if has_placeholder(value))
    if referenced:
        raise ValueError(
            f"a Spark run renders {referenced} as Spark settings, and nothing resolves a "
            f"{PLACEHOLDER_FORM} in one; a credential the job can resolve goes in site.kafka.security"
        )
    return conf


def render_conf_file(spec: RunSpec, site: SiteConfig, derived: Derived) -> str:
    """Serialize settings for spark-submit --properties-file."""
    return "".join(f"{key} {value}\n" for key, value in render_conf(spec, site, derived).items())


def render_env(spec: RunSpec) -> str:
    """Render the local submission environment.

    Compose needs the core count and driver heap before the Spark session starts.
    """
    knobs = read(spec.engine_block)
    return f"{LOCAL_CORES_VAR}={knobs.cores_total()}\n{DRIVER_MEM_VAR}={knobs.driver_mem_mb}\n"


# ---------------------------------------------------------------------------
# Rendering the job's documents
# ---------------------------------------------------------------------------


def _is_msk_iam(security: dict[str, str]) -> bool:
    """Return whether Kafka properties request MSK IAM authentication."""
    return MECHANISM_KEY in security and security[MECHANISM_KEY] == _OAUTHBEARER and REGION_KEY in security


def kafka_options(security: dict[str, str]) -> dict[str, str]:
    """Translate Kafka properties to source options, including MSK IAM settings.

    Preserve unrelated TLS and client options during authentication translation.
    """
    if not _is_msk_iam(security):
        return {f"kafka.{key}": value for key, value in security.items()}
    carried = [(key, value) for key, value in security.items() if key not in _MSK_IAM_REPLACED]
    return {f"kafka.{key}": value for key, value in (*_MSK_IAM_PROPS, *carried)}


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _zoneless(node: object) -> object:
    """Return ``node`` with millisecond timestamps annotated as local timestamps."""
    if isinstance(node, dict):
        return {
            key: (_LOCAL_TIMESTAMP_MILLIS if key == "logicalType" and value == _TIMESTAMP_MILLIS else _zoneless(value))
            for key, value in cast(dict[str, object], node).items()
        }
    if isinstance(node, list):
        return [_zoneless(entry) for entry in cast(list[object], node)]
    return node


def render_reader_schema(meta: CorpusMetadata) -> str:
    """Render the Avro reader schema passed to from_avro."""
    return json.dumps(_zoneless(meta.schema), indent=2) + "\n"


def render_job(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """Render the job's source, sink, and trigger configuration."""
    knobs = read(spec.engine_block)
    namespace, table = table_identifier(derived.table)
    document = {
        "topic": derived.topic,
        "bootstrap": site.kafka_bootstrap,
        "group_id": derived.run_id,
        # Use the same encoding selected for the producer and schema registration.
        "value_encoding": spec.kafka.value_encoding,
        "table": f"{CATALOG_NAME}.{namespace}.{table}",
        "columns": meta.field_names(),
        "kafka_options": kafka_options(site.kafka_security),
        "write_options": {
            "distribution-mode": knobs.distribution_mode,
            "fanout-enabled": _flag(knobs.fanout),
            # from_avro marks fields nullable regardless of the schema. Skip Iceberg's
            # static nullability comparison; Spark still enforces required columns with
            # AssertNotNull at runtime.
            "check-nullability": "false",
            # Set a stable per-run writer checkpoint path. The session-level setting
            # is a parent directory and gives unnamed queries random subdirectories,
            # which would replay committed rows after a restart.
            "checkpointLocation": checkpoint_uri(site, derived),
        },
        "trigger_interval": knobs.trigger_interval,
        "max_offsets_per_trigger": knobs.max_offsets_per_trigger,
    }
    return json.dumps(document, indent=2) + "\n"


# ---------------------------------------------------------------------------
# Rendering the Kubernetes documents
# ---------------------------------------------------------------------------


def _cluster(site: SiteConfig) -> KubernetesConfig:
    if site.kubernetes is None:
        raise ValueError("a Spark run on Kubernetes is placed by site.kubernetes, and the site declares no cluster")
    return site.kubernetes


def kubernetes_name(run_id: str) -> str:
    """Convert the run ID using the shared Kubernetes naming rules."""
    return object_name(run_id)


def configmap_name(derived: Derived) -> str:
    """Return the name of the ConfigMap containing the run's files."""
    return f"{kubernetes_name(derived.run_id)}-spark-job"


def _mount() -> list[dict[str, object]]:
    """Return a fresh mount specification to avoid YAML anchors between pods."""
    return [{"name": _JOB_VOLUME, "mountPath": _RUN_MOUNT, "readOnly": True}]


def _placement(cluster: KubernetesConfig) -> dict[str, object]:
    """Return a fresh placement specification to avoid YAML anchors between pods."""
    return {
        "nodeSelector": dict(cluster.node_selector),
        "tolerations": [dict(toleration) for toleration in cluster.tolerations],
    }


def _region_env(cluster: KubernetesConfig) -> list[dict[str, str]]:
    """Return a fresh region environment for a pod.

    Java SDKs read AWS_REGION; botocore reads AWS_DEFAULT_REGION. Supply both.
    """
    if cluster.aws_region is None:
        return []
    return [{"name": name, "value": cluster.aws_region} for name in ("AWS_REGION", "AWS_DEFAULT_REGION")]


def _secret_env_from(cluster: KubernetesConfig) -> list[dict[str, object]]:
    """Return a fresh reference to the fleet's environment Secret.

    Both driver and executors receive it. Rendered files retain environment
    placeholders; the Secret supplies their values inside the pods.
    """
    if cluster.secret_name is None:
        return []
    return [{"secretRef": {"name": cluster.secret_name}}]


def render_sparkapplication(
    spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata, image_tag: str
) -> str:
    """Render the run's SparkApplication.

    ``meta`` is retained for the shared Kubernetes renderer interface.
    Match CPU requests and limits for Guaranteed QoS; Spark also matches memory
    requests and limits.
    """
    knobs = read(spec.engine_block)
    cluster = _cluster(site)
    driver: dict[str, object] = {
        "cores": knobs.driver_cores,
        "coreLimit": str(knobs.driver_cores),
        "memory": f"{knobs.driver_mem_mb}m",
        # The driver creates executors; this service account supplies fleet identity.
        "serviceAccount": cluster.spark_service_account,
        "volumeMounts": _mount(),
        **_placement(cluster),
    }
    executor: dict[str, object] = {
        "instances": knobs.executors,
        "cores": knobs.executor_cores,
        "coreLimit": str(knobs.executor_cores),
        "memory": f"{knobs.executor_mem_mb}m",
        "volumeMounts": _mount(),
        **_placement(cluster),
    }
    # Both driver and executors need the region for broker and storage access.
    if cluster.aws_region is not None:
        driver["env"] = _region_env(cluster)
        executor["env"] = _region_env(cluster)
    if cluster.secret_name is not None:
        driver["envFrom"] = _secret_env_from(cluster)
        executor["envFrom"] = _secret_env_from(cluster)
    document: dict[str, object] = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": kubernetes_name(derived.run_id), "namespace": cluster.namespace},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            # Run the driver as a pod so all charged compute belongs to the cluster.
            "mode": "cluster",
            "image": f"{cluster.registry}/{IMAGE_REPOSITORY}:{image_tag}",
            # Commit tags are immutable, so cached images can be reused.
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": _JOB_SCRIPT,
            "sparkVersion": SPARK_VERSION,
            # Do not restart a scored run: a second attempt is outside its recorded result.
            "restartPolicy": {"type": "Never"},
            "sparkConf": render_conf(spec, site, derived),
            "driver": driver,
            "executor": executor,
            "volumes": [{"name": _JOB_VOLUME, "configMap": {"name": configmap_name(derived)}}],
        },
    }
    return yaml.safe_dump(document, sort_keys=False)


def render_job_configmap(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """Render the ConfigMap containing all four run files.

    The job reads two files. Include properties and local environment settings
    as well so the complete rendered configuration is available in the pod.
    """
    cluster = _cluster(site)
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": configmap_name(derived), "namespace": cluster.namespace},
        "data": {
            CONF_FILE: render_conf_file(spec, site, derived),
            ENV_FILE: render_env(spec),
            SCHEMA_FILE: render_reader_schema(meta),
            JOB_FILE: render_job(spec, site, derived, meta),
        },
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
    files = {
        CONF_FILE: render_conf_file(spec, site, derived),
        ENV_FILE: render_env(spec),
        SCHEMA_FILE: render_reader_schema(meta),
        JOB_FILE: render_job(spec, site, derived, meta),
    }
    if site.kubernetes is None:
        return files
    if image_tag is None:
        raise ValueError("a run on a cluster starts an image, so render needs image_tag: the tag that was pushed")
    files[SPARKAPPLICATION_FILE] = render_sparkapplication(spec, site, derived, meta, image_tag)
    files[CONFIGMAP_FILE] = render_job_configmap(spec, site, derived, meta)
    return files
