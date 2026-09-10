# SPDX-License-Identifier: Apache-2.0
"""The knobs a managed Spark run is sized by, and the files one run needs.

Everything here renders text: the properties the job is submitted with, the
Avro schema it decodes against, the document naming its source and sink, and
the shape the local stack starts. Nothing in this module reaches a cluster,
which is what lets a run's whole configuration be read — and diffed against
another run's — before any compute is paid for.

The engine is stock Spark: a released image, released connector jars, and
Structured Streaming. No source, sink or serializer is written anywhere in this
package, so a result attributed to Spark is Spark's rather than this harness's.
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
from ingest_bench.specs.kubernetes import NAME, EngineKubernetes
from ingest_bench.specs.model import KubernetesConfig, RunSpec, SiteConfig

# The catalog the job's table is addressed through. Nothing outside the
# rendered files refers to it, so it is fixed rather than derived from the run.
CATALOG_NAME = "ice"

# The files `render` writes into the run directory. The two the job itself
# reads are named off its own paths, so the renderer cannot write one under a
# name the job does not open. The other two are read before any Python runs —
# `spark-submit` takes the properties file, the stack reads the environment —
# so those names are this module's, and the compose file's copy of them is
# checked by a test.
CONF_FILE = "spark-defaults.conf"
ENV_FILE = "job.env"
SCHEMA_FILE = READER_SCHEMA.name
JOB_FILE = JOB_DOCUMENT.name

# The two documents a run on a cluster is, which the local stack has no use for
# — it submits the job itself instead of handing an operator an object.
SPARKAPPLICATION_FILE = "sparkapplication.yaml"
CONFIGMAP_FILE = "spark-job-configmap.yaml"

# The two settings the submission line is shaped by, which the compose file
# interpolates by name. Named here rather than written twice so a test can hold
# the two copies together.
LOCAL_CORES_VAR = "LOCAL_CORES"
DRIVER_MEM_VAR = "DRIVER_MEM_MB"

NONE = "none"
HASH = "hash"
RANGE = "range"
DISTRIBUTION_MODES = frozenset({NONE, HASH, RANGE})

REST = "rest"

# The image the operator starts, under the registry the site names, and the
# Spark inside it. `sparkVersion` is a required field of a SparkApplication;
# a test holds this to the tag the Dockerfile pins so the two cannot drift.
#
# The repository is public because it is one of three statements of this name —
# `push-images.sh` pushes it and `deploy/aws/setup.sh` creates the repository —
# and a test holds the three together.
IMAGE_REPOSITORY = "lakehouse-ingest-bench/spark"
SPARK_VERSION = "3.5.9"

# The job, as the operator submits it: a `local://` reference is a path inside
# the image rather than a file the operator would have to stage. A test holds
# this to the path the Dockerfile copies the job to.
_JOB_SCRIPT = "local:///opt/bench/engines/spark/stream_to_iceberg.py"

# Where the run's rendered files are mounted, and the volume that carries them.
# The job's own constant, so a mount under any other path would be one the job
# does not open.
_RUN_MOUNT = str(RUN_DIR)
_JOB_VOLUME = "job"

# How a driver addresses a Spark run on a cluster. Every name is the
# spark-operator's: it publishes the driver's UI as a Service called
# `<application>-ui-svc`, names the driver pod `<application>-driver`, and
# labels both halves of the fleet with the application's name.
#
# `pods_selector` matches the driver and the executors together, because two of
# the things `verify` compares — how many executors there are, and whether
# their CPU is guaranteed rather than a share the node may reclaim — are
# properties of the pods and are reported nowhere in the driver's own answers.
KUBERNETES = EngineKubernetes(
    kind="sparkapplication",
    running_state="RUNNING",
    # A submission the operator could not make, an application that failed or
    # is on its way to failing, and one that ended cleanly: none of the five
    # has a fleet left to wait for. A streaming query that reached COMPLETED
    # before the run started is as unrunnable as one that failed, and waiting
    # it out would only postpone the same refusal.
    failed_states=("FAILED", "SUBMISSION_FAILED", "FAILING", "COMPLETED", "SUCCEEDING"),
    state_jsonpath="{.status.applicationState.state}",
    rest_service_suffix="-ui-svc",
    rest_port=4040,
    log_target=f"pod/{NAME}-driver",
    provenance_selector=f"spark-role=driver,sparkoperator.k8s.io/app-name={NAME}",
    pods_selector=f"sparkoperator.k8s.io/app-name={NAME}",
    document_file=SPARKAPPLICATION_FILE,
    configmap_file=CONFIGMAP_FILE,
)

# Spark's own duration grammar for a processing-time trigger, which is a count
# and a whole unit word: `Trigger.ProcessingTime` parses the string as a SQL
# interval, and that parser takes neither an abbreviation (`10s`, `1m`, `500ms`)
# nor a missing space (`10seconds`). Checked here because an interval Spark
# cannot parse fails the query at its first micro-batch, which is minutes into a
# staged run with a topic and a table already created.
_TRIGGER_RE = re.compile(r"^\d+\s+(millisecond|milliseconds|second|seconds|minute|minutes|hour|hours)$")

# The Iceberg catalog implementation Spark binds the catalog name to. Without
# it `spark.sql.catalog.ice.*` configures a catalog Spark never instantiates.
_SPARK_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"

# Iceberg's Spark extensions, which is what makes `USING iceberg` tables and
# the sink's write options available to the session.
_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

# The corpus carries zoneless milliseconds and the table's column is a zoneless
# `timestamp`, so the reader schema annotates the same `long` as Avro's local
# variant: identical on the wire — the annotation is not encoded — and it is
# what makes `from_avro` yield a `TimestampNTZ` rather than a zoned instant
# that would land in a `timestamptz` column instead.
_TIMESTAMP_MILLIS = "timestamp-millis"
_LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis"

# Amazon MSK's IAM authentication, as the Java client spells it. The harness
# signals it with librdkafka's `OAUTHBEARER` beside its own `aws.region`
# pseudo-key, because librdkafka has no MSK mechanism and signs the token
# itself; the Java client has one, under a name of its own, and the login
# module below signs per connection from whatever credentials the pod holds.
# So the signal is translated rather than passed through — and neither form
# carries a credential, which is why an MSK site needs no secret in a file.
_OAUTHBEARER = "OAUTHBEARER"
_MSK_IAM_PROPS: tuple[tuple[str, str], ...] = (
    ("security.protocol", "SASL_SSL"),
    (MECHANISM_KEY, "AWS_MSK_IAM"),
    ("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;"),
    ("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler"),
)

# The keys the translation answers for: the four it renders, and the pseudo-key
# that no Kafka client knows — the region reaches the signer as `AWS_REGION` in
# the pod's environment instead. Carrying any of them from the site as well
# would set the same option twice.
_MSK_IAM_REPLACED = frozenset({key for key, _ in _MSK_IAM_PROPS} | {REGION_KEY})

# The one S3 property that pyiceberg and Iceberg's Java library spell
# differently. Every other key in a property block is spelled the same, so
# only the rename is listed and the rest are carried through untouched.
_CATALOG_PROP_RENAMES = {"s3.region": "client.region"}

# A REST catalog hands back a table's location but not the implementation that
# reads it, so the scheme of the site's warehouse selects one. Left unset,
# Iceberg falls back to a Hadoop filesystem, which the bundled jars do not
# configure for the catalog's own reads.
_FILE_IO_BY_SCHEME = {
    "s3://": "org.apache.iceberg.aws.s3.S3FileIO",
    "gs://": "org.apache.iceberg.gcp.gcs.GCSFileIO",
}

# pyiceberg reads `type` as the name of its own catalog implementation, while
# Iceberg Spark reads it as the backend the catalog class talks to. The
# property is therefore restated, never passed through.
_PYICEBERG_TYPE = "type"

# Properties the catalog block states itself, so carrying them again would emit
# each one twice.
_STATED_CATALOG_PROPS = frozenset({_PYICEBERG_TYPE, "uri", "warehouse"})

# Spark reaches object storage through a Hadoop filesystem, and the S3A one is
# registered under its own scheme; `s3://` is a vendor alias that a stock Spark
# leaves unbound.
_S3_SCHEME = "s3://"
_S3A_SCHEME = "s3a://"

# The S3A settings a site with no cluster needs spelled Hadoop's way, mapped
# from the Iceberg property carrying the same value. Only that site: a run on a
# cluster reaches storage as the pod's own identity, where a static key is both
# unnecessary and a credential this file must not hold.
_S3A_FROM_CATALOG_PROP = (
    ("s3.endpoint", "spark.hadoop.fs.s3a.endpoint"),
    ("s3.path-style-access", "spark.hadoop.fs.s3a.path.style.access"),
    ("s3.access-key-id", "spark.hadoop.fs.s3a.access.key"),
    ("s3.secret-access-key", "spark.hadoop.fs.s3a.secret.key"),
)

# The type each knob is declared as. This is also the accepted surface: a key
# that is not here is refused, so a misspelled knob costs one error message
# instead of starting a run whose tuning silently did not apply.
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

# The knobs with no defensible default: the fleet's size, the memory an
# executor gets, how often it commits and how it distributes writes are the
# axes a run exists to vary, and guessing any of them would publish a result
# nobody chose.
REQUIRED_KNOBS = frozenset({"executors", "executor_cores", "executor_mem_mb", "trigger_interval", "distribution_mode"})


# ---------------------------------------------------------------------------
# Reading the block
# ---------------------------------------------------------------------------


def _int_at(value: object, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} must be an integer, got {value!r}")
    return value


def _str_at(value: object, where: str) -> str:
    # A YAML scalar is not coerced: `trigger_interval: 10` reads as an int that
    # Spark rejects as an interval. Quoting is the fix, and saying so is more
    # useful than passing an unusable value along.
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
    """One run's Spark sizing and tuning, with every default already filled in."""

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
        """The executor cores the writers spread over, which is the fleet's width."""
        return self.executors * self.executor_cores


# KNOBS is the surface a spec is checked against and `Knobs` is what a checked
# block becomes, so a name in one and not the other would either be accepted
# and dropped or set and unreachable.
if frozenset(KNOBS) != {field.name for field in fields(Knobs)}:
    raise ValueError(f"KNOBS declares {sorted(KNOBS)} and Knobs holds {sorted(field.name for field in fields(Knobs))}")


def read(block: dict[str, object]) -> Knobs:
    """The block's knobs with defaults applied, or a refusal to read it.

    Every default is applied by name rather than through a lookup default, so
    a knob this module forgot to read cannot masquerade as one a spec left
    out.
    """
    unknown = sorted(set(block) - set(KNOBS))
    if unknown:
        raise ValueError(f"spec.spark has unknown keys {unknown}; the ones it takes are {sorted(KNOBS)}")
    missing = sorted(REQUIRED_KNOBS - set(block))
    if missing:
        raise ValueError(f"spec.spark must set {missing}")

    executors = _int_at(block["executors"], "spec.spark.executors")
    executor_cores = _int_at(block["executor_cores"], "spec.spark.executor_cores")
    driver_cores = 1 if "driver_cores" not in block else _int_at(block["driver_cores"], "spec.spark.driver_cores")
    # Each of these becomes a core count or a container count, where zero
    # describes no runnable job rather than a smaller one.
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

    # Unset is the answer for a run whose micro-batch takes whatever has
    # accumulated by the time it fires. A limit of zero is no answer at all: it
    # describes a batch that can hold no records rather than a smaller one.
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
    """Refuse a Spark block that cannot describe a runnable job, or a run it cannot read.

    ``meta`` is unread: every knob here is about the compute, and neither the
    topic nor the corpus constrains one — Spark's Kafka source spreads a
    topic's partitions over whatever cores it has, so a fleet wider than the
    topic costs idle cores rather than a reader with nothing to read. It stays
    in the signature because the harness calls every managed engine's validator
    the same way.

    ``spec`` is read for its value encoding, and both of the ones the harness
    offers pass: the job strips a Confluent header before it decodes, so
    neither framing needs anything of the compute. What this refuses is an
    encoding the spec surface grew without a branch in the job, which would
    otherwise fail inside the image with a topic and a table already created.
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
    """The Iceberg Java name of a pyiceberg property name."""
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
    """The catalog block, as `spark.sql.catalog.<name>` settings."""
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
        # The catalog's own addressing, which is not always a location: a Glue
        # REST endpoint takes the account that owns the catalog here.
        f"{prefix}.warehouse": _required_prop(props, "warehouse", "the catalog resolves a table under it"),
    }
    # The site's warehouse and not the catalog property of that name, which
    # carries no scheme wherever the catalog addresses itself by something
    # other than a location.
    file_io = _file_io_for(site.warehouse)
    if file_io is not None:
        conf[f"{prefix}.io-impl"] = file_io
    # Sorted by the name the site wrote, so two runs of one site render
    # byte-identical catalog blocks and any diff between two properties files
    # is a difference in their knobs.
    for key in sorted(set(props) - _STATED_CATALOG_PROPS):
        conf[f"{prefix}.{_catalog_key(key)}"] = props[key]
    return conf


def checkpoint_uri(site: SiteConfig, derived: Derived) -> str:
    """Where the query's checkpoints go, addressed as Spark reaches storage.

    Under the run's own directory, so an abandoned run's state is found and
    removed by the name of the run that wrote it.
    """
    location = uri.join(site.runs_root, derived.run_id, "checkpoints")
    if location.startswith(_S3_SCHEME):
        return _S3A_SCHEME + location[len(_S3_SCHEME) :]
    return location


def _local_store_conf(site: SiteConfig, derived: Derived) -> dict[str, str]:
    """The S3A settings for a site with no cluster, from its Iceberg properties.

    The checkpoint path is a Hadoop filesystem and the table's data files are
    Iceberg's own FileIO, so the same store has to be described twice under two
    spellings. Read from the catalog properties rather than declared again
    because a second declaration of one endpoint is a second thing to keep in
    step, and the one that lost would be the one nobody reread.

    A site whose runs are not on S3 has no S3A filesystem to configure, so
    there is nothing to say about one.
    """
    if not checkpoint_uri(site, derived).startswith(_S3A_SCHEME):
        return {}
    return {
        setting: _required_prop(site.catalog_props, prop, "a run with no cluster reaches its checkpoints through it")
        for prop, setting in _S3A_FROM_CATALOG_PROP
    }


def render_conf(spec: RunSpec, site: SiteConfig, derived: Derived) -> dict[str, str]:
    """The Spark settings the job is submitted with."""
    knobs = read(spec.engine_block)
    conf = {
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        **_catalog_conf(site),
        "spark.executor.cores": str(knobs.executor_cores),
        "spark.executor.memory": f"{knobs.executor_mem_mb}m",
        "spark.driver.cores": str(knobs.driver_cores),
        "spark.driver.memory": f"{knobs.driver_mem_mb}m",
        # One shuffle partition per executor core, so a `hash` or `range`
        # distribution spreads its writers over the whole fleet instead of over
        # Spark's default two hundred — which on this fleet would be that many
        # writers per commit, and that many files.
        "spark.sql.shuffle.partitions": str(knobs.cores_total()),
        # The run id, so an application listing names the run rather than the
        # script.
        "spark.app.name": derived.run_id,
    }
    if site.kubernetes is None:
        conf.update(_local_store_conf(site, derived))
    # Last, so a run can override any setting above without this module growing
    # a knob for it.
    conf.update(knobs.extra_spark_conf)
    # A Spark setting is read by the framework and never by this harness's own
    # code, so nothing substitutes a reference in one: it would reach the
    # catalog as the six literal characters `${env:`. The Kafka source's
    # options are the resolvable half — they travel in `job.json`, which the
    # job reads — so a credential belongs in site.kafka.security, and a
    # reference anywhere in this conf is refused rather than rendered.
    referenced = sorted(key for key, value in conf.items() if has_placeholder(value))
    if referenced:
        raise ValueError(
            f"a Spark run renders {referenced} as Spark settings, and nothing resolves a "
            f"{PLACEHOLDER_FORM} in one; a credential the job can resolve goes in site.kafka.security"
        )
    return conf


def render_conf_file(spec: RunSpec, site: SiteConfig, derived: Derived) -> str:
    """The settings as `spark-submit --properties-file` reads them."""
    return "".join(f"{key} {value}\n" for key, value in render_conf(spec, site, derived).items())


def render_env(spec: RunSpec) -> str:
    """The submission line's own shape, which is not a job setting.

    The driver's core count and heap are chosen before a session exists, so the
    local stack reads these as environment rather than out of the properties.
    """
    knobs = read(spec.engine_block)
    return f"{LOCAL_CORES_VAR}={knobs.cores_total()}\n{DRIVER_MEM_VAR}={knobs.driver_mem_mb}\n"


# ---------------------------------------------------------------------------
# Rendering the job's documents
# ---------------------------------------------------------------------------


def _is_msk_iam(security: dict[str, str]) -> bool:
    """Whether the site's Kafka properties are the harness's MSK IAM signal."""
    return MECHANISM_KEY in security and security[MECHANISM_KEY] == _OAUTHBEARER and REGION_KEY in security


def kafka_options(security: dict[str, str]) -> dict[str, str]:
    """The site's Kafka properties as source options, IAM translated.

    A key outside the signal is carried through whatever the authentication is:
    a TLS or a client setting is orthogonal to how the connection is
    authenticated, and dropping it would silently undo something the site asked
    for.
    """
    if not _is_msk_iam(security):
        return {f"kafka.{key}": value for key, value in security.items()}
    carried = [(key, value) for key, value in security.items() if key not in _MSK_IAM_REPLACED]
    return {f"kafka.{key}": value for key, value in (*_MSK_IAM_PROPS, *carried)}


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _zoneless(node: object) -> object:
    """``node`` with every millisecond timestamp annotated as its local variant."""
    if isinstance(node, dict):
        return {
            key: (_LOCAL_TIMESTAMP_MILLIS if key == "logicalType" and value == _TIMESTAMP_MILLIS else _zoneless(value))
            for key, value in cast(dict[str, object], node).items()
        }
    if isinstance(node, list):
        return [_zoneless(entry) for entry in cast(list[object], node)]
    return node


def render_reader_schema(meta: CorpusMetadata) -> str:
    """The corpus's Avro schema as the schema `from_avro` decodes against."""
    return json.dumps(_zoneless(meta.schema), indent=2) + "\n"


def render_job(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """The document naming the job's source, its sink and its cadence."""
    knobs = read(spec.engine_block)
    namespace, table = table_identifier(derived.table)
    document = {
        "topic": derived.topic,
        "bootstrap": site.kafka_bootstrap,
        "group_id": derived.run_id,
        # How each value is framed, which is what the job decodes through.
        # Carried from the spec rather than re-derived, so the framing the job
        # reads is the one the producer wrote and staging registered for.
        "value_encoding": spec.kafka.value_encoding,
        "table": f"{CATALOG_NAME}.{namespace}.{table}",
        "columns": meta.field_names(),
        "kafka_options": kafka_options(site.kafka_security),
        "write_options": {
            "distribution-mode": knobs.distribution_mode,
            "fanout-enabled": _flag(knobs.fanout),
            # `from_avro` returns a struct Spark declares nullable whatever the
            # Avro schema says, so every column read out of it is nullable too
            # and Iceberg's static check refuses the write against a table whose
            # columns are required. Turning the check off leaves Spark's own
            # `AssertNotNull` on each required column, which is a stronger
            # guarantee than the one skipped: a null would end the run rather
            # than be caught by a schema comparison that ran before any row was
            # read. Not a knob — it is a consequence of the decode, not an axis
            # a run varies.
            "check-nullability": "false",
            # The writer's own option and not the session's
            # `spark.sql.streaming.checkpointLocation`, which Spark treats as a
            # parent: `createQuery` joins it with the query's name, and an
            # unnamed query gets a fresh random one on every start. The query
            # would then resume from no state after a driver restart, read the
            # topic from `earliest` again, and duplicate every row already
            # committed — which is the column exactness measures. A location
            # given here is used as it stands, so one run has one checkpoint
            # whatever restarts it.
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
    """The run id as a Kubernetes object name.

    An RFC 1123 subdomain is lowercase, and a run id's stamp is not: the `T`
    and the `Z` in it are refused by the API server. Only the names are
    lowercased — the run id itself is the identifier the topic, the table and
    the run directory are addressed by, and it stays as it is.
    """
    return run_id.lower()


def configmap_name(derived: Derived) -> str:
    """The ConfigMap the run's rendered files are mounted from."""
    return f"{kubernetes_name(derived.run_id)}-spark-job"


def _mount() -> list[dict[str, object]]:
    """The run's files on a pod, as its own copy.

    Copied per pod rather than shared, because `yaml.safe_dump` renders one
    object reached twice as an anchor and an alias — and a manifest is read by
    people at least as often as by an API server.
    """
    return [{"name": _JOB_VOLUME, "mountPath": _RUN_MOUNT, "readOnly": True}]


def _placement(cluster: KubernetesConfig) -> dict[str, object]:
    """Where a pod may run, as its own copy, for the reason above."""
    return {
        "nodeSelector": dict(cluster.node_selector),
        "tolerations": [dict(toleration) for toleration in cluster.tolerations],
    }


def _region_env(cluster: KubernetesConfig) -> list[dict[str, str]]:
    """The region under both names an SDK reads it as, as its own copy.

    Both halves of an MSK IAM connection need one — the token signer in the
    Kafka client and S3 under the table's FileIO — and the SDKs disagree about
    which name carries it: this image's Java client reads `AWS_REGION`, while
    botocore reads `AWS_DEFAULT_REGION` alone and is left with no region at all
    when only the other is set.

    A copy per caller, like the mount and the placement above: one list
    reached twice renders as an anchor and an alias.
    """
    if cluster.aws_region is None:
        return []
    return [{"name": name, "value": cluster.aws_region} for name in ("AWS_REGION", "AWS_DEFAULT_REGION")]


def _secret_env_from(cluster: KubernetesConfig) -> list[dict[str, object]]:
    """The Secret the fleet reads its environment from, as its own copy.

    What the job resolves the rendered document's `${env:NAME}` references
    against: `job.json` reaches a pod through a ConfigMap and the run's prefix
    in the bucket, so it holds the reference and this Secret holds the value.
    Both halves of the fleet get it — the executors open the Kafka source and
    the driver commits — and a copy per caller, like the mount and the
    placement above, because one list reached twice renders as an anchor and
    an alias.
    """
    if cluster.secret_name is None:
        return []
    return [{"secretRef": {"name": cluster.secret_name}}]


def render_sparkapplication(
    spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata, image_tag: str
) -> str:
    """The SparkApplication one run is, as the operator takes it.

    ``meta`` is unread — the corpus shapes the job document and not the fleet —
    and stays in the signature so both of a run's Kubernetes documents are
    rendered from the same arguments.

    Both halves of the fleet ask for as much CPU as they cap at, which is what
    makes their pods Guaranteed. A Burstable pod's cores are a share the node
    may reclaim, so a rate measured on one is the node's answer rather than the
    engine's; the memory limit Spark sets equal to the request on its own.
    """
    knobs = read(spec.engine_block)
    cluster = _cluster(site)
    driver: dict[str, object] = {
        "cores": knobs.driver_cores,
        "coreLimit": str(knobs.driver_cores),
        "memory": f"{knobs.driver_mem_mb}m",
        # The identity the whole fleet runs as: the driver creates the executor
        # pods itself, and both halves reach the broker and the table as this
        # account rather than with anything carried in a file.
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
    # The same environment on both halves, and the region is the whole of it:
    # the executors do the reading and the writing, and the driver signs the
    # commits. Off AWS there is no region to carry and neither gets an `env`.
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
            # Cluster and not client mode: the driver is a pod of its own, so
            # the fleet a run is costed for is the fleet the cluster scheduled
            # rather than one attached to whatever submitted it.
            "mode": "cluster",
            "image": f"{cluster.registry}/{IMAGE_REPOSITORY}:{image_tag}",
            # The tag is a commit, so an image already on the node is the image
            # that tag names and pulling it again buys nothing.
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": _JOB_SCRIPT,
            "sparkVersion": SPARK_VERSION,
            # A run is scored once and never resumed: a restarted driver would
            # read the topic from its checkpoint or from the beginning, and
            # either way the rows it committed would be attributed to an
            # attempt the result does not describe.
            "restartPolicy": {"type": "Never"},
            "sparkConf": render_conf(spec, site, derived),
            "driver": driver,
            "executor": executor,
            "volumes": [{"name": _JOB_VOLUME, "configMap": {"name": configmap_name(derived)}}],
        },
    }
    return yaml.safe_dump(document, sort_keys=False)


def render_job_configmap(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """The ConfigMap the run's rendered files are mounted from.

    All four and not only the two the job opens. The properties reached the
    operator as `spec.sparkConf` and the environment file is the local stack's,
    so neither is read off this mount — but a driver pod that carries every
    file the run rendered is one a person can read the run out of without
    fetching anything.
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
    """The engine's files for the run directory, keyed by filename.

    A site with a cluster gets the two Kubernetes documents as well, and needs
    the tag of the image they start. A site without one is the local stack,
    which builds its own image and submits the job itself.
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
