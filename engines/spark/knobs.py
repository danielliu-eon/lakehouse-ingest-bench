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

from engines.spark.stream_to_iceberg import JOB_DOCUMENT, READER_SCHEMA
from ingest_bench import uri
from ingest_bench.catalog import table_identifier
from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.kafka_auth import REGION_KEY
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.model import RunSpec, SiteConfig

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

# The corpus carries zoneless microseconds and the table's column is a zoneless
# `timestamp`, so the reader schema annotates the same `long` as Avro's local
# variant: identical on the wire — the annotation is not encoded — and it is
# what makes `from_avro` yield a `TimestampNTZ` rather than a zoned instant
# that would land in a `timestamptz` column instead.
_TIMESTAMP_MICROS = "timestamp-micros"
_LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros"

# Amazon MSK's IAM authentication, as the Java client spells it. The harness
# signals it with librdkafka's `OAUTHBEARER` beside its own `aws.region`
# pseudo-key, because librdkafka has no MSK mechanism and signs the token
# itself; the Java client has one, under a name of its own, and the login
# module below signs per connection from whatever credentials the pod holds.
# So the signal is translated rather than passed through — and neither form
# carries a credential, which is why an MSK site needs no secret in a file.
_SASL_MECHANISM_KEY = "sasl.mechanism"
_OAUTHBEARER = "OAUTHBEARER"
_MSK_IAM_PROPS: tuple[tuple[str, str], ...] = (
    ("security.protocol", "SASL_SSL"),
    (_SASL_MECHANISM_KEY, "AWS_MSK_IAM"),
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
    """Refuse a Spark block that cannot describe a runnable job.

    ``spec`` and ``meta`` are unread: every knob here is about the compute, and
    neither the topic nor the corpus constrains one — Spark's Kafka source
    spreads a topic's partitions over whatever cores it has, so a fleet wider
    than the topic costs idle cores rather than a reader with nothing to read.
    They stay in the signature because the harness calls every managed
    engine's validator the same way.
    """
    read(block)


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
        # A streaming query with no checkpoint location refuses to start, and
        # naming it here rather than on the writer keeps the job free of any
        # knowledge of where the run's artifacts live.
        "spark.sql.streaming.checkpointLocation": checkpoint_uri(site, derived),
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
    return conf


def render_conf_file(spec: RunSpec, site: SiteConfig, derived: Derived) -> str:
    """The settings as `spark-submit --properties-file` reads them."""
    return "".join(f"{key} {value}\n" for key, value in render_conf(spec, site, derived).items())


# ---------------------------------------------------------------------------
# Rendering the job's documents
# ---------------------------------------------------------------------------


def _is_msk_iam(security: dict[str, str]) -> bool:
    """Whether the site's Kafka properties are the harness's MSK IAM signal."""
    return _SASL_MECHANISM_KEY in security and security[_SASL_MECHANISM_KEY] == _OAUTHBEARER and REGION_KEY in security


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
    """``node`` with every microsecond timestamp annotated as its local variant."""
    if isinstance(node, dict):
        return {
            key: (_LOCAL_TIMESTAMP_MICROS if key == "logicalType" and value == _TIMESTAMP_MICROS else _zoneless(value))
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
        },
        "trigger_interval": knobs.trigger_interval,
        "max_offsets_per_trigger": knobs.max_offsets_per_trigger,
    }
    return json.dumps(document, indent=2) + "\n"


# ---------------------------------------------------------------------------
# The run directory
# ---------------------------------------------------------------------------


def render(
    spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata, *, image_tag: str | None = None
) -> dict[str, str]:
    """The engine's files for the run directory, keyed by filename.

    ``image_tag`` is unread: the local stack builds its own image, and the
    Kubernetes documents that would name a pushed one are not rendered here. It
    stays in the signature because the harness calls every managed engine's
    renderer the same way.
    """
    knobs = read(spec.engine_block)
    return {
        CONF_FILE: render_conf_file(spec, site, derived),
        # The submission line's own shape, which is not a job setting: the
        # driver's core count and heap are chosen before a session exists, so
        # the stack reads these as environment instead.
        ENV_FILE: (f"{LOCAL_CORES_VAR}={knobs.cores_total()}\n{DRIVER_MEM_VAR}={knobs.driver_mem_mb}\n"),
        SCHEMA_FILE: render_reader_schema(meta),
        JOB_FILE: render_job(spec, site, derived, meta),
    }
