# SPDX-License-Identifier: Apache-2.0
"""The knobs a managed Flink run is sized by, and the files one run needs.

Everything here renders text: the SQL the job submits, the configuration it is
submitted with, and the cluster shape the local stack starts. Nothing in this
module reaches a cluster, which is what lets a run's whole configuration be
read — and diffed against another run's — before any compute is paid for.

The engine is stock Flink: a released image, released connector jars and SQL.
No source, sink or serializer is written anywhere in this package, so a result
attributed to Flink is Flink's rather than this harness's.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import cast

import yaml

from engines.flink.script import join_statements
from ingest_bench import uri
from ingest_bench.catalog import table_identifier
from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.kafka_auth import REGION_KEY
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.kubernetes import NAME, EngineKubernetes
from ingest_bench.specs.model import (
    VALUE_ENCODING_AVRO,
    VALUE_ENCODING_CONFLUENT,
    KubernetesConfig,
    RunSpec,
    SiteConfig,
)

# The names inside the submitted script. Nothing outside the script refers to
# either, so both are fixed rather than derived from the run.
SOURCE_TABLE = "kafka_source"
CATALOG_NAME = "ice"

# The files `render` writes into the run directory. The last two are written
# only for a run on a cluster, which is the one that has an operator to read
# them.
SQL_FILE = "job.sql"
CONF_FILE = "flink-conf.yaml"
ENV_FILE = "flink.env"
FLINKDEPLOYMENT_FILE = "flinkdeployment.yaml"
CONFIGMAP_FILE = "flink-job-configmap.yaml"

NONE = "none"
HASH = "hash"
RANGE = "range"
DISTRIBUTION_MODES = frozenset({NONE, HASH, RANGE})

# The image the operator starts, under the registry the site names. Public
# because it is one of three statements of this name — `push-images.sh` pushes
# it and `deploy/aws/setup.sh` creates the repository — and a test holds the
# three together.
IMAGE_REPOSITORY = "lakehouse-ingest-bench/flink"

# The Flink the image carries, in the two spellings the documents need: the
# operator's version label, and the jar whose driver runs a Python job.
_FLINK_VERSION_LABEL = "v1_20"
_PYFLINK_JAR = "local:///opt/flink/opt/flink-python-1.20.1.jar"
_PYTHON_DRIVER = "org.apache.flink.client.python.PythonDriver"
_JOB_SCRIPT = "/opt/bench/engines/flink/job.py"

# Where the run's rendered files are mounted, and the volume that carries
# them. Under `/opt/bench` beside the submitter rather than at `/run`, which
# is the container's own runtime directory.
_RUN_MOUNT = "/opt/bench/run"
_JOB_VOLUME = "job"

# The operator's fixed name for the Flink container. A podTemplate container
# under any other name is added to the pod as a sidecar instead of being
# merged into the one that runs Flink, so this name is not ours to choose.
_FLINK_CONTAINER = "flink-main-container"

# PyFlink publishes no Linux aarch64 wheel in any release, so the image is
# amd64 and a node that cannot run it is not a placement the site may pick.
# Merged over the site's selector for that reason.
_ARCH_PIN = {"kubernetes.io/arch": "amd64"}

REST = "rest"

# How a driver addresses a Flink run on a cluster. The names are the operator's
# rather than ours: it publishes the JobManager's REST endpoint as a Service
# called `<deployment>-rest` and labels the pods it creates `app` and
# `component`, and the FlinkDeployment rendered below declares neither.
#
# The jobmanager is the pod provenance is read off because it is the one of the
# two whose image is the engine's for every submission mode. No pods selector:
# everything `verify` compares is reported by the job itself.
KUBERNETES = EngineKubernetes(
    kind="flinkdeployment",
    running_state="RUNNING",
    # A job that finished or was cancelled before the run started is as far
    # from runnable as one that failed, and its fleet is gone either way — so
    # waiting any of the three out would only postpone the same refusal.
    failed_states=("FAILED", "CANCELED", "FINISHED"),
    state_jsonpath="{.status.jobStatus.state}",
    rest_service_suffix="-rest",
    rest_port=8081,
    log_target=f"deploy/{NAME}",
    provenance_selector=f"app={NAME},component=jobmanager",
    pods_selector="",
    document_file=FLINKDEPLOYMENT_FILE,
    configmap_file=CONFIGMAP_FILE,
)

# The type each knob is declared as. This is also the accepted surface: a key
# that is not here is refused, so a misspelled knob costs one error message
# instead of starting a run whose tuning silently did not apply.
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

# The knobs with no defensible default: the fleet's size, the memory a
# taskmanager gets, how often it commits and how it distributes writes are the
# axes a run exists to vary, and guessing any of them would publish a result
# nobody chose.
REQUIRED_KNOBS = frozenset({"taskmanagers", "slots", "tm_cpu", "tm_mem_mb", "checkpoint_interval", "distribution_mode"})

# Flink writes `pipeline.max-parallelism` into a job's state at its first
# checkpoint and cannot raise it on a restore, so the default leaves room to
# grow a run's fleet without discarding the state it had.
_MAX_PARALLELISM_FACTOR = 4

# The Flink SQL type each type name a corpus publishes is declared as.
# `TIMESTAMP(3)` and not `TIMESTAMP_LTZ` because the Iceberg column is a
# zoneless timestamp, and precision 3 because the corpus carries zoneless
# milliseconds. Three is also the widest timestamp `avro-confluent` can plan —
# it converts the DDL under Flink's legacy Avro mapping and declares no option
# to disable it — so one DDL serves both of the source formats below.
_DDL_TYPES = {
    "long": "BIGINT",
    "string": "STRING",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP(3)",
    "binary": "BYTES",
}

# The source format each wire format is read with. This is also the accepted
# surface: an encoding with no format here is refused before a topic exists,
# rather than failing inside the cluster with a run already staged.
_SOURCE_FORMATS = {
    VALUE_ENCODING_AVRO: "avro",
    VALUE_ENCODING_CONFLUENT: "avro-confluent",
}

# The plain format's one option. The non-legacy mapping sends a SQL `TIMESTAMP`
# to Avro's `local-timestamp-*` rather than to `timestamp-*`, which Avro
# defines as a UTC instant: both annotate the same `long` and the annotation is
# not on the wire, so this states the column's zoneless meaning rather than
# changing the bytes read. It belongs to `avro` alone — an unknown
# `avro-confluent.*` key fails validation.
_AVRO_OPTIONS: tuple[tuple[str, str], ...] = (("avro.timestamp_mapping.legacy", "false"),)

# `avro-confluent`'s registry options. It resolves each value's writer schema
# by the id in that value's header, so the registry is not optional for it. The
# credentials source has to be named beside the user info, or the format reads
# the registry unauthenticated and ignores it.
_REGISTRY_URL_KEY = "avro-confluent.url"
_REGISTRY_USER_INFO_SOURCE = ("avro-confluent.basic-auth.credentials-source", "USER_INFO")
_REGISTRY_USER_INFO_KEY = "avro-confluent.basic-auth.user-info"

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
# would put the same option in the WITH clause twice.
_MSK_IAM_REPLACED = frozenset({key for key, _ in _MSK_IAM_PROPS} | {REGION_KEY})

# The one S3 property that pyiceberg and Iceberg's Java library spell
# differently. Every other key in a property block is spelled the same, so
# only the rename is listed and the rest are carried through untouched.
_CATALOG_PROP_RENAMES = {"s3.region": "client.region"}

# A REST catalog hands back a table's location but not the implementation that
# reads it, so the scheme of the site's warehouse selects one. Left unset,
# Iceberg falls back to a Hadoop filesystem, which the bundled jars do not
# configure.
_FILE_IO_BY_SCHEME = {
    "s3://": "org.apache.iceberg.aws.s3.S3FileIO",
    "gs://": "org.apache.iceberg.gcp.gcs.GCSFileIO",
}

# pyiceberg reads `type` as the name of its own catalog implementation, while
# Iceberg Flink reads it as the literal `iceberg` and takes the backend from
# `catalog-type`. The property is therefore translated, never passed through.
_PYICEBERG_TYPE = "type"

# Properties the catalog clause states itself, so carrying them again would
# emit each one twice.
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
    # A YAML scalar is not coerced: `checkpoint_interval: 10` reads as an int
    # that Flink rejects as a duration, and `min_pause: 0s` is only a string
    # by accident of its suffix. Quoting is the fix, and saying so is more
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
        raise ValueError(f"{where} must be a mapping of Flink setting to value, got {value!r}")
    return {str(key): _str_at(entry, f"{where}.{key}") for key, entry in cast(dict[object, object], value).items()}


@dataclass(frozen=True)
class Knobs:
    """One run's Flink sizing and tuning, with every default already filled in."""

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
        """The fleet's task slots, which is what the writers can spread over."""
        return self.taskmanagers * self.slots

    def parallelism_default(self) -> int:
        """The job's default parallelism, which the Kafka readers inherit.

        The pinned Kafka connector exposes no per-source parallelism option,
        so a reader count below the fleet's slots can only be had by making it
        the job default and lifting the writers back up with a sink hint.
        Defaulting to the slot count instead would start a reader per slot,
        and every reader past the topic's partition count would sit idle.
        """
        return self.source_parallelism


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
        raise ValueError(f"spec.flink has unknown keys {unknown}; the ones it takes are {sorted(KNOBS)}")
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
    # Each of these becomes a parallelism or a container count, where zero
    # describes no runnable job rather than a smaller one.
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
    """Refuse a Flink block that cannot describe a runnable cluster, or a run it cannot read.

    ``meta`` is unread: every knob here is about the compute, and the corpus
    constrains none of them. It stays in the signature because the harness
    calls every managed engine's validator the same way.

    ``spec`` is read for its value encoding, and both of the ones the harness
    offers pass: each has a source format that reads it, so neither framing
    needs anything of the compute. What this refuses is an encoding the spec
    surface grew without such a format, which would otherwise fail on the
    cluster with a topic and a table already created.
    """
    knobs = read(block)
    _source_format(spec)
    if knobs.source_parallelism > spec.kafka.partitions:
        raise ValueError(
            f"spec.flink.source_parallelism {knobs.source_parallelism} exceeds the topic's "
            f"{spec.kafka.partitions} partitions, and a Kafka reader with no partition to read never reads"
        )
    if knobs.slots_total() < knobs.source_parallelism:
        raise ValueError(
            f"spec.flink.source_parallelism {knobs.source_parallelism} needs that many task slots, and "
            f"{knobs.taskmanagers} taskmanagers of {knobs.slots} slots give {knobs.slots_total()}"
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
    """The Flink SQL type name for a type a corpus publishes."""
    if iceberg_type not in _DDL_TYPES:
        raise ValueError(f"type {iceberg_type!r} has no Flink SQL name here; the ones read are {sorted(_DDL_TYPES)}")
    return _DDL_TYPES[iceberg_type]


def _literal(value: str) -> str:
    """``value`` as a SQL string literal, with its quotes doubled as SQL wants."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _with_clause(options: list[tuple[str, str]]) -> str:
    return ",\n".join(f"  {_literal(key)} = {_literal(value)}" for key, value in options)


def _column_ddl(name: str, meta: CorpusMetadata) -> str:
    """One source column, declared required.

    The `avro` format derives its reader schema from this DDL, so the DDL is
    what has to describe the bytes the producer wrote. Every column is
    `NOT NULL` because a nullable one becomes a union with null, and a union
    is a different wire encoding — a branch index precedes the value — than
    the non-union schema the corpus published. A row the corpus wrote carries
    a value in every column anyway.
    """
    published = meta.iceberg_types[name]
    if published not in _DDL_TYPES:
        raise ValueError(f"corpus column {name!r} publishes type {published!r}, which has no Flink SQL name")
    return f"  {name} {_DDL_TYPES[published]} NOT NULL"


def _is_msk_iam(security: dict[str, str]) -> bool:
    """Whether the site's Kafka properties are the harness's MSK IAM signal."""
    return _SASL_MECHANISM_KEY in security and security[_SASL_MECHANISM_KEY] == _OAUTHBEARER and REGION_KEY in security


def _kafka_options(security: dict[str, str]) -> list[tuple[str, str]]:
    """The site's Kafka properties as source options, IAM translated.

    A key outside the signal is carried through whatever the authentication is:
    a TLS or a client setting is orthogonal to how the connection is
    authenticated, and dropping it would silently undo something the site asked
    for.
    """
    if not _is_msk_iam(security):
        return [(f"properties.{key}", value) for key, value in security.items()]
    carried = [(key, value) for key, value in security.items() if key not in _MSK_IAM_REPLACED]
    return [(f"properties.{key}", value) for key, value in (*_MSK_IAM_PROPS, *carried)]


def _source_format(spec: RunSpec) -> str:
    """The format that reads the run's wire encoding, or a refusal to read it."""
    if spec.kafka.value_encoding not in _SOURCE_FORMATS:
        raise ValueError(
            f"spec.kafka.value_encoding is {spec.kafka.value_encoding!r} and a Flink source reads "
            f"{sorted(_SOURCE_FORMATS)}"
        )
    return _SOURCE_FORMATS[spec.kafka.value_encoding]


def _format_options(spec: RunSpec, site: SiteConfig) -> list[tuple[str, str]]:
    """The source's format, and whatever that format needs to decode a value."""
    options = [("format", _source_format(spec))]
    if spec.kafka.value_encoding != VALUE_ENCODING_CONFLUENT:
        return [*options, *_AVRO_OPTIONS]
    registry = site.schema_registry
    if registry is None:
        raise ValueError(
            f"spec.kafka.value_encoding is {VALUE_ENCODING_CONFLUENT!r}, which resolves each value's writer "
            "schema by the id in its header, and the site declares no kafka.schema_registry.url to resolve it "
            "against"
        )
    options.append((_REGISTRY_URL_KEY, registry.url))
    # As the site wrote it, `${env:NAME}` included: the submitter substitutes
    # the environment over the whole script, so the file names a credential
    # rather than holding one.
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
        # The run id, so a consumer group an abandoned run left behind names
        # the run that left it.
        ("properties.group.id", derived.run_id),
        # From the topic's head: the producer publishes before an engine is
        # asked to consume, and a latest-offset reader would skip that head
        # and be scored as having lost it.
        ("scan.startup.mode", "earliest-offset"),
        *_kafka_options(site.kafka_security),
        *_format_options(spec, site),
    ]
    return f"CREATE TABLE {SOURCE_TABLE} (\n{columns}\n) WITH (\n{_with_clause(options)}\n)"


def _catalog_key(key: str) -> str:
    """The Iceberg Java name of a pyiceberg property name."""
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
            f"a Flink run reads its table through an Iceberg REST catalog, and site.catalog.props names catalog "
            f"type {props[_PYICEBERG_TYPE]!r}"
        )
    options: list[tuple[str, str]] = [
        ("type", "iceberg"),
        ("catalog-type", REST),
        ("uri", _required_prop(props, "uri")),
        # The catalog's own addressing, which is not always a location: a Glue
        # REST endpoint takes the account that owns the catalog here.
        ("warehouse", _required_prop(props, "warehouse")),
    ]
    # Sorted by the name the site wrote, so two runs of one site render
    # byte-identical catalog clauses and any diff between two scripts is a
    # difference in their knobs.
    options += [(_catalog_key(key), props[key]) for key in sorted(set(props) - _STATED_CATALOG_PROPS)]
    # The site's warehouse and not the catalog property of that name, which
    # carries no scheme wherever the catalog addresses itself by something
    # other than a location.
    file_io = _file_io_for(site.warehouse)
    if file_io is not None:
        options.append(("io-impl", file_io))
    return f"CREATE CATALOG {CATALOG_NAME} WITH (\n{_with_clause(options)}\n)"


def _insert(derived: Derived, meta: CorpusMetadata, knobs: Knobs) -> str:
    namespace, table = table_identifier(derived.table)
    hints: list[tuple[str, str]] = [("distribution-mode", knobs.distribution_mode)]
    if knobs.slots_total() != knobs.parallelism_default():
        # The readers are held at `source_parallelism` by the job default, so
        # the writers need saying otherwise or they inherit it and leave most
        # of the fleet's slots idle.
        hints.append(("write-parallelism", str(knobs.slots_total())))
    rendered = ", ".join(f"{_literal(key)} = {_literal(value)}" for key, value in hints)
    columns = ", ".join(meta.field_names())
    return (
        f"INSERT INTO {CATALOG_NAME}.`{namespace}`.`{table}` /*+ OPTIONS({rendered}) */\n"
        f"SELECT {columns} FROM {SOURCE_TABLE}"
    )


def render_sql(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """The script the job submits: the source, the catalog and the insert."""
    knobs = read(spec.engine_block)
    return join_statements((_source_ddl(spec, site, derived, meta), _catalog_ddl(site), _insert(derived, meta, knobs)))


# ---------------------------------------------------------------------------
# Rendering configuration
# ---------------------------------------------------------------------------


def _flag(value: bool) -> str:
    return "true" if value else "false"


def render_conf(spec: RunSpec, derived: Derived) -> dict[str, str]:
    """The Flink settings the script is submitted with.

    The cluster-shaped settings are here as well as in ``flink.env`` because
    the two are read at different moments: the stack sizes the containers
    before any job exists, and this is what the submitted job asks for.
    """
    knobs = read(spec.engine_block)
    conf = {
        "execution.checkpointing.interval": knobs.checkpoint_interval,
        "execution.checkpointing.min-pause": knobs.min_pause,
        "execution.checkpointing.unaligned.enabled": _flag(knobs.unaligned_checkpoints),
        # Exactly once is the promise the benchmark scores duplication
        # against, so it is not a knob: a run that relaxed it would be scored
        # against a weaker claim than every other run.
        "execution.checkpointing.mode": "EXACTLY_ONCE",
        "parallelism.default": str(knobs.parallelism_default()),
        "pipeline.max-parallelism": str(knobs.max_parallelism),
        "taskmanager.numberOfTaskSlots": str(knobs.slots),
        "taskmanager.memory.process.size": f"{knobs.tm_mem_mb}m",
        "jobmanager.memory.process.size": f"{knobs.jm_mem_mb}m",
        # The run id, so a job listing names the run rather than the SQL.
        "pipeline.name": derived.run_id,
    }
    # Last, so a run can override any setting above without this module
    # growing a knob for it.
    conf.update(knobs.extra_flink_conf)
    return conf


def _conf_yaml(spec: RunSpec, derived: Derived) -> str:
    """The settings as the submitter reads them, from a file or from a mount."""
    return yaml.safe_dump(render_conf(spec, derived), sort_keys=True, default_flow_style=False)


# ---------------------------------------------------------------------------
# Rendering the Kubernetes documents
# ---------------------------------------------------------------------------


def _cluster(site: SiteConfig) -> KubernetesConfig:
    if site.kubernetes is None:
        raise ValueError("a Flink run on Kubernetes is placed by site.kubernetes, and the site declares no cluster")
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
    return f"{kubernetes_name(derived.run_id)}-flink-job"


def render_flinkdeployment(
    spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata, image_tag: str
) -> str:
    """The FlinkDeployment one run is, as the operator takes it.

    ``meta`` is unread — the corpus shapes the SQL and not the cluster — and
    stays in the signature so both of a run's Kubernetes documents are
    rendered from the same arguments.
    """
    knobs = read(spec.engine_block)
    cluster = _cluster(site)
    conf = {
        # Under the run's own directory, so an abandoned run's state is found
        # and removed by the name of the run that wrote it. First, because
        # `extra_flink_conf` is applied last and a run that says where its
        # checkpoints go means it.
        "state.checkpoints.dir": uri.join(site.runs_root, derived.run_id, "checkpoints"),
        **render_conf(spec, derived),
    }
    container: dict[str, object] = {
        "name": _FLINK_CONTAINER,
        "volumeMounts": [{"name": _JOB_VOLUME, "mountPath": _RUN_MOUNT, "readOnly": True}],
    }
    if cluster.aws_region is not None:
        # What an AWS SDK reads when nothing else names a region for it, which
        # is the case for both halves of an MSK IAM connection: the token
        # signer in the Kafka client, and S3 under the table's FileIO.
        #
        # Both names, because the SDKs disagree about which one carries it.
        # This container's Java client reads `AWS_REGION`; botocore, which any
        # Python tooling beside it goes through, reads `AWS_DEFAULT_REGION`
        # alone and is left with no region at all when only the other is set.
        container["env"] = [
            {"name": name, "value": cluster.aws_region} for name in ("AWS_REGION", "AWS_DEFAULT_REGION")
        ]
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
            # Standalone and not the operator's native mode: native asks
            # Kubernetes for the taskmanagers the job's parallelism implies,
            # which would make `taskmanagers` a number nobody honoured.
            "mode": "standalone",
            "serviceAccount": cluster.flink_service_account,
            "flinkConfiguration": conf,
            # The three fields below are read back out of the effective conf
            # rather than off the knobs, because the operator applies a CRD
            # field over `spec.flinkConfiguration`: a run that overrode one of
            # these through `extra_flink_conf` would otherwise be honoured by
            # the job it submitted and discarded by the cluster running it.
            # `replicas` and the slot count are not restated in the conf, and
            # `cpu` has no conf key at all, so those stay knobs.
            "jobManager": {"resource": {"memory": conf["jobmanager.memory.process.size"], "cpu": knobs.jm_cpu}},
            "taskManager": {
                "resource": {"memory": conf["taskmanager.memory.process.size"], "cpu": knobs.tm_cpu},
                "replicas": knobs.taskmanagers,
            },
            "job": {
                # PyFlink's own jar and driver: the job is the script the args
                # name, so a run builds no jar of its own.
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
                # A run is scored once and never resumed, so there is no state
                # to carry across an edit of this object.
                "upgradeMode": "stateless",
                "state": "running",
            },
            "podTemplate": {"apiVersion": "v1", "kind": "Pod", "spec": pod_spec},
        },
    }
    return yaml.safe_dump(document, sort_keys=False)


def render_job_configmap(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """The ConfigMap holding the two files the submitter reads off its mount."""
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
    """The engine's files for the run directory, keyed by filename.

    A site with a cluster gets the two Kubernetes documents as well, and needs
    the tag of the image they start. A site without one is the local stack,
    which builds its own image and submits the job itself.
    """
    knobs = read(spec.engine_block)
    files = {
        SQL_FILE: render_sql(spec, site, derived, meta),
        CONF_FILE: _conf_yaml(spec, derived),
        # The cluster's shape is not a job setting: the stack starts the
        # taskmanagers and sizes the containers before a job is submitted, so
        # it reads these as environment instead.
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
        raise ValueError("a run on a cluster starts an image, so render needs image_tag: the tag that was pushed")
    files[FLINKDEPLOYMENT_FILE] = render_flinkdeployment(spec, site, derived, meta, image_tag)
    files[CONFIGMAP_FILE] = render_job_configmap(spec, site, derived, meta)
    return files
