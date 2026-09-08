"""The knobs a managed Flink leg is sized by, and the files one run needs.

Everything here renders text: the SQL the job submits, the configuration it is
submitted with, and the cluster shape the local stack starts. Nothing in this
module reaches a cluster, which is what lets a leg's whole configuration be
read — and diffed against another leg's — before any compute is paid for.

The engine is stock Flink: a released image, released connector jars and SQL.
No source, sink or serializer is written anywhere in this package, so a result
attributed to Flink is Flink's rather than this harness's.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import cast

import yaml

from engines.flink.script import join_statements
from ingest_bench.catalog import table_identifier
from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.model import RunSpec, SiteConfig

# The names inside the submitted script. Nothing outside the script refers to
# either, so both are fixed rather than derived from the run.
SOURCE_TABLE = "kafka_source"
CATALOG_NAME = "ice"

# The files `render` writes into the run directory.
SQL_FILE = "job.sql"
CONF_FILE = "flink-conf.yaml"
ENV_FILE = "flink.env"

NONE = "none"
HASH = "hash"
RANGE = "range"
DISTRIBUTION_MODES = frozenset({NONE, HASH, RANGE})

REST = "rest"

# The type each knob is declared as. This is also the accepted surface: a key
# that is not here is refused, so a misspelled knob costs one error message
# instead of running a leg whose tuning silently did not apply.
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
# axes a leg exists to vary, and guessing any of them would publish a result
# nobody chose.
REQUIRED_KNOBS = frozenset({"taskmanagers", "slots", "tm_cpu", "tm_mem_mb", "checkpoint_interval", "distribution_mode"})

# Flink writes `pipeline.max-parallelism` into a job's state at its first
# checkpoint and cannot raise it on a restore, so the default leaves room to
# grow a leg's fleet without discarding the state it had.
_MAX_PARALLELISM_FACTOR = 4

# The Flink SQL type each type name a corpus publishes is declared as.
# `TIMESTAMP(6)` and not `TIMESTAMP_LTZ` because the Iceberg column is a
# zoneless timestamp and the corpus carries zoneless microseconds. Under the
# non-legacy Avro mapping the source reads that column as Avro
# `local-timestamp-micros`, which annotates a `long` — the same wire form as
# the corpus's `timestamp-micros`, differing only in the logical type's name,
# and Avro's binary encoding ignores the annotation.
_DDL_TYPES = {
    "long": "BIGINT",
    "string": "STRING",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP(6)",
    "binary": "BYTES",
}

# The one S3 property that pyiceberg and Iceberg's Java library spell
# differently. Every other key in a property block is spelled the same, so
# only the rename is listed and the rest are carried through untouched.
_CATALOG_PROP_RENAMES = {"s3.region": "client.region"}

# A REST catalog hands back a table's location but not the implementation that
# reads it, so the warehouse's scheme selects one. Left unset, Iceberg falls
# back to a Hadoop filesystem, which the bundled jars do not configure.
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
    """One leg's Flink sizing and tuning, with every default already filled in."""

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
    """Refuse a Flink block that cannot describe a runnable leg.

    ``meta`` is unread: every knob here is about the compute, and the corpus
    constrains none of them. It stays in the signature because the harness
    calls every managed engine's validator the same way.
    """
    knobs = read(block)
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


def _source_ddl(site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
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
        *((f"properties.{key}", value) for key, value in site.kafka_security.items()),
        ("format", "avro"),
        # Flink's legacy mapping sends SQL `TIMESTAMP` to Avro `timestamp-*`,
        # which it caps at millisecond precision, so a `TIMESTAMP(6)` column
        # cannot be planned at all while the legacy default stands. Disabled,
        # the column maps to `local-timestamp-micros` instead: microseconds,
        # zoneless, and the same `long` on the wire as the corpus wrote.
        ("avro.timestamp_mapping.legacy", "false"),
    ]
    return f"CREATE TABLE {SOURCE_TABLE} (\n{columns}\n) WITH (\n{_with_clause(options)}\n)"


def _catalog_key(key: str) -> str:
    """The Iceberg Java name of a pyiceberg property name."""
    return _CATALOG_PROP_RENAMES[key] if key in _CATALOG_PROP_RENAMES else key


def _required_prop(props: dict[str, str], key: str) -> str:
    if key not in props:
        raise ValueError(f"site.catalog.props must set {key!r}: a Flink leg addresses its table through it")
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
            f"a Flink leg reads its table through an Iceberg REST catalog, and site.catalog.props names catalog "
            f"type {props[_PYICEBERG_TYPE]!r}"
        )
    warehouse = _required_prop(props, "warehouse")
    options: list[tuple[str, str]] = [
        ("type", "iceberg"),
        ("catalog-type", REST),
        ("uri", _required_prop(props, "uri")),
        ("warehouse", warehouse),
    ]
    # Sorted by the name the site wrote, so two legs of one site render
    # byte-identical catalog clauses and any diff between two scripts is a
    # difference in their knobs.
    options += [(_catalog_key(key), props[key]) for key in sorted(set(props) - _STATED_CATALOG_PROPS)]
    file_io = _file_io_for(warehouse)
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
    return join_statements((_source_ddl(site, derived, meta), _catalog_ddl(site), _insert(derived, meta, knobs)))


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
        # against, so it is not a knob: a leg that relaxed it would be scored
        # against a weaker claim than every other leg.
        "execution.checkpointing.mode": "EXACTLY_ONCE",
        "parallelism.default": str(knobs.parallelism_default()),
        "pipeline.max-parallelism": str(knobs.max_parallelism),
        "taskmanager.numberOfTaskSlots": str(knobs.slots),
        "taskmanager.memory.process.size": f"{knobs.tm_mem_mb}m",
        "jobmanager.memory.process.size": f"{knobs.jm_mem_mb}m",
        # The run id, so a job listing names the run rather than the SQL.
        "pipeline.name": derived.run_id,
    }
    # Last, so a leg can override any setting above without this module
    # growing a knob for it.
    conf.update(knobs.extra_flink_conf)
    return conf


def render(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> dict[str, str]:
    """The engine's files for the run directory, keyed by filename."""
    knobs = read(spec.engine_block)
    return {
        SQL_FILE: render_sql(spec, site, derived, meta),
        CONF_FILE: yaml.safe_dump(render_conf(spec, derived), sort_keys=True, default_flow_style=False),
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
