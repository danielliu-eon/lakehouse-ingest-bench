"""Declared value shape of a corpus schema, and the validation that gates it.

A schema is a JSON file listing one declaration per generated column; the
reserved columns are implicit and lead every schema. Everything a column can
say about its values is declared here and checked before a single row is
written, so a corpus cannot publish an axis it then flattens.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TypedDict, cast

# The counter-based value generator mixes a row identity through this prime, so
# it is part of the corpus contract: changing it changes every drawn value.
P = 1_000_000_007

# Row identities are handed out a block per batch, so a row's identity is
# `batch * ID_BLOCK + position` and stays disjoint across batches without any
# shared counter.
ID_BLOCK = 2**32


class SchemaField(TypedDict):
    name: str
    type: object


# Exactness is scored off `id` and partition truth off `partition_key`, so they
# lead every schema with fixed names and types: a schema free to rename or
# retype them would change what the scored ground truth means.
RESERVED_SCHEMA_FIELDS: list[SchemaField] = [
    {"name": "id", "type": "long"},
    {"name": "partition_key", "type": "string"},
]


# ---------------------------------------------------------------------------
# Per-column value distributions
# ---------------------------------------------------------------------------

KIND_RESERVED = "reserved"
KIND_CATEGORICAL = "categorical"
KIND_INTEGER = "integer"
KIND_DECIMAL = "decimal"
KIND_BOOLEAN = "boolean"
KIND_TIMESTAMP = "timestamp"
KIND_BLOB = "blob"

# Row identity and partition truth are read out of these columns, so they are
# computed, never drawn: a distribution over them would change what the scored
# ground truth means.
RESERVED_FIELDS = frozenset({"id", "partition_key"})

# A role is what the benchmark asks of a column irrespective of its name, which
# is what lets a declared schema rename its columns without invalidating the
# fixed benchmark queries or the arrival-time and payload machinery.
ROLE_GENERIC = "generic"
ROLE_EVENT_TIME = "event_time"
ROLE_ENTITY = "entity"
ROLE_SUM_MEASURE = "sum_measure"
ROLE_PAYLOAD = "payload"

ROLES = frozenset({ROLE_GENERIC, ROLE_EVENT_TIME, ROLE_ENTITY, ROLE_SUM_MEASURE, ROLE_PAYLOAD})

# Each of these is asked for by exactly one consumer, so exactly one column may
# answer: event time drives the arrival timeline, the entity column is the
# point-lookup key, the sum measure is what the aggregate queries sum, and the
# payload column is the one the row-size calibration trims.
SINGLETON_ROLES: dict[str, frozenset[str]] = {
    ROLE_EVENT_TIME: frozenset({KIND_TIMESTAMP}),
    ROLE_ENTITY: frozenset({KIND_CATEGORICAL}),
    ROLE_SUM_MEASURE: frozenset({KIND_INTEGER, KIND_DECIMAL}),
    ROLE_PAYLOAD: frozenset({KIND_BLOB}),
}

# A cardinality of zero means "a fresh value per row" — the state that denies
# Parquet a dictionary and makes a column's bytes survive compression.
UNBOUNDED_CARDINALITY = 0

# An unbounded token is a truncated digest, so its value space is 16**width. A
# row identity fits in 64 bits (`batch * ID_BLOCK + position`), so 16 hex
# characters is the narrowest token that stays effectively injective over a
# corpus; a shorter one would promise a fresh value per row and hand Parquet a
# dictionary instead.
MIN_UNBOUNDED_TOKEN_WIDTH = 16

# A skewed rank is drawn through a materialized CDF, so its memory is linear in
# the cardinality and is spent before the first row is written. Uniform ranks
# are drawn by modulus and cost nothing, which is why this bounds a declared
# skew rather than a cardinality.
COLUMN_ZIPF_MAX_CARDINALITY = 1_000_000

# The value kind is what a column declares; the Avro type is a consequence of
# it, so a schema cannot disagree with the generator that fills it.
AVRO_TYPE_BY_KIND: dict[str, object] = {
    KIND_CATEGORICAL: "string",
    KIND_INTEGER: "long",
    KIND_DECIMAL: "double",
    KIND_BOOLEAN: "boolean",
    KIND_TIMESTAMP: {"type": "long", "logicalType": "timestamp-micros"},
    KIND_BLOB: "bytes",
}

VALUE_KINDS = frozenset(AVRO_TYPE_BY_KIND)


@dataclass(frozen=True)
class ColumnDistribution:
    """Declared value shape of one schema field.

    ``cardinality`` is the distinct value count and ``UNBOUNDED_CARDINALITY``
    means one fresh value per row. ``alpha`` is a Zipf exponent over the value
    ranks, so 0.0 is uniform. Every value is a pure function of
    ``(seed, batch, position)``, which is what keeps regeneration bit-identical
    and makes a shard's rows equal to the unsharded corpus's rows.

    Cardinality counts ranks, and a rank is not always a distinct corpus value.
    A numeric column bounds its ranks to ``[minimum, maximum]``, so an unbounded
    numeric column is dense in that range rather than injective over the corpus,
    and a timestamp's ranks are jitter buckets inside one object's arrival slot,
    so its corpus-wide value set is that count times the object count. Only
    columns whose rank determines a value corpus-wide are gated on realized
    cardinality; the incompressibility axis rides on blob and token columns,
    whose unbounded form is injective in the row identity.
    """

    name: str
    kind: str
    role: str = ROLE_GENERIC
    cardinality: int = UNBOUNDED_CARDINALITY
    alpha: float = 0.0
    vocabulary: tuple[str, ...] = ()
    minimum: float = 0.0
    maximum: float = 0.0
    width: int = 0


def reserved_column(name: str) -> ColumnDistribution:
    return ColumnDistribution(name=name, kind=KIND_RESERVED)


def categorical_column(
    name: str, cardinality: int, vocabulary: tuple[str, ...], alpha: float = 0.0, role: str = ROLE_GENERIC
) -> ColumnDistribution:
    return ColumnDistribution(
        name=name, kind=KIND_CATEGORICAL, role=role, cardinality=cardinality, alpha=alpha, vocabulary=vocabulary
    )


def token_column(name: str, width: int, role: str = ROLE_GENERIC) -> ColumnDistribution:
    return ColumnDistribution(
        name=name, kind=KIND_CATEGORICAL, role=role, cardinality=UNBOUNDED_CARDINALITY, width=width
    )


def integer_column(
    name: str, cardinality: int, minimum: int, maximum: int, alpha: float = 0.0, role: str = ROLE_GENERIC
) -> ColumnDistribution:
    return ColumnDistribution(
        name=name,
        kind=KIND_INTEGER,
        role=role,
        cardinality=cardinality,
        alpha=alpha,
        minimum=float(minimum),
        maximum=float(maximum),
    )


def decimal_column(
    name: str, cardinality: int, minimum: float, maximum: float, alpha: float = 0.0, role: str = ROLE_GENERIC
) -> ColumnDistribution:
    return ColumnDistribution(
        name=name, kind=KIND_DECIMAL, role=role, cardinality=cardinality, alpha=alpha, minimum=minimum, maximum=maximum
    )


def boolean_column(name: str, alpha: float = 0.0, cardinality: int = 2) -> ColumnDistribution:
    return ColumnDistribution(name=name, kind=KIND_BOOLEAN, cardinality=cardinality, alpha=alpha)


def timestamp_column(name: str, cardinality: int) -> ColumnDistribution:
    return ColumnDistribution(name=name, kind=KIND_TIMESTAMP, role=ROLE_EVENT_TIME, cardinality=cardinality)


def blob_column(name: str, cardinality: int = UNBOUNDED_CARDINALITY) -> ColumnDistribution:
    return ColumnDistribution(name=name, kind=KIND_BLOB, role=ROLE_PAYLOAD, cardinality=cardinality)


def validate_value_space(column: ColumnDistribution) -> None:
    """Reject a declaration whose value space cannot realize what it declares.

    ``cardinality`` counts ranks, and a rank becomes a distinct value only where
    the kind has somewhere to put it. A numeric range narrower than the rank
    count, a boolean asked for more than two values, a skew over ranks that are
    never drawn, and a knob a kind never reads all declare an axis the corpus
    then flattens — and ``corpus.json`` would still publish the declaration.
    The realized-cardinality gate cannot substitute for this: it skips small
    expectations by design, which is exactly where a collapsed column lands.
    """
    bounded = column.cardinality != UNBOUNDED_CARDINALITY
    if column.alpha > 0.0:
        if not bounded:
            raise ValueError(
                f"column {column.name} declares alpha on an unbounded column, whose values come from the row identity "
                "rather than from ranks"
            )
        if column.cardinality > COLUMN_ZIPF_MAX_CARDINALITY:
            raise ValueError(
                f"column {column.name} declares alpha over cardinality {column.cardinality}, above the "
                f"{COLUMN_ZIPF_MAX_CARDINALITY} a rank CDF may be materialized for"
            )
    if column.kind == KIND_CATEGORICAL:
        if column.minimum or column.maximum:
            raise ValueError(f"column {column.name} is categorical, so it has no numeric range to declare")
        if bounded:
            if not column.vocabulary:
                raise ValueError(f"column {column.name} needs a vocabulary")
            if column.width:
                raise ValueError(
                    f"column {column.name} is a bounded categorical, whose labels come from its vocabulary "
                    "rather than from a width"
                )
        else:
            if column.vocabulary:
                raise ValueError(
                    f"column {column.name} is an unbounded categorical, whose tokens are digests "
                    "rather than vocabulary labels"
                )
            if column.width < MIN_UNBOUNDED_TOKEN_WIDTH:
                raise ValueError(
                    f"column {column.name} needs width >= {MIN_UNBOUNDED_TOKEN_WIDTH} to emit a fresh token per row"
                )
    elif column.kind in {KIND_INTEGER, KIND_DECIMAL}:
        if column.vocabulary or column.width:
            raise ValueError(f"column {column.name} is numeric, so it has no vocabulary or width to declare")
        if column.maximum < column.minimum:
            raise ValueError(f"column {column.name} has an inverted numeric range")
        # Ranks are spread across [minimum, maximum] and then rounded — to a
        # whole number, or to two decimal places — so the range has to hold at
        # least as many values as there are ranks to spread over it. A single
        # rank is the constant column, whose value is the minimum.
        if column.kind == KIND_INTEGER:
            span = int(column.maximum) - int(column.minimum) + 1
        else:
            span = int(round((column.maximum - column.minimum) * 100)) + 1
        needed = column.cardinality if bounded else 2
        if column.cardinality != 1 and needed > span:
            raise ValueError(
                f"column {column.name} needs a numeric range holding at least {needed} values, "
                f"but [{column.minimum}, {column.maximum}] holds {span}"
            )
    elif column.kind == KIND_BOOLEAN:
        if column.vocabulary or column.width or column.minimum or column.maximum:
            raise ValueError(
                f"column {column.name} is boolean, so it has no vocabulary, width or numeric range to declare"
            )
        if column.cardinality not in {1, 2}:
            raise ValueError(
                f"column {column.name} is boolean, so its cardinality must be 1 or 2, not {column.cardinality}"
            )
    elif column.kind == KIND_TIMESTAMP:
        if column.vocabulary or column.width or column.minimum or column.maximum:
            raise ValueError(
                f"column {column.name} is a timestamp, so it has no vocabulary, width or numeric range to declare"
            )
    elif column.kind == KIND_BLOB:
        if column.vocabulary or column.minimum or column.maximum:
            raise ValueError(f"column {column.name} is a blob, so it has no vocabulary or numeric range to declare")
        # The calibrated remainder is a single budget spent on the payload
        # column, so any other blob declares the width it contributes to the row.
        if column.role == ROLE_PAYLOAD:
            if column.width:
                raise ValueError(
                    f"column {column.name} holds the {ROLE_PAYLOAD} role, whose width is the calibrated remainder "
                    "rather than a declaration"
                )
        elif column.width <= 0:
            raise ValueError(
                f"column {column.name} needs a positive width, since only the {ROLE_PAYLOAD} column takes the "
                "calibrated width"
            )


def validate_columns(columns: tuple[ColumnDistribution, ...]) -> None:
    reserved_names = [schema_field["name"] for schema_field in RESERVED_SCHEMA_FIELDS]
    if [column.name for column in columns[: len(reserved_names)]] != reserved_names:
        raise ValueError("the reserved columns must lead the schema, in reserved order")
    generated = columns[len(reserved_names) :]
    if not generated:
        raise ValueError("a schema needs at least one generated column")
    names = [column.name for column in generated]
    if len(set(names)) != len(names):
        raise ValueError("column names must be unique")
    for role, kinds in SINGLETON_ROLES.items():
        holders = [column for column in generated if column.role == role]
        if len(holders) != 1:
            raise ValueError(f"a schema needs exactly one {role} column, found {len(holders)}")
        if holders[0].kind not in kinds:
            raise ValueError(f"column {holders[0].name} cannot hold role {role} with kind {holders[0].kind!r}")
    for column in generated:
        if column.name in RESERVED_FIELDS or column.kind == KIND_RESERVED:
            raise ValueError(f"column {column.name} collides with a reserved column")
        if not column.name.isidentifier():
            raise ValueError(f"column name {column.name!r} is not a valid Avro/SQL identifier")
        if column.kind not in VALUE_KINDS:
            raise ValueError(f"column {column.name} has unknown kind {column.kind!r}")
        if column.role not in ROLES:
            raise ValueError(f"column {column.name} has unknown role {column.role!r}")
        if column.cardinality < 0:
            raise ValueError(f"column {column.name} has a negative cardinality")
        if column.alpha < 0:
            raise ValueError(f"column {column.name} has a negative alpha")
        # A timestamp is the arrival clock, and arrival order belongs to the object
        # stream rather than to a column, so only the event-time column can be one.
        if column.kind == KIND_TIMESTAMP and column.role != ROLE_EVENT_TIME:
            raise ValueError(f"column {column.name} is a timestamp, which only the {ROLE_EVENT_TIME} column may be")
        validate_value_space(column)


RESERVED_COLUMNS: tuple[ColumnDistribution, ...] = tuple(
    reserved_column(schema_field["name"]) for schema_field in RESERVED_SCHEMA_FIELDS
)


def build_columns(configured: tuple[ColumnDistribution, ...]) -> tuple[ColumnDistribution, ...]:
    columns = RESERVED_COLUMNS + configured
    validate_columns(columns)
    return columns


def role_column(columns: tuple[ColumnDistribution, ...], role: str) -> ColumnDistribution:
    return next(column for column in columns if column.role == role)


def schema_fields(columns: tuple[ColumnDistribution, ...]) -> list[SchemaField]:
    generated: list[SchemaField] = [
        {"name": column.name, "type": AVRO_TYPE_BY_KIND[column.kind]}
        for column in columns
        if column.kind != KIND_RESERVED
    ]
    return [*RESERVED_SCHEMA_FIELDS, *generated]


def avro_schema(columns: tuple[ColumnDistribution, ...]) -> dict[str, object]:
    return {
        "type": "record",
        "name": "event",
        "namespace": "ingest_bench",
        "fields": schema_fields(columns),
    }


# ---------------------------------------------------------------------------
# Declaring a schema
# ---------------------------------------------------------------------------

COLUMN_DECLARATION_KEYS = frozenset(
    {"name", "kind", "role", "cardinality", "alpha", "vocabulary", "minimum", "maximum", "width"}
)


def _as_int_value(value: object) -> int:
    return int(cast(int | float | str, value))


def _as_float_value(value: object) -> float:
    return float(cast(int | float | str, value))


def apply_column_override(column: ColumnDistribution, raw: dict[str, object]) -> ColumnDistribution:
    updated = column
    for key, value in raw.items():
        match key:
            case "kind":
                updated = replace(updated, kind=str(value))
            case "role":
                updated = replace(updated, role=str(value))
            case "cardinality":
                updated = replace(updated, cardinality=_as_int_value(value))
            case "alpha":
                updated = replace(updated, alpha=_as_float_value(value))
            case "vocabulary":
                updated = replace(updated, vocabulary=tuple(str(item) for item in cast(list[object], value)))
            case "minimum":
                updated = replace(updated, minimum=_as_float_value(value))
            case "maximum":
                updated = replace(updated, maximum=_as_float_value(value))
            case "width":
                updated = replace(updated, width=_as_int_value(value))
            case _:
                raise ValueError(f"unknown column override key {key!r} for column {column.name}")
    return updated


def column_from_dict(raw: dict[str, object]) -> ColumnDistribution:
    """One declared column: ``name`` and ``kind``, plus any distribution key.

    The kind is what fixes the Avro type, so a declaration cannot describe a
    column the generator would fill with a different type. Every other key is
    optional and falls back to the dataclass default, which is what lets a
    schema file say only what it means to bend.
    """
    name = str(raw["name"])
    unknown = sorted(set(raw) - COLUMN_DECLARATION_KEYS)
    if unknown:
        raise ValueError(f"column {name} declares unknown key(s): {', '.join(unknown)}")
    base = ColumnDistribution(name=name, kind=str(raw["kind"]))
    return apply_column_override(base, {key: value for key, value in raw.items() if key not in {"name", "kind"}})


def load_schema(path: Path) -> tuple[ColumnDistribution, ...]:
    """A schema file is `{"name": str, "columns": [ColumnDistribution as dict, ...]}`; reserved columns are implicit."""
    raw = json.loads(path.read_text())
    declared = tuple(column_from_dict(cast(dict[str, object], entry)) for entry in cast(list[object], raw["columns"]))
    return build_columns(declared)


def apply_column_overrides(
    columns: tuple[ColumnDistribution, ...], overrides: dict[str, dict[str, object]]
) -> tuple[ColumnDistribution, ...]:
    by_name = {column.name: column for column in columns}
    unknown = sorted(set(overrides) - set(by_name))
    if unknown:
        raise ValueError(f"column_overrides name unknown column(s): {', '.join(unknown)}")
    reserved = [name for name in overrides if name in RESERVED_FIELDS]
    if reserved:
        raise ValueError(f"column_overrides may not touch reserved column(s): {', '.join(reserved)}")
    patched = tuple(
        apply_column_override(column, overrides[column.name]) if column.name in overrides else column
        for column in columns
    )
    validate_columns(patched)
    return patched


def iceberg_type_name(avro_type: object) -> str:
    """The Iceberg type each corpus Avro type lands as, published in corpus.json
    for engines that create their own table.
    """
    if avro_type == "int" or avro_type == "long":
        return "long"
    if avro_type == "double":
        return "double"
    if avro_type == "boolean":
        return "boolean"
    if avro_type == "string":
        return "string"
    if avro_type == "bytes":
        return "binary"
    if isinstance(avro_type, dict) and avro_type.get("logicalType") == "timestamp-micros":
        return "timestamp"
    raise ValueError(f"unsupported corpus Avro type: {avro_type!r}")
