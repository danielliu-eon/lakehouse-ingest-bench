# SPDX-License-Identifier: Apache-2.0
"""Declare and validate generated column distributions.

Schema files list generated columns; reserved columns are implicit and come
first. Validate declarations before generation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TypedDict, cast

# Checksum modulus; changing it changes published scoring truth.
P = 1_000_000_007

# Reserve a disjoint ID range per batch: batch * ID_BLOCK + position.
ID_BLOCK = 2**32


class SchemaField(TypedDict):
    name: str
    type: object


# Keep the scoring ID and partition-truth fields fixed in every schema.
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

# Reserved fields are computed from row position.
RESERVED_FIELDS = frozenset({"id", "partition_key"})

# Roles let queries and generation logic identify columns independently of names.
ROLE_GENERIC = "generic"
ROLE_EVENT_TIME = "event_time"
ROLE_ENTITY = "entity"
ROLE_SUM_MEASURE = "sum_measure"
ROLE_PAYLOAD = "payload"

ROLES = frozenset({ROLE_GENERIC, ROLE_EVENT_TIME, ROLE_ENTITY, ROLE_SUM_MEASURE, ROLE_PAYLOAD})

# Require one event clock, lookup key, aggregate measure, and calibrated payload.
SINGLETON_ROLES: dict[str, frozenset[str]] = {
    ROLE_EVENT_TIME: frozenset({KIND_TIMESTAMP}),
    ROLE_ENTITY: frozenset({KIND_CATEGORICAL}),
    ROLE_SUM_MEASURE: frozenset({KIND_INTEGER, KIND_DECIMAL}),
    ROLE_PAYLOAD: frozenset({KIND_BLOB}),
}

# Zero selects the kind-specific unbounded distribution.
UNBOUNDED_CARDINALITY = 0

# A 16-character hex token provides a 64-bit value space. Enforce this
# minimum to limit collisions in unbounded token columns.
MIN_UNBOUNDED_TOKEN_WIDTH = 16

# Bound memory for materialized Zipf CDFs. Uniform ranks use modulus
# and do not need this allocation.
COLUMN_ZIPF_MAX_CARDINALITY = 1_000_000

# Derive the Avro type from the value kind.
AVRO_TYPE_BY_KIND: dict[str, object] = {
    KIND_CATEGORICAL: "string",
    KIND_INTEGER: "long",
    KIND_DECIMAL: "double",
    KIND_BOOLEAN: "boolean",
    KIND_TIMESTAMP: {"type": "long", "logicalType": "timestamp-millis"},
    KIND_BLOB: "bytes",
}

VALUE_KINDS = frozenset(AVRO_TYPE_BY_KIND)


@dataclass(frozen=True)
class ColumnDistribution:
    """Distribution and semantic role of one generated field.

    ``cardinality`` counts ranks; zero selects the unbounded form. ``alpha`` is
    the Zipf exponent, with zero selecting uniform ranks. Generation is
    deterministic for each row position.

    Ranks need not equal distinct corpus values: numeric values are bounded by
    their range, and timestamps map ranks to millisecond buckets within each
    batch window. Realized-cardinality checks apply only where the mapping can
    be compared meaningfully.
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
    """Reject distributions that the column kind cannot realize.

    Check ranges, cardinality, skew, and unsupported options before sampling;
    realized-cardinality gates skip small expectations and cannot replace this
    validation.
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
        # The rounded numeric range must accommodate the declared ranks.
        # A single-rank column is constant at the minimum.
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
        # Only the payload role uses calibrated width; other blobs declare their own.
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
        # Timestamp generation depends on the batch arrival window.
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
    """Read required ``name`` and ``kind`` fields and optional distribution settings.

    Reject unknown keys and use dataclass defaults for omitted settings.
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
    """Return the Iceberg type name published for an Avro field."""
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
    # Iceberg timestamps use microsecond precision; widen millisecond values.
    if isinstance(avro_type, dict) and avro_type.get("logicalType") == "timestamp-millis":
        return "timestamp"
    raise ValueError(f"unsupported corpus Avro type: {avro_type!r}")
