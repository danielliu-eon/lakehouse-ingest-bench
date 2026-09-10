# SPDX-License-Identifier: Apache-2.0
"""Create Iceberg tables from published corpus schemas.

Use recorded corpus columns and types, then apply the run's partitioning and
table properties. This avoids depending on presets edited after generation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    IcebergType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from ingest_bench.catalog import open_catalog, table_identifier
from ingest_bench.corpus.metadata import CorpusMetadata

IDENTITY = "identity"
BUCKET = "bucket"
UNPARTITIONED = "unpartitioned"

PARTITION_RE = (
    r"^(?:(identity)\(([A-Za-z_][A-Za-z0-9_]*)\)"
    r"|(bucket)\((\d+)\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\)"
    r"|(unpartitioned))$"
)

# Use Iceberg's initial partition-field ID for consistent table specs.
PARTITION_FIELD_ID = 1000

# The Iceberg type each type name a corpus publishes lands as.
_TYPES: dict[str, IcebergType] = {
    "long": LongType(),
    "string": StringType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
    "binary": BinaryType(),
}


@dataclass(frozen=True)
class Partition:
    """Requested partition transform before binding to a schema."""

    transform: str
    column: str | None
    buckets: int | None

    def __post_init__(self) -> None:
        if self.transform == UNPARTITIONED:
            if self.column is not None or self.buckets is not None:
                raise ValueError("an unpartitioned scheme names no column and no bucket count")
        elif self.transform == IDENTITY:
            if self.column is None or self.buckets is not None:
                raise ValueError("an identity scheme names a column and no bucket count")
        elif self.transform == BUCKET:
            if self.column is None or self.buckets is None:
                raise ValueError("a bucket scheme names a column and a bucket count")
        else:
            raise ValueError(f"unknown partition transform {self.transform!r}")

    def source_column(self) -> str:
        if self.column is None:
            raise ValueError(f"a {self.transform} scheme has no source column")
        return self.column

    def bucket_count(self) -> int:
        if self.buckets is None:
            raise ValueError(f"a {self.transform} scheme has no bucket count")
        return self.buckets


def parse_partition(text: str) -> Partition:
    """Parse a supported benchmark partition transform.

    Restrict transforms to those covered by corpus partition truth.
    """
    match = re.match(PARTITION_RE, text.strip())
    if match is None:
        raise ValueError(f"partition must be identity(<column>), bucket(<N>, <column>) or unpartitioned, got {text!r}")
    identity, identity_column, _bucket, buckets, bucket_column, unpartitioned = match.groups()
    if unpartitioned is not None:
        return Partition(UNPARTITIONED, None, None)
    if identity is not None:
        return Partition(IDENTITY, identity_column, None)
    return Partition(BUCKET, bucket_column, int(buckets))


def iceberg_schema(meta: CorpusMetadata) -> Schema:
    """Build required fields in corpus order with stable field IDs."""
    fields: list[NestedField] = []
    for index, name in enumerate(meta.field_names()):
        published = meta.iceberg_types[name]
        if published not in _TYPES:
            raise ValueError(f"corpus column {name!r} publishes type {published!r}, which cannot be created")
        fields.append(NestedField(index + 1, name, _TYPES[published], required=True))
    return Schema(*fields)


def partition_spec(meta: CorpusMetadata, partition: Partition) -> PartitionSpec:
    """Bind the requested transform to the Iceberg schema field IDs."""
    if partition.transform == UNPARTITIONED:
        return PartitionSpec()
    names = meta.field_names()
    column = partition.source_column()
    if column not in names:
        raise ValueError(f"partition column {column!r} is not in the corpus schema; columns are {names}")
    source_id = names.index(column) + 1
    if partition.transform == IDENTITY:
        return PartitionSpec(
            PartitionField(source_id=source_id, field_id=PARTITION_FIELD_ID, transform=IdentityTransform(), name=column)
        )
    return PartitionSpec(
        PartitionField(
            source_id=source_id,
            field_id=PARTITION_FIELD_ID,
            transform=BucketTransform(num_buckets=partition.bucket_count()),
            name=f"{column}_bucket",
        )
    )


def _namespace_properties(
    props: dict[str, str], namespace: str, namespace_location: str | None, location: str | None
) -> dict[str, str]:
    """Choose namespace properties from an explicit location or storage warehouse.

    Only URI warehouses can place namespaces; Glue REST may use an account ID.
    If neither is available, use the parent of an explicit table location.
    """
    if namespace_location is not None:
        return {"location": namespace_location}
    warehouse = props.get("warehouse")
    if warehouse is not None and "://" in warehouse:
        return {"location": f"{warehouse}/{namespace}"}
    if location is not None:
        return {"location": location.rstrip("/").rsplit("/", 1)[0]}
    return {}


def create_table(
    props: dict[str, str],
    table: str,
    meta: CorpusMetadata,
    partition: Partition,
    properties: dict[str, str],
    location: str | None = None,
    namespace_location: str | None = None,
) -> Table:
    """Create the namespace if needed, then create the table with its properties.

    ``location`` is the table root; ``namespace_location`` is the shared namespace
    root. Keep them separate so later tables do not inherit one run's directory.
    """
    catalog = open_catalog(props)
    namespace, name = table_identifier(table)
    schema = iceberg_schema(meta)
    spec = partition_spec(meta, partition)
    try:
        catalog.create_namespace(
            namespace, properties=_namespace_properties(props, namespace, namespace_location, location)
        )
    except NamespaceAlreadyExistsError:
        pass
    return catalog.create_table(
        (namespace, name), schema=schema, partition_spec=spec, location=location, properties=properties
    )


def drop_table(props: dict[str, str], table: str) -> None:
    """Delete the table if present, allowing teardown retries."""
    catalog = open_catalog(props)
    try:
        catalog.drop_table(table_identifier(table))
    except NoSuchTableError:
        pass
