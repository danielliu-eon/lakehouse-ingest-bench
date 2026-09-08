"""Create the Iceberg table a corpus is fed into, and drop it again.

The table is the contract between the corpus and the engine under test: its
column set and types come from what the corpus published, and its partition
spec and table properties are read back out of the catalog by every engine, so
this is the one place a run's scheme and encoding are set. Deriving the schema
from a preset instead would let a preset edited after generation give the
writer one schema and the engines another.
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

# Iceberg numbers partition fields from 1000, and a table created with a single
# partition field starts there. Pinning it keeps the spec of one run's table
# comparable with another's.
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
    """A partition scheme as a run asked for it, before a schema binds it."""

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
    """The scheme a ``--partition`` argument names.

    Only the transforms the benchmark scores are accepted. A time transform
    would partition on a column the generator draws, so the partition truth
    the corpus published would stop describing where its rows land.
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
    """The table's columns, in the corpus schema's order and with its field ids.

    Every field is required. A row the corpus wrote carries a value in every
    column, so an optional column would let an engine that dropped one still
    commit, and the scorer would read the loss as a null rather than a fault.
    """
    fields: list[NestedField] = []
    for index, name in enumerate(meta.field_names()):
        published = meta.iceberg_types[name]
        if published not in _TYPES:
            raise ValueError(f"corpus column {name!r} publishes type {published!r}, which cannot be created")
        fields.append(NestedField(index + 1, name, _TYPES[published], required=True))
    return Schema(*fields)


def partition_spec(meta: CorpusMetadata, partition: Partition) -> PartitionSpec:
    """The scheme bound to the field ids ``iceberg_schema`` assigns."""
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


def _namespace_properties(props: dict[str, str], namespace: str, location: str | None) -> dict[str, str]:
    """What the namespace is created with, which is a location or nothing.

    Some catalogs reject a namespace that names no location, so one is derived
    when the catalog's ``warehouse`` is a storage URI. Only such a warehouse
    can carry a namespace under it: a catalog that resolves ``warehouse`` as a
    catalog identifier instead (AWS Glue's Iceberg REST endpoint reads an
    account id there) would yield a location in no bucket, and there the
    namespace legitimately has none.
    """
    if location is not None:
        return {"location": location}
    warehouse = props.get("warehouse")
    if warehouse is not None and "://" in warehouse:
        return {"location": f"{warehouse}/{namespace}"}
    return {}


def create_table(
    props: dict[str, str],
    table: str,
    meta: CorpusMetadata,
    partition: Partition,
    properties: dict[str, str],
    location: str | None = None,
) -> Table:
    """The created table, with its namespace created first if it was missing.

    ``properties`` take effect only here. An engine writing into a table it did
    not create applies no table properties of its own, so a codec or a
    row-group size set anywhere else silently does nothing.
    """
    catalog = open_catalog(props)
    namespace, name = table_identifier(table)
    schema = iceberg_schema(meta)
    spec = partition_spec(meta, partition)
    try:
        catalog.create_namespace(namespace, properties=_namespace_properties(props, namespace, location))
    except NamespaceAlreadyExistsError:
        pass
    return catalog.create_table(
        (namespace, name), schema=schema, partition_spec=spec, location=location, properties=properties
    )


def drop_table(props: dict[str, str], table: str) -> None:
    """Drop the table, or accept that it is already gone.

    A teardown also runs after a run that failed before creating anything, and
    again when a retry re-enters it, so absence is the intended end state
    rather than an error.
    """
    catalog = open_catalog(props)
    try:
        catalog.drop_table(table_identifier(table))
    except NoSuchTableError:
        pass
