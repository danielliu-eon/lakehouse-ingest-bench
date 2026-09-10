# SPDX-License-Identifier: Apache-2.0
"""Render Spark SQL equivalent to harness table creation.

Preserve corpus field types, required status, partitioning, and properties
for engines that create their own tables.
"""

from __future__ import annotations

from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.table.create import IDENTITY, UNPARTITIONED, Partition

# Use TIMESTAMP_NTZ to match Iceberg timestamp without a timezone.
_TYPES = {
    "long": "BIGINT",
    "string": "STRING",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP_NTZ",
    "binary": "BINARY",
}


def _partition_clause(partition: Partition) -> str:
    """The ``PARTITIONED BY`` argument for a scheme, empty when there is none."""
    if partition.transform == UNPARTITIONED:
        return ""
    if partition.transform == IDENTITY:
        return partition.source_column()
    return f"bucket({partition.bucket_count()}, {partition.source_column()})"


def spark_sql_ddl(meta: CorpusMetadata, table: str, partition: Partition, properties: dict[str, str]) -> str:
    """The ``CREATE TABLE`` statement for this corpus, scheme and properties."""
    columns = []
    for name in meta.field_names():
        published = meta.iceberg_types[name]
        if published not in _TYPES:
            raise ValueError(f"corpus column {name!r} publishes type {published!r}, which has no Spark SQL name")
        columns.append(f"  {name} {_TYPES[published]} NOT NULL")
    lines = [f"CREATE TABLE {table} (", ",\n".join(columns), ") USING iceberg"]
    clause = _partition_clause(partition)
    if clause:
        lines.append(f"PARTITIONED BY ({clause})")
    if properties:
        rendered = ", ".join(f"'{key}' = '{value}'" for key, value in properties.items())
        lines.append(f"TBLPROPERTIES ({rendered})")
    return "\n".join(lines)
