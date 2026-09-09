"""Render the Spark SQL that creates the same table ``create`` would.

An engine that manages its own table is given DDL rather than a pre-created
table, because its own writer path applies table properties only to a table it
created. The rendered statement therefore has to agree with `create` on the
column set, the required-ness and the partition scheme, or the two engines in
one run would be scored against differently shaped tables.
"""

from __future__ import annotations

from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.table.create import IDENTITY, UNPARTITIONED, Partition

# The Spark SQL type each type name a corpus publishes is declared as.
# `TIMESTAMP_NTZ` rather than `TIMESTAMP`: the corpus carries wall-clock
# milliseconds with no zone, and Spark's zoned `TIMESTAMP` maps to Iceberg's
# `timestamptz`, which is a different Iceberg type than `create` builds.
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
