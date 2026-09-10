# SPDX-License-Identifier: Apache-2.0
"""Read table schema, commit history, and added data files.

Use table metadata and manifests as evidence of visible data, independently
of the engine writer.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFileContent, ManifestEntryStatus
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import Snapshot, Summary

from ingest_bench.catalog import open_catalog, table_identifier
from ingest_bench.corpus.metadata import CorpusMetadata


@dataclass(frozen=True)
class SnapshotInfo:
    """Commit metadata used by scoring, independent of optional summary row counts."""

    snapshot_id: int
    parent_id: int | None
    timestamp_ms: int
    operation: str


@dataclass(frozen=True)
class AddedFile:
    """Data-file metadata from the snapshot that added it."""

    path: str
    file_format: str
    record_count: int
    size_bytes: int


def load_table(props: dict[str, str], table: str) -> Table:
    return open_catalog(props).load_table(table_identifier(table))


def read_metadata(table: Table) -> TableMetadata:
    """Read full metadata through the table's configured IO.

    REST catalogs may hydrate only the current snapshot; scoring needs the
    complete history in ``metadata_location``.
    """
    return FromInputFile.table_metadata(table.io.new_input(table.metadata_location))


def check_table_schema(schema: Schema, meta: CorpusMetadata) -> list[str]:
    """Find missing, renamed, retyped, or nullable corpus columns.

    Allow extra columns, but require each corpus field to retain its name, type,
    and required status. Compare published type names with Iceberg primitives.
    """
    held = {field.name: field for field in schema.fields}
    mismatches: list[str] = []
    for name in meta.field_names():
        published = meta.iceberg_types[name]
        if name not in held:
            mismatches.append(f"the table has no column {name!r}, which the corpus publishes as {published}")
            continue
        field = held[name]
        if str(field.field_type) != published:
            mismatches.append(f"column {name!r} is {field.field_type} in the table and {published} in the corpus")
        if not field.required:
            mismatches.append(f"column {name!r} is optional, corpus columns are required")
    return mismatches


def _summary(snapshot: Snapshot) -> Summary:
    """Read the required snapshot summary without defaulting its operation."""
    if snapshot.summary is None:
        raise ValueError(f"snapshot {snapshot.snapshot_id} carries no summary, so its operation is unknown")
    return snapshot.summary


def snapshots_in_order(metadata: TableMetadata) -> list[SnapshotInfo]:
    """Sort snapshots by commit sequence, then timestamp.

    Sequence numbers preserve order when writer clocks disagree or timestamps tie.
    """
    ordered = sorted(metadata.snapshots, key=lambda snapshot: (snapshot.sequence_number or 0, snapshot.timestamp_ms))
    infos: list[SnapshotInfo] = []
    for snapshot in ordered:
        summary = _summary(snapshot)
        infos.append(
            SnapshotInfo(
                snapshot_id=snapshot.snapshot_id,
                parent_id=snapshot.parent_snapshot_id,
                timestamp_ms=snapshot.timestamp_ms,
                operation=summary.operation.value,
            )
        )
    return infos


def snapshot_by_id(metadata: TableMetadata, snapshot_id: int) -> Snapshot:
    for snapshot in metadata.snapshots:
        if snapshot.snapshot_id == snapshot_id:
            return snapshot
    raise ValueError(f"snapshot {snapshot_id} is not in the metadata at {metadata.location}")


def added_files(metadata: TableMetadata, snapshot_id: int, io: FileIO) -> list[AddedFile]:
    """Yield data files newly added by this snapshot.

    Filter by entry status and snapshot ID to avoid recounting inherited files.
    Skip delete files, which do not contain the scored data rows.
    """
    added: list[AddedFile] = []
    for manifest in snapshot_by_id(metadata, snapshot_id).manifests(io):
        # Skip manifests from other commits to keep reads linear. Rewritten manifests
        # carry inherited entries as EXISTING. Parse older lists without this field
        # to avoid dropping files; the entry filter remains authoritative.
        if manifest.added_snapshot_id is not None and manifest.added_snapshot_id != snapshot_id:
            continue
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            if entry.status != ManifestEntryStatus.ADDED or entry.snapshot_id != snapshot_id:
                continue
            data_file = entry.data_file
            if data_file.content != DataFileContent.DATA:
                continue
            added.append(
                AddedFile(
                    path=data_file.file_path,
                    file_format=data_file.file_format.name.lower(),
                    record_count=data_file.record_count,
                    size_bytes=data_file.file_size_in_bytes,
                )
            )
    return added
