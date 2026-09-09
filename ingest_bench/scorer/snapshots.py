"""A table's shape and its commit history, and which data files each commit added.

The scorer never asks a writer what it wrote. Freshness is the wall time of a
commit and exactness is the rows that commit made visible, so both are read out
of the table's own metadata and manifests — the same surface any reader of the
table sees. Trusting a writer's report instead would score the engine's
bookkeeping rather than the table it produced. The column set is read the same
way and for the same reason.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFileContent, ManifestEntryStatus
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import ADDED_RECORDS, TOTAL_RECORDS, Snapshot, Summary

from ingest_bench.catalog import open_catalog, table_identifier
from ingest_bench.corpus.metadata import CorpusMetadata


@dataclass(frozen=True)
class SnapshotInfo:
    """One commit, reduced to what the score is computed from."""

    snapshot_id: int
    parent_id: int | None
    timestamp_ms: int
    operation: str
    total_records: int | None
    added_records: int | None


@dataclass(frozen=True)
class AddedFile:
    """A data file, as the snapshot that added it describes it."""

    path: str
    file_format: str
    record_count: int


def load_table(props: dict[str, str], table: str) -> Table:
    return open_catalog(props).load_table(table_identifier(table))


def read_metadata(table: Table) -> TableMetadata:
    """The whole metadata document the catalog pointed at.

    A catalog may hand back a table object carrying only the current snapshot —
    the REST protocol has a mode that does exactly that — while the score needs
    every commit in order to time each batch's arrival. Parsing the document at
    ``metadata_location`` again yields the full history whatever the catalog
    chose to hydrate, and going through ``table.io`` keeps that read on the
    credentials and endpoint the catalog handed out.
    """
    return FromInputFile.table_metadata(table.io.new_input(table.metadata_location))


def check_table_schema(schema: Schema, meta: CorpusMetadata) -> list[str]:
    """Every way the table's columns depart from the ones the corpus publishes.

    An engine that creates its own table chooses the column set, and a table
    that dropped, renamed or retyped a column still carries the ids the tally
    is built from — so the run would score exact and fresh against a table
    that is not the one the corpus describes. Extra columns are allowed: the
    contract is that the corpus's columns survive one to one, not that nothing
    else may be added.

    A corpus column must also be required, as the corpus's own schema declares
    it. Nullability is not cosmetic: an optional column is encoded with
    definition levels and is a candidate for a different page layout, so the
    file geometry two runs are compared on stops being a fact about their
    engines. It is also what would let a writer that dropped a value commit
    anyway, and the loss would read as a null rather than as a fault.

    ``str`` of an Iceberg primitive type is the same name the corpus publishes,
    which is what lets the comparison stay a string one rather than needing a
    second copy of the type map that built the table.
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
    """The commit's summary, which the operation and the row counts are read from.

    A summary is optional only in v1 metadata, which no engine under test
    writes. Refusing one that is missing keeps the operation a fact about the
    commit: defaulting it would score a rewrite or a delete as an append.
    """
    if snapshot.summary is None:
        raise ValueError(f"snapshot {snapshot.snapshot_id} carries no summary, so its operation is unknown")
    return snapshot.summary


def _summary_int(summary: Summary, key: str) -> int | None:
    """One numeric summary property, or ``None`` where the writer omitted it.

    These properties are optional by spec and engines differ over which they
    write, so absence is reported rather than defaulted: a zero here would read
    as a commit that added nothing.
    """
    raw = summary.get(key)
    return None if raw is None else int(raw)


def snapshots_in_order(metadata: TableMetadata) -> list[SnapshotInfo]:
    """Every snapshot the table holds, in commit order.

    Sequence number leads the sort because the commit assigns it, while the
    timestamp is stamped by whichever machine wrote the metadata. Two commits
    landing inside one millisecond, or from clocks that disagree, would
    otherwise be ordered by wall time rather than by what happened first.
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
                total_records=_summary_int(summary, TOTAL_RECORDS),
                added_records=_summary_int(summary, ADDED_RECORDS),
            )
        )
    return infos


def _snapshot_by_id(metadata: TableMetadata, snapshot_id: int) -> Snapshot:
    for snapshot in metadata.snapshots:
        if snapshot.snapshot_id == snapshot_id:
            return snapshot
    raise ValueError(f"snapshot {snapshot_id} is not in the metadata at {metadata.location}")


def added_files(metadata: TableMetadata, snapshot_id: int, io: FileIO) -> list[AddedFile]:
    """The data files this snapshot added, and none it merely inherited.

    A manifest stays reachable from every snapshot after the one that wrote it,
    so a snapshot's manifest list is the whole live table rather than its own
    contribution. The entry's status and snapshot id are what attribute a file
    to one commit; taking the manifest list whole would count every earlier file
    again at each commit, and a table receiving a steady stream would score as
    one growing quadratically.

    Delete files are skipped: their record counts describe rows being removed,
    and a position-delete file carries no row ids at all.
    """
    added: list[AddedFile] = []
    for manifest in _snapshot_by_id(metadata, snapshot_id).manifests(io):
        # The entry filter below is the truth; this one is what keeps the score
        # loop linear. A manifest is immutable, so an ADDED entry can only live
        # in the manifest the same commit wrote — a manifest rewrite carries
        # entries forward as EXISTING. Fetching the rest would parse every live
        # manifest at every commit, and the read cost would grow with the
        # square of the run. A manifest list old enough to omit the field is
        # still parsed, since skipping it could drop a file.
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
                )
            )
    return added
