# SPDX-License-Identifier: Apache-2.0
"""The shape of the files a run left behind, sampled along the run.

Freshness says whether the table was current and exactness whether it was
right; neither says whether it is readable. A fleet can meet a lag bound by
committing thousands of tiny files a minute, and every cost of that choice
falls on the next reader of the table rather than on the run that made it. So
geometry is reported as a curve over the run and not once at the end: a fleet
that starts coarse and a fleet that degrades are different results, and only a
ladder of points tells them apart.

Everything here is read from one metadata document and the manifests it names,
so it costs no table scan and can be run after the engine, its cluster and its
catalog are gone.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.manifest import DataFileContent
from pyiceberg.serializers import FromInputFile
from pyiceberg.table.metadata import TableMetadata

from ingest_bench.scorer.snapshots import SnapshotInfo, added_files, snapshot_by_id, snapshots_in_order

GEOMETRY_FILE = "geometry.json"

# What an offset the run never reached is reported as, rather than a point.
ABSENT = "absent"

MIB = 1024 * 1024

_QUANTILES = (("p50", 0.5), ("p90", 0.9), ("p99", 0.99))


@dataclass(frozen=True)
class DataFileInfo:
    """One data file, as the manifest entry pointing at it describes it.

    ``added_snapshot_id`` is absent where a manifest list old enough to omit
    the field left the entry unattributed. Defaulting it would credit one
    commit with another's files.
    """

    path: str
    size_bytes: int
    record_count: int
    added_snapshot_id: int | None


def open_metadata_document(location: str, props: dict[str, str]) -> tuple[TableMetadata, FileIO]:
    """One metadata document and an IO for the files it names, with no catalog.

    A run's geometry is read from the document a teardown copied, so the read
    needs object-store access and nothing else — the catalog may have been left
    behind with the table, and asking it would make a figure about files depend
    on a service that holds none.

    This is what ``StaticTable.from_metadata`` does past its own name check,
    which reads a location not ending in ``.metadata.json`` as a table root and
    goes looking for a version hint beside it. A copied document is named for
    the run, so that check would refuse every document the harness writes.
    The IO is built from the given properties merged with the document's own,
    so a table property naming an endpoint or a region is honoured the way a
    catalog-loaded table honours it.
    """
    metadata = FromInputFile.table_metadata(load_file_io(properties=props, location=location).new_input(location))
    return metadata, load_file_io({**props, **metadata.properties}, location=location)


def data_files_at(metadata: TableMetadata, snapshot_id: int, io: FileIO) -> list[DataFileInfo]:
    """Every live data file as of one snapshot: the whole table, not one commit.

    A snapshot's manifest list reaches every manifest still live at that
    commit, which is exactly what a reader of the table at that instant would
    open, so the list is taken whole here. That is the opposite of what
    ``added_files`` wants, and the reason the two walks are separate rather
    than one with a flag.

    Delete files are skipped: their sizes describe rows being removed, and
    counting them among the table's files would report the geometry of the
    bookkeeping rather than of the data.
    """
    files: list[DataFileInfo] = []
    for manifest in snapshot_by_id(metadata, snapshot_id).manifests(io):
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            data_file = entry.data_file
            if data_file.content != DataFileContent.DATA:
                continue
            files.append(
                DataFileInfo(
                    path=data_file.file_path,
                    size_bytes=data_file.file_size_in_bytes,
                    record_count=data_file.record_count,
                    added_snapshot_id=entry.snapshot_id,
                )
            )
    return files


def _prefix_end(ordered: Sequence[SnapshotInfo], at_ms: int) -> int | None:
    """Index of the last commit an instant had seen, or ``None`` before the first.

    A table's state at an instant is a prefix of its commit history, so the
    walk stops at the first commit that had not happened yet rather than
    scanning on for a later commit with an earlier timestamp. Taking the newest
    such timestamp instead would answer with a commit that a reader at that
    instant could not have seen, on the strength of a clock that disagrees.
    """
    end: int | None = None
    for index, info in enumerate(ordered):
        if info.timestamp_ms > at_ms:
            break
        end = index
    return end


def _quantiles(values: Sequence[int]) -> dict[str, float | None]:
    """p50/p90/p99 of a set of counts or sizes, or nulls where there are none.

    ``numpy.quantile``'s default linear interpolation, so a quantile falling
    between two files is a point on the line between them: a share of a run's
    files, not the nearest one, is what stays comparable across runs whose file
    counts differ. Sizes and counts are integers well under 2^53, so the
    float64 the interpolation runs in carries them exactly.
    """
    if not values:
        return {name: None for name, _ in _QUANTILES}
    array = np.array(values, dtype=np.float64)
    return {name: float(np.quantile(array, q)) for name, q in _QUANTILES}


def _log2_histogram(sizes: Sequence[int]) -> dict[str, int]:
    """How many files fall in each ``[2^k, 2^(k+1))`` band, empty bands omitted.

    Log-scaled because the question is which order of magnitude a run's files
    sit at, and a linear histogram of sizes spanning kilobytes to hundreds of
    megabytes answers it with one occupied bucket. ``bit_length`` is the exact
    floor of the base-2 logarithm, so the band a file lands in never depends on
    a floating-point rounding.
    """
    bands = Counter(size.bit_length() - 1 for size in sizes)
    return {f"2^{band}..2^{band + 1}": bands[band] for band in sorted(bands)}


def _small_file_share(sizes: Sequence[int], limit: int) -> float | None:
    """The fraction of files under a size, by file and not by byte.

    The cost a small file stands for is one open and one footer read, which a
    byte-weighted share would hide behind the few large files that hold most of
    the data.
    """
    if not sizes:
        return None
    return sum(1 for size in sizes if size < limit) / len(sizes)


def live_geometry(files: Sequence[DataFileInfo]) -> dict[str, object]:
    """What a reader of the table would find, from its live data files."""
    sizes = [info.size_bytes for info in files]
    return {
        "files": len(files),
        "rows": sum(info.record_count for info in files),
        "bytes": sum(sizes),
        "size_quantiles": {
            **_quantiles(sizes),
            "min": min(sizes) if sizes else None,
            "max": max(sizes) if sizes else None,
        },
        # 32 MiB is around where a scan starts paying more for opening files
        # than for reading them; 8 MiB separates a table that merely writes
        # small files from one a reader cannot use.
        "small_file_share_32mib": _small_file_share(sizes, 32 * MIB),
        "small_file_share_8mib": _small_file_share(sizes, 8 * MIB),
        "log2_histogram": _log2_histogram(sizes),
    }


def _per_commit(metadata: TableMetadata, io: FileIO, commits: Sequence[SnapshotInfo]) -> dict[str, object]:
    """The commits since the previous point, by what each of them added.

    This is the half of geometry the live set cannot show. A table compacted
    behind the writer reads tidy at every point while the writer is still
    committing thousands of files a minute, and it is the commit stream that
    says which of the two the engine did.
    """
    added_counts: list[int] = []
    sizes: list[int] = []
    for info in commits:
        files = added_files(metadata, info.snapshot_id, io)
        added_counts.append(len(files))
        sizes.extend(added.size_bytes for added in files)
    return {
        "commits": len(commits),
        "files_added_quantiles": _quantiles(added_counts),
        "file_size_quantiles": _quantiles(sizes),
    }


def _point(
    metadata: TableMetadata, io: FileIO, info: SnapshotInfo, commits: Sequence[SnapshotInfo]
) -> dict[str, object]:
    return {
        "snapshot_id": info.snapshot_id,
        "timestamp_ms": info.timestamp_ms,
        "live": live_geometry(data_files_at(metadata, info.snapshot_id, io)),
        "per_commit": _per_commit(metadata, io, commits),
    }


def geometry_report(
    metadata: TableMetadata, io: FileIO, epoch_ms: int, offsets_s: Sequence[int], final: bool = True
) -> dict[str, object]:
    """The ladder of geometry points, and the table as the run left it.

    Every run reports the same offsets so two runs' geometry columns line up,
    which means a shorter run has to say that a rung is missing rather than
    filling it. An offset past the last commit is therefore ``absent`` and not
    the final snapshot under another name: repeating the end of the run at
    every rung it never reached would read as a table that stopped changing.

    An absent rung leaves the per-commit boundary where it was, so the commits
    it would have covered are reported by the next point that is present, and
    every commit of the run belongs to exactly one point. That partition is
    what makes the ladder have to ascend: an offset behind the one before it
    would take its commits from the point that had already reported them.
    """
    if any(later <= earlier for earlier, later in zip(offsets_s, offsets_s[1:], strict=False)):
        raise ValueError(f"the geometry offsets must ascend, got {list(offsets_s)}")
    ordered = snapshots_in_order(metadata)
    at: dict[str, object] = {str(offset_s): ABSENT for offset_s in offsets_s}
    report: dict[str, object] = {
        "epoch_ms": epoch_ms,
        "offsets_s": list(offsets_s),
        "at": at,
        "final": None,
    }
    if not ordered:
        return report
    last_ms = ordered[-1].timestamp_ms
    boundary = 0
    for offset_s in offsets_s:
        point_ms = epoch_ms + offset_s * 1000
        if point_ms > last_ms:
            continue
        reached = _prefix_end(ordered, point_ms)
        if reached is None:
            continue
        at[str(offset_s)] = _point(metadata, io, ordered[reached], ordered[boundary : reached + 1])
        boundary = reached + 1
    if final:
        report["final"] = _point(metadata, io, ordered[-1], ordered[boundary:])
    return report
