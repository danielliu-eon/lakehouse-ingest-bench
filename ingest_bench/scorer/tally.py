# SPDX-License-Identifier: Apache-2.0
"""Track per-batch coverage and exactness from committed row IDs.

Accumulate counts and row-ID checksums modulo a prime as snapshots arrive.
Together they detect loss, duplication, and many substitutions without
rescanning the table.

Coverage means the observed count has reached the expected count; it drives
the freshness watermark. Completeness requires matching counts and checksums.
These checks do not prove row-set identity, and late duplicates do not move
the coverage watermark backward.
"""

from __future__ import annotations

import numpy as np
import pyarrow.fs as pa_fs
import pyarrow.orc as pa_orc
import pyarrow.parquet as pa_parquet

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus.generate import BatchRecord

ID_COLUMN = "id"

LOSS = "loss"
DUPLICATION = "duplication"
CORRUPTION = "corruption"

PARQUET = "parquet"
ORC = "orc"

# Float64 weighted counts are exact only below 2**53. Chunk well below
# roughly nine million residues per sum to preserve checksum equality.
_CHUNK_ROWS = 4_000_000


class BatchTally:
    """Track row counts and checksums for the manifest's contiguous batch IDs.

    Decode batch ownership from the high bits of each row ID.
    """

    def __init__(self, records: list[BatchRecord], p: int) -> None:
        held = [record.batch for record in records]
        if held != list(range(len(records))):
            raise ValueError(f"a tally needs batches 0..{len(records) - 1} in order, and was given {held[:8]}")
        self.p = p
        self.records = records
        count = len(records)
        self.expected_rows = np.array([record.rows for record in records], dtype=np.int64)
        self.expected_checksum = np.array([record.checksum for record in records], dtype=np.int64)
        self.counts = np.zeros(count, dtype=np.int64)
        self.sum_mod = np.zeros(count, dtype=np.int64)
        self._prefix = -1

    def add_ids(self, ids: np.ndarray) -> None:
        """Accumulate counts and checksums by encoded batch ID."""
        flat = np.asarray(ids).reshape(-1)
        if flat.size == 0:
            return
        if flat.dtype.kind not in ("i", "u"):
            raise ValueError(f"row ids must be integers, and arrived as {flat.dtype}")
        flat = flat.astype(np.int64, copy=False)
        batches = flat // c.ID_BLOCK
        lowest, highest = int(batches.min()), int(batches.max())
        if lowest < 0 or highest >= self.counts.size:
            offender = lowest if lowest < 0 else highest
            raise ValueError(f"row id batch {offender} is outside the manifest's {self.counts.size} batches")
        for start in range(0, flat.size, _CHUNK_ROWS):
            chunk = batches[start : start + _CHUNK_ROWS]
            residues = (flat[start : start + _CHUNK_ROWS] % self.p).astype(np.float64)
            self.counts += np.bincount(chunk, minlength=self.counts.size)
            self.sum_mod += np.bincount(chunk, weights=residues, minlength=self.counts.size).astype(np.int64)
            # Reduce each chunk to prevent int64 overflow across large runs.
            self.sum_mod %= self.p
        while self._prefix + 1 < self.counts.size and self.covered(self._prefix + 1):
            self._prefix += 1

    def covered(self, batch: int) -> bool:
        """Return whether the observed count is at least the expected count."""
        return bool(self.counts[batch] >= self.expected_rows[batch])

    def complete(self, batch: int) -> bool:
        """Return whether the row count and modular checksum match the manifest."""
        return bool(
            self.counts[batch] == self.expected_rows[batch] and self.sum_mod[batch] == self.expected_checksum[batch]
        )

    def _mismatched(self) -> np.ndarray:
        mismatched: np.ndarray = (self.counts != self.expected_rows) | (self.sum_mod != self.expected_checksum)
        return mismatched

    def prefix(self) -> int:
        """Return the largest fully covered prefix ``0..k``, or ``-1``.

        Coverage is monotonic. Late duplicates affect exactness without moving the
        arrival watermark backward.
        """
        return self._prefix

    def committed_rows(self) -> int:
        return int(self.counts.sum())

    def violations(self, *, include_missing: bool = False) -> list[dict[str, object]]:
        """Report batches whose counts or checksums disagree with the manifest.

        Exclude wholly absent batches unless ``include_missing`` is set, since they
        may still be in flight. Partially received batches report their current loss.
        """
        found: list[dict[str, object]] = []
        for index in np.flatnonzero(self._mismatched()):
            batch = int(index)
            rows = int(self.counts[batch])
            expected = int(self.expected_rows[batch])
            if rows == 0 and not include_missing:
                continue
            if rows < expected:
                kind = LOSS
            elif rows > expected:
                kind = DUPLICATION
            else:
                kind = CORRUPTION
            found.append(
                {
                    "batch": batch,
                    "expected_rows": expected,
                    "rows": rows,
                    "expected_checksum": int(self.expected_checksum[batch]),
                    "checksum": int(self.sum_mod[batch]),
                    "kind": kind,
                }
            )
        return found


def _pyarrow_filesystem(location: str) -> tuple[pa_fs.FileSystem, str]:
    """Return a filesystem and path for reading ID columns.

    Use fsspec for object-store endpoint and credential consistency, and native
    PyArrow IO for local files to avoid bridge overhead.
    """
    fs, path = uri.filesystem_for(location)
    if uri.is_remote(location):
        return pa_fs.PyFileSystem(pa_fs.FSSpecHandler(fs)), path
    return pa_fs.LocalFileSystem(), path


def read_id_column(path: str, file_format: str) -> np.ndarray:
    """Read only the scoring ID column and reject null IDs.

    Projection avoids loading the corpus payload; null IDs cannot be attributed
    to a batch.
    """
    filesystem, inner = _pyarrow_filesystem(path)
    if file_format == PARQUET:
        table = pa_parquet.read_table(inner, columns=[ID_COLUMN], filesystem=filesystem)
    elif file_format == ORC:
        with filesystem.open_input_file(inner) as handle:
            table = pa_orc.ORCFile(handle).read(columns=[ID_COLUMN])
    else:
        raise ValueError(f"cannot read row ids out of a {file_format!r} data file at {path}")
    column = table.column(ID_COLUMN)
    if column.null_count:
        raise ValueError(f"{path} holds {column.null_count} null row ids, which belong to no batch")
    return np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.int64)
