# SPDX-License-Identifier: Apache-2.0
"""Per-batch completeness, accumulated from the row ids a table holds.

Freshness and exactness are both answers to one question asked of every batch
the corpus published: are all of its rows in the table, and only its rows? The
tally answers it incrementally, so a whole run is scored by walking the
snapshots once and adding each commit's ids as they are read, rather than by
re-scanning the table at every point of interest.

A batch is judged on two figures the corpus froze: its row count, and the sum
of its row ids modulo a prime. The count alone cannot tell a lost row from a
row that arrived twice, and the two together cannot be satisfied by any wrong
set of ids that a real engine fault would produce.

Two predicates come out of those figures and they answer different questions.
Coverage asks whether a batch's rows have arrived, which is what freshness
times. Completeness asks whether exactly its rows arrived, which is what
exactness judges. Keeping them apart is what lets a late duplicate be a fault
without also rewriting when the rows before it became visible.
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

# `np.bincount` accumulates weights in float64, which represents integers
# exactly only below 2**53. A residue is under P, near 1e9, so a single call
# stays exact for roughly 9e6 rows; chunking well under that is what makes the
# checksum comparison an equality test rather than an approximation.
_CHUNK_ROWS = 4_000_000


class BatchTally:
    """How much of each batch the table holds, and where it disagrees.

    The tally is indexed by batch number, so it holds one manifest's batches
    from 0 upward and nothing else. Ids carry their batch in their high bits,
    which is what lets a commit's ids be attributed without any join.
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
        """Count one commit's row ids into the batches they came from."""
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
            # Reduced every chunk, not once at the end: a run large enough to
            # need many chunks would otherwise accumulate past int64.
            self.sum_mod %= self.p
        while self._prefix + 1 < self.counts.size and self.covered(self._prefix + 1):
            self._prefix += 1

    def covered(self, batch: int) -> bool:
        """Whether the batch's rows have arrived, whatever else arrived with them."""
        return bool(self.counts[batch] >= self.expected_rows[batch])

    def complete(self, batch: int) -> bool:
        """Whether exactly the batch's rows arrived, and nothing else."""
        return bool(
            self.counts[batch] == self.expected_rows[batch] and self.sum_mod[batch] == self.expected_checksum[batch]
        )

    def _mismatched(self) -> np.ndarray:
        mismatched: np.ndarray = (self.counts != self.expected_rows) | (self.sum_mod != self.expected_checksum)
        return mismatched

    def prefix(self) -> int:
        """The largest ``k`` for which every batch ``0..k`` is covered, or ``-1``.

        This is the watermark freshness is read off, so it is coverage and not
        completeness: it marks when a batch's rows became visible to a reader,
        and a duplicate landing later cannot unmake that instant. Counts only
        grow, so the watermark only advances, which is what lets it be carried
        forward from where the last commit left it rather than rescanned. A run
        whose loss is masked by a duplicate is already failing exactness, and
        its freshness figure is moot.
        """
        return self._prefix

    def committed_rows(self) -> int:
        return int(self.counts.sum())

    def violations(self, *, include_missing: bool = False) -> list[dict[str, object]]:
        """Every batch the table disagrees with the manifest about.

        A batch nothing has arrived for is withheld unless ``include_missing``:
        mid-run it is a batch still in flight, and only the judgement taken
        after the drain window can call it lost. A batch that has partly
        arrived is not withheld — mid-run it reports as ``loss``, which is what
        it is at that instant and what it stays if nothing more arrives.
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
    """The pyarrow filesystem serving ``location``, beside the path to hand it.

    An object store is reached through the same fsspec filesystem the rest of
    these tools use, so one endpoint and one set of credentials serve every
    reader here. A local path takes pyarrow's own filesystem instead: the
    fsspec bridge costs a Python call per read, and this reads whole columns.
    """
    fs, path = uri.filesystem_for(location)
    if uri.is_remote(location):
        return pa_fs.PyFileSystem(pa_fs.FSSpecHandler(fs)), path
    return pa_fs.LocalFileSystem(), path


def read_id_column(path: str, file_format: str) -> np.ndarray:
    """The row ids one data file holds.

    Only the id column is read. It is everything exactness needs, and a corpus
    row is mostly payload, so projecting it is the difference between scoring a
    run and reading back every byte the engine wrote.

    A null id is refused rather than counted: the row cannot be attributed to a
    batch, and the table's schema declares the column required, so a null there
    is a fault in the writer and not a row the corpus can be scored against.
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
