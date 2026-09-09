# SPDX-License-Identifier: Apache-2.0
"""The names a run answers to, all derived from its spec and its stamp.

A run's topic, table and run directory are one identifier in different
alphabets. Deriving them in one function is what lets the producer, the engine
and the scorer be handed names by different commands, minutes apart, and still
address the same run — and what makes a leftover topic or table traceable to
the run that created it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from ingest_bench import uri
from ingest_bench.specs.model import RunSpec, SiteConfig

STAMP_FORMAT = "%Y%m%dT%H%M%SZ"

# The Iceberg namespace every run's table is created in. One namespace keeps a
# catalog listing readable and makes the tables of abandoned runs easy to find.
TABLE_NAMESPACE = "ingest_bench"


@dataclass(frozen=True)
class Derived:
    """Everything about a run that follows from its name and the moment it began."""

    run_id: str
    topic: str
    table: str
    run_root: str
    corpus_uri: str


def utc_stamp(moment: datetime | None = None) -> str:
    return (moment or datetime.now(UTC)).strftime(STAMP_FORMAT)


def derive(spec: RunSpec, site: SiteConfig, *, stamp: str | None = None, corpus_dir: str) -> Derived:
    """The run's names, for the corpus directory that was resolved for it.

    ``stamp`` is an argument rather than a reading of the clock so a caller
    can re-derive the names of a run that has already happened — the run
    directory of a finished run is addressable without a record of what its
    identifier was.
    """
    resolved = utc_stamp() if stamp is None else stamp
    run_id = f"{spec.name}-{resolved}"
    return Derived(
        run_id=run_id,
        topic=run_id,
        # A table name has no hyphens to spend: the identifier is a SQL one in
        # every engine that reads it, and quoting it everywhere is worse than
        # translating it once here.
        table=f"{TABLE_NAMESPACE}.t_{run_id.replace('-', '_')}",
        run_root=uri.join(site.runs_root, run_id),
        corpus_uri=uri.join(site.corpus_root, corpus_dir),
    )
