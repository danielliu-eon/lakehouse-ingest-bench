"""The one catalog client and the one property loader every tool here shares.

A run addresses its table through whatever catalog the site runs, and the
properties that reach it decide which one that is. Building the client in a
single place is what keeps a property file authoritative: a tool that
constructed a REST client directly would honour the file's credentials while
silently ignoring the catalog implementation it named.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog


def _parse_key_value(raw: str, source: str) -> tuple[str, str]:
    """One ``KEY=VALUE`` setting, or a refusal naming where it came from.

    Strict about the separator and the key: a malformed line in a credential
    file otherwise reaches the catalog as a property nobody wrote, and the
    failure then surfaces as an authentication error against the wrong claim.
    """
    if "=" not in raw:
        raise ValueError(f"{source} must contain KEY=VALUE, got {raw!r}")
    key, value = raw.split("=", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"{source} has an empty key")
    return key, value.strip()


def parse_key_values(pairs: Sequence[str], source: str) -> dict[str, str]:
    return dict(_parse_key_value(pair, source) for pair in pairs)


def load_catalog_props(values: Sequence[str], files: Sequence[str] = ()) -> dict[str, str]:
    """Catalog properties from files and command line, files first.

    Command line last so an operator can override one property of a checked-in
    file without editing it. Comment and blank lines are skipped so a property
    file can carry its own provenance. A file is preferred over a flag for a
    credential: on argv a token lands in every process listing.
    """
    props: dict[str, str] = {}
    for filename in files:
        path = Path(filename)
        try:
            lines = path.read_text().splitlines()
        except OSError as error:
            raise ValueError(f"could not read catalog property file {path}: {error}") from error
        for line_number, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            key, value = _parse_key_value(stripped, f"{path}:{line_number}")
            props[key] = value
    for raw in values:
        key, value = _parse_key_value(raw, "--catalog-prop")
        props[key] = value
    return props


def open_catalog(props: dict[str, str]) -> Catalog:
    """The catalog the properties name.

    ``load_catalog`` rather than a concrete class: it resolves the
    implementation from the properties, so a property file naming a non-REST
    catalog is honoured instead of being read as REST while every other
    property is applied.
    """
    return load_catalog("bench", **props)


def table_identifier(name: str) -> tuple[str, str]:
    """``(namespace, table)`` for a catalog, from a two- or three-part name.

    A query engine addresses a table as ``catalog.schema.table`` while a
    catalog client is already scoped to one catalog, so a leading catalog
    segment is dropped and the namespace is always the single segment before
    the table name. Splitting off only the last segment instead would yield
    the namespace ``catalog.schema``, which resolves to a different table than
    the same ``--table`` string gives a tool that dropped it.
    """
    parts = name.split(".")
    if len(parts) not in (2, 3) or not all(parts):
        raise ValueError(f"table must be namespace.name or catalog.namespace.name, got {name!r}")
    return parts[-2], parts[-1]
