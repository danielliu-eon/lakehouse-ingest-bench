# SPDX-License-Identifier: Apache-2.0
"""Remove credentials and configured site roots from published results.

Replace roots with named placeholders to preserve path relationships. Apply
root redaction recursively to strings and mapping keys so newly added fields
receive the same treatment.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from ingest_bench.specs.env import has_placeholder, names_a_secret
from ingest_bench.specs.model import SiteConfig

REDACTED = "<redacted>"


def _roots(site: SiteConfig) -> list[tuple[str, str]]:
    """Pair site roots with placeholders, longest first to handle nested roots."""
    named = [("corpus_root", site.corpus_root), ("runs_root", site.runs_root), ("warehouse", site.warehouse)]
    # Image registries and Glue account IDs need explicit roots; neither is
    # necessarily under a configured storage path.
    if site.kubernetes is not None:
        named.append(("registry", site.kubernetes.registry))
    catalog_warehouse = site.catalog_props.get("warehouse")
    if catalog_warehouse is not None and catalog_warehouse.rstrip("/") not in {root.rstrip("/") for _, root in named}:
        named.append(("catalog_warehouse", catalog_warehouse))
    return sorted(
        # An empty root would match every string.
        ((root.rstrip("/"), f"<{name}>") for name, root in named if root.strip("/")),
        key=lambda pair: len(pair[0]),
        reverse=True,
    )


def redact_uri(uri: str, site: SiteConfig) -> str:
    """Replace a matching site root with its placeholder.

    Match at path boundaries so ``corpus`` does not match ``corpus-archive``.
    Leave strings outside the configured roots unchanged.
    """
    for root, placeholder in _roots(site):
        if uri == root:
            return placeholder
        if uri.startswith(f"{root}/"):
            return placeholder + uri[len(root) :]
    return uri


# Share credential-name rules with configuration validation.
def redact_props(props: Mapping[str, str], site: SiteConfig | None) -> dict[str, str]:
    """Redact literal credentials and, when ``site`` is supplied, site roots.

    Preserve environment references so users can identify required variables.
    Pass ``site=None`` for engine configuration that still needs actual URIs.
    """
    redacted: dict[str, str] = {}
    for key, value in props.items():
        if names_a_secret(key) and not has_placeholder(value):
            redacted[key] = REDACTED
        else:
            redacted[key] = value if site is None else redact_uri(value, site)
    return redacted


def redact_document(value: object, site: SiteConfig) -> object:
    """Recursively redact site roots in string values and mapping keys."""
    if isinstance(value, str):
        return redact_uri(value, site)
    if isinstance(value, dict):
        return {
            redact_uri(str(key), site): redact_document(entry, site)
            for key, entry in cast(dict[object, object], value).items()
        }
    if isinstance(value, list):
        return [redact_document(entry, site) for entry in cast(list[object], value)]
    return value
