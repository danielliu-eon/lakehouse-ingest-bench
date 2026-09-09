"""What a published result may not carry, and the one rule that takes it out.

A run directory is full of one operator's site: the bucket the corpus was read
from, the prefix the artifacts were uploaded to, the warehouse the table was
created under, and the properties the catalog was reached with. A result is
meant to be published, so all of that has to go — and it has to go the same way
in every artifact, whatever fields that artifact happens to carry, because a
redaction that names fields is a redaction the next field added slips past.

The site's roots are substituted rather than deleted. A reader of a result can
still see that the corpus came from under one root and the table was written
under another, which is what keeps two runs' paths comparable, and cannot tell
which bucket either of them was.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from ingest_bench.specs.env import has_placeholder
from ingest_bench.specs.model import SiteConfig

REDACTED = "<redacted>"

# A property whose name contains one of these carries a credential. Matching on
# the name rather than the value is what keeps a run directory publishable:
# `facts.json` is meant to be pasted into an issue or an engine's config, and a
# catalog token is the one thing in it that must not travel.
_SECRET_HINTS = ("token", "credential", "secret", "password")


def _names_a_secret(key: str) -> bool:
    return any(hint in key.lower() for hint in _SECRET_HINTS)


def _roots(site: SiteConfig) -> list[tuple[str, str]]:
    """The site's roots paired with the literal each becomes, longest first.

    Longest first because roots nest: a site whose runs prefix sits under its
    corpus root would otherwise have every run path attributed to the corpus,
    and the two are not the same place.
    """
    named = (("corpus_root", site.corpus_root), ("runs_root", site.runs_root), ("warehouse", site.warehouse))
    return sorted(
        # An empty root is dropped rather than matched: it would sit at the
        # start of every string and rewrite paths that are under no root at all.
        ((root.rstrip("/"), f"<{name}>") for name, root in named if root.strip("/")),
        key=lambda pair: len(pair[0]),
        reverse=True,
    )


def redact_uri(uri: str, site: SiteConfig) -> str:
    """``uri`` with the site root it sits under replaced by that root's name.

    A root matches at a path boundary rather than as a text prefix: a sibling
    prefix that merely starts with the root — `corpus-archive` beside `corpus` —
    is a different location, and rewriting it would publish a path that never
    existed. A string that is under no root is returned as it stands, so this
    is safe to apply to every string in a document.
    """
    for root, placeholder in _roots(site):
        if uri == root:
            return placeholder
        if uri.startswith(f"{root}/"):
            return placeholder + uri[len(root) :]
    return uri


def redact_props(props: Mapping[str, str], site: SiteConfig | None) -> dict[str, str]:
    """``props`` with credentials replaced, and site roots replaced where asked.

    A value naming an environment variable is published as it stands. It is a
    reference and not a secret, and it is the one thing a reader of the
    properties needs in order to supply the credential from their own copy of
    them — redacting it would hide which variable to set.

    ``site`` is ``None`` where the properties stay on the operator's machine.
    `facts.json` is what an engine is configured from, so the warehouse URI in
    it has to survive as the bucket it names; only the copy that goes into a
    published result loses it.
    """
    redacted: dict[str, str] = {}
    for key, value in props.items():
        if _names_a_secret(key) and not has_placeholder(value):
            redacted[key] = REDACTED
        else:
            redacted[key] = value if site is None else redact_uri(value, site)
    return redacted


def redact_document(value: object, site: SiteConfig) -> object:
    """``value`` with every string in it, however deeply nested, root-redacted.

    Recursive and field-blind on purpose: the artifacts a result embeds are
    written by the scorer and the producer, and a rule naming the fields that
    carry paths today would let the next one through. Mapping keys are covered
    too, since a document keyed by path is a document that names a bucket.
    """
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
