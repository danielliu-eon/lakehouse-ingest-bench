"""Environment indirection, so no file a run writes has to hold a secret.

A site config, a rendered engine script and a run's published facts are all
meant to be read: pasted into an issue, diffed against another run's, checked
into a repository. A credential in any of them is a credential that travels, so
anything secret is written as ``${env:NAME}`` and replaced inside the process
that uses it, at the moment it uses it. Nothing resolved here is written back.

This is the whole of the harness's involvement in authentication. It implements
none: the resolved properties go to a Kafka client or a catalog verbatim, which
is what lets a cluster this repository has never heard of be reached by
configuration alone.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

# `engines/flink/script.py` carries the same pattern. The copy is deliberate:
# that module is copied into the engine's image, where this package does not
# exist, so it may import nothing but the standard library.
PLACEHOLDER_RE = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}")


def has_placeholder(value: str) -> bool:
    return PLACEHOLDER_RE.search(value) is not None


def _substituted(key: str, value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise ValueError(f"${{env:{name}}} is not set in the environment, and {key!r} references it")
        return os.environ[name]

    return PLACEHOLDER_RE.sub(replace, value)


def resolve_env_placeholders(values: Mapping[str, str]) -> dict[str, str]:
    """``values`` with every ``${env:NAME}`` replaced by that variable.

    An unset variable is refused rather than substituted empty. An empty
    password reaches a broker as an authentication failure raised from inside a
    client library, which names neither the property that was empty nor the
    file that asked for it.
    """
    return {key: _substituted(key, value) for key, value in values.items()}
