# SPDX-License-Identifier: Apache-2.0
"""Resolving a rendered document's ``${env:NAME}`` references, inside the image.

The renderer runs in the harness and the job runs inside the Spark image,
minutes apart and with nothing else in common, so the one thing they must
agree on lives here alone. This module imports nothing but the standard
library: it is copied into the image beside the job, where neither the harness
nor its dependencies exist.

`engines/flink/script.py` carries the same pattern for the same reason, and
`ingest_bench/specs/env.py` is the harness's own copy. A test holds the three
together.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

_ENV_PLACEHOLDER = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}")


def substitute_env(text: str) -> str:
    """``text`` with every ``${env:NAME}`` replaced by that variable.

    An unset variable is refused rather than substituted empty: an empty
    password reaches a broker as an authentication failure raised from inside a
    client library, which names neither the property that was empty nor the
    file that asked for it.
    """

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise ValueError(f"${{env:{name}}} is not set in the environment")
        return os.environ[name]

    return _ENV_PLACEHOLDER.sub(replace, text)


def substitute_env_values(values: Mapping[str, str]) -> dict[str, str]:
    """``values`` with every reference in them resolved, and nothing written back."""
    return {key: substitute_env(value) for key, value in values.items()}
