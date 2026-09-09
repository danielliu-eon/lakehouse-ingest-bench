"""Reading what a running engine reports about itself, and refusing to guess.

Every managed engine is checked against its spec the same way: a document is
fetched, a field is read out of it, and a value is compared with the one the
knobs asked for. This module is that grammar — the readers, the shape of a
drift line, and the two exit statuses a driver branches on — so two engines'
checks are the same thing to whoever reads their output and to the driver that
runs them.

A document that does not hold what it is read for is unreadable and says so.
The alternative is an empty drift list, which reads as a verified run — the one
answer nobody may be given without having looked.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable
from typing import cast

# Long enough for a driver under a cold job graph, short enough that an
# endpoint nothing is listening on is answered in one breath rather than held
# until a driver's own timeout.
REST_TIMEOUT_S = 5.0

# A verdict and a refusal to give one are different answers: staging retries
# the second and fails on the first, so they cannot share an exit status.
DRIFT_EXIT = 3
UNVERIFIED_EXIT = 2
# A fleet the scheduler has not finished placing is a third answer: not drift,
# because nothing was dropped, and not an unreadable endpoint, because the
# driver answered. Staging waits this one out against the engine's own running
# wait rather than the handful of tries an endpoint gets.
PENDING_EXIT = 4

# What a field the engine did not report reads as. A name rather than an empty
# string, because the line is read by a person deciding whether to restage or
# to look at the engine's own log.
NOT_REPORTED = "not reported"


def document(value: object, where: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} answered {type(value).__name__} rather than a JSON object")
    return {str(key): entry for key, entry in cast(dict[object, object], value).items()}


def documents(value: object, where: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ValueError(f"{where} answered {type(value).__name__} rather than a JSON array")
    return [document(entry, f"{where}[{index}]") for index, entry in enumerate(cast(list[object], value))]


def field(holder: dict[str, object], key: str, where: str) -> object:
    if key not in holder:
        raise ValueError(f"{where} answered no {key!r}; it holds {sorted(holder)}")
    return holder[key]


def int_field(holder: dict[str, object], key: str, where: str) -> int:
    value = field(holder, key, where)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} answered {key} {value!r}, which is not a whole number")
    return value


def str_field(holder: dict[str, object], key: str, where: str) -> str:
    value = field(holder, key, where)
    if not isinstance(value, str):
        raise ValueError(f"{where} answered {key} {value!r}, which is not a string")
    return value


def optional_str_field(holder: dict[str, object], key: str, where: str) -> str:
    """``key`` as a string, or empty where the engine reported no such field.

    For the fields a live object gains rather than declares: a pod carries no
    QoS class until the API server admits it. Absence is a reading about the
    run and is compared like any other value, so it is not an error here — but
    a field that is present and of the wrong type still is.
    """
    if key not in holder:
        return ""
    return str_field(holder, key, where)


def line(what: str, spec: object, engine: object) -> str:
    """One drift, in the shape every line of every engine's report takes."""
    return f"{what}: spec {spec}, engine {engine}"


def fetch_json(base_url: str) -> Callable[[str], object]:
    """A reader of an engine's own HTTP documents by path.

    Every failure — a refused connection, a timeout, an answer that is not
    JSON — is one message naming the URL. The endpoint is reached through a
    tunnel on a cluster and through a service name on the local stack, so
    which address did not answer is the whole of what a caller needs.
    """
    root = base_url.rstrip("/")

    def fetch(path: str) -> object:
        url = f"{root}{path}"
        try:
            with urllib.request.urlopen(url, timeout=REST_TIMEOUT_S) as answer:
                return cast(object, json.loads(answer.read()))
        except (OSError, ValueError) as error:
            raise ValueError(f"could not read {url}: {error}") from error

    return fetch
