# SPDX-License-Identifier: Apache-2.0
"""Register a corpus's Avro schema, and the five bytes a Confluent record carries.

A run's records are raw Avro single-record binary by default: the corpus
publishes one schema, every value is encoded against it, and nothing on the
wire says so. An engine that only reads Confluent Avro cannot join such a run
at all — its deserializer takes the schema id off the front of every value and
fetches the writer schema by it — so `kafka.value_encoding: confluent` puts the
header there and registers the schema the header points at.

The registry is the operator's: any Confluent-API endpoint named by
`site.kafka.schema_registry`. Registration is one HTTP call, so it is made with
`urllib` rather than a client library — the whole of the protocol used here is
the POST below, and the header layout is five bytes.

Nothing here is on a hot path: the schema is registered once per run at
staging, and the header is built once per run and prepended per record.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from typing import cast
from urllib.parse import quote

# The Confluent wire format: a zero byte, then the schema's registry id as a
# four-byte big-endian integer, then the Avro binary of the value.
CONFLUENT_MAGIC = b"\x00"
_SCHEMA_ID_BYTES = 4

# The media type the Confluent schema-registry API declares. Registries that
# serve the API accept `application/json` too, but the versioned type is what
# the specification names, and sending it is what makes a 415 mean the endpoint
# is not a schema registry at all.
_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"

# Long enough for a registry that has to start a database connection, short
# enough that an unreachable one fails staging in under a minute rather than
# hanging before the topic is created.
_TIMEOUT_S = 30.0


def subject_for(topic: str) -> str:
    """The subject a topic's values are registered under.

    TopicNameStrategy, which is every Confluent client's default: a consumer
    that was told only the topic can still find the schema.
    """
    return f"{topic}-value"


def confluent_header(schema_id: int) -> bytes:
    """The five bytes that precede an Avro value in the Confluent wire format."""
    if schema_id < 0:
        raise ValueError(f"a schema id is a non-negative integer, got {schema_id}")
    return CONFLUENT_MAGIC + schema_id.to_bytes(_SCHEMA_ID_BYTES, "big")


def register_schema(url: str, basic_auth_user_info: str | None, subject: str, schema_text: str) -> int:
    """Register ``schema_text`` under ``subject`` and return the id it was given.

    Registering the same schema under the same subject twice returns the same
    id, so re-staging a run under its original identifier is not a second
    version of anything.

    ``basic_auth_user_info`` is the ``user:password`` a hosted registry
    authenticates with, already resolved from whatever the site referenced.
    """
    endpoint = f"{url.rstrip('/')}/subjects/{quote(subject, safe='')}/versions"
    headers = {"Content-Type": _CONTENT_TYPE}
    if basic_auth_user_info is not None:
        headers["Authorization"] = f"Basic {base64.b64encode(basic_auth_user_info.encode()).decode()}"
    request = urllib.request.Request(
        endpoint,
        data=json.dumps({"schema": schema_text, "schemaType": "AVRO"}).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        # The body carries the registry's own reason — an incompatible schema,
        # a subject in a read-only mode — and it is the only thing that says
        # which. Quoted rather than summarised for that reason.
        detail = error.read().decode("utf-8", errors="replace")
        raise ValueError(f"POST {endpoint} was refused with HTTP {error.code}: {detail}") from error
    except urllib.error.URLError as error:
        raise ValueError(f"POST {endpoint} could not be reached: {error.reason}") from error
    payload = cast(dict[str, object], json.loads(body))
    if "id" not in payload:
        raise ValueError(f"POST {endpoint} answered without a schema id: {body}")
    schema_id = payload["id"]
    if isinstance(schema_id, bool) or not isinstance(schema_id, int):
        raise ValueError(f"POST {endpoint} answered with a schema id that is not an integer: {body}")
    return schema_id
