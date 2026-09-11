# SPDX-License-Identifier: Apache-2.0
"""Checked fields shared by result validation and table rendering.

Additional artifact fields are preserved. Nullable measurements represent partial
runs; publication requirements are checked separately by ``validate``.
"""

import math
from types import UnionType
from typing import NotRequired, TypedDict, cast, get_args, get_origin, get_type_hints, is_typeddict

SCHEMA_VERSION = 2


class ProducerSpec(TypedDict):
    seconds: NotRequired[float | None]


class ResultSpec(TypedDict):
    corpus: str
    producer: NotRequired[ProducerSpec]


class FleetRole(TypedDict):
    role: str
    count: int
    machine_type: str
    vcpu: float
    gib: float


class Pricing(TypedDict):
    vcpu_hour_usd: float
    gib_hour_usd: float


class Run(TypedDict):
    engine: str
    variant: str
    spec: ResultSpec
    fleet: list[FleetRole]
    site_pricing: Pricing
    corpus_hash: str | None
    table: NotRequired[str | None]
    topic: NotRequired[str | None]


class Summary(TypedDict):
    run_valid: bool
    state: str


class Data(TypedDict):
    summary: Summary | None


class Quantiles(TypedDict):
    p50_s: float | None
    p95_s: float | None
    max_s: float | None


class Freshness(TypedDict):
    window: Quantiles


class Exactness(TypedDict):
    exact: bool
    loss_rows: int
    duplicate_rows: int


class Keepup(TypedDict):
    absorbed_at_offer_end: float | None
    drain_s: float | None


class Producer(TypedDict):
    producer_bound: bool | None


class Cost(TypedDict):
    usd_per_hour: float | None


class Derived(TypedDict):
    freshness: Freshness | None
    exactness: Exactness | None
    keepup: Keepup | None
    producer: Producer
    cost: Cost


class SizeQuantiles(TypedDict):
    p50: float | None


class LiveFiles(TypedDict):
    size_quantiles: SizeQuantiles


class FinalGeometry(TypedDict):
    live: LiveFiles


class Geometry(TypedDict):
    final: FinalGeometry | None


class ResultDocument(TypedDict):
    schema_version: int
    collected_at: str
    harness_version: str
    run: Run
    data: Data
    derived: Derived
    geometry: Geometry | None


def _check(value: object, expected: object, field: str) -> None:
    """Check the limited JSON types used above, retaining unknown fields."""
    origin = get_origin(expected)
    if origin is NotRequired:
        _check(value, get_args(expected)[0], field)
    elif origin is UnionType:
        options = get_args(expected)
        if value is None and type(None) in options:
            return
        _check(value, next(option for option in options if option is not type(None)), field)
    elif is_typeddict(expected):
        if not isinstance(value, dict):
            raise ValueError(f"{field}: must be an object")
        for name, member_type in get_type_hints(expected, include_extras=True).items():
            if name not in value:
                if get_origin(member_type) is NotRequired:
                    continue
                raise ValueError(f"{field}.{name}: is missing")
            _check(value[name], member_type, f"{field}.{name}")
    elif origin is list:
        if not isinstance(value, list):
            raise ValueError(f"{field}: must be an array")
        for index, item in enumerate(value):
            _check(item, get_args(expected)[0], f"{field}[{index}]")
    elif expected is float:
        if type(value) not in (int, float) or (isinstance(value, float) and not math.isfinite(value)):
            raise ValueError(f"{field}: must be a finite number")
    elif type(value) is not expected:
        raise ValueError(f"{field}: must be {getattr(expected, '__name__', expected)}")


def parse_result(value: object) -> ResultDocument:
    """Validate consumer fields and return the original document, including extras."""
    if not isinstance(value, dict):
        raise ValueError("document: must be an object")
    version = value.get("schema_version")
    if type(version) is not int or version != SCHEMA_VERSION:
        raise ValueError(f"schema_version: must be {SCHEMA_VERSION}, got {version!r}")
    _check(value, ResultDocument, "document")
    return cast(ResultDocument, value)
