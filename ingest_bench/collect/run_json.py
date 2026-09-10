# SPDX-License-Identifier: Apache-2.0
"""Assemble a publishable run document from staged facts and recorded artifacts.

Use scorer and producer measurements, redact configured site details, and
list unavailable optional artifacts in ``missing``. Partial runs remain
collectable even when geometry or publish logs are absent.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import yaml

from ingest_bench.collect.redact import redact_document, redact_props
from ingest_bench.producer import publish_log
from ingest_bench.producer.publish_log import PublishRecord
from ingest_bench.scorer import score
from ingest_bench.scorer.geometry import GEOMETRY_FILE
from ingest_bench.specs.engines import fleet_for
from ingest_bench.specs.model import FleetRole, RunSpec, SiteConfig, load_run_spec

SCHEMA_VERSION = 2

RUN_JSON_FILE = "run.json"

SPEC_FILE = "spec.yaml"
FACTS_FILE = "facts.json"
TIMELINE_FILE = "timeline.log"
ENGINE_IMAGE_FILE = "engine-image.json"

SCORES_DIR = "scores"
PRODUCER_DIR = "producer"

# Use a pattern because shard counts vary; also report it for missing logs.
PUBLISH_LOG_GLOB = f"{publish_log.LOG_NAME_PREFIX}*{publish_log.LOG_NAME_SUFFIX}"

# Match the run timeline format.
_COLLECTED_AT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_MS_PER_HOUR = 3_600_000

# Select published metrics explicitly so additions require a schema decision.
_KEEPUP_FIGURES = ("absorbed_at_offer_end", "drain_s", "backlog_rows_max", "backlog_rows_p50")

# Keep detailed violations in the full artifact under `data`.
_VIOLATIONS = "violations"


def _score_path(name: str) -> str:
    return f"{SCORES_DIR}/{name}"


def _read_json(path: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [
        cast(dict[str, object], json.loads(line))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@dataclass
class _Inputs:
    """Run directory and accumulated missing optional inputs."""

    run_dir: Path
    missing: list[str] = field(default_factory=list)

    def required(self, relative: str) -> Path:
        path = self.run_dir / relative
        if not path.exists():
            raise ValueError(
                f"{path} does not exist; a run is collected from its {SPEC_FILE} and its {FACTS_FILE}, which "
                "staging writes together"
            )
        return path

    def optional(self, relative: str) -> Path | None:
        path = self.run_dir / relative
        if path.exists():
            return path
        self.missing.append(relative)
        return None

    def publish_logs(self) -> list[Path]:
        paths = sorted((self.run_dir / PRODUCER_DIR).glob(PUBLISH_LOG_GLOB))
        if not paths:
            self.missing.append(f"{PRODUCER_DIR}/{PUBLISH_LOG_GLOB}")
        return paths


# ---------------------------------------------------------------------------
# What the run was
# ---------------------------------------------------------------------------


def run_fleet(spec: RunSpec) -> list[FleetRole]:
    """Return the declared external fleet or derive managed roles from engine knobs."""
    if spec.is_external():
        return list(spec.fleet)
    return cast(list[FleetRole], fleet_for(spec.engine).fleet(spec))


def engine_versions(spec: RunSpec, image: dict[str, object] | None) -> dict[str, object] | None:
    """Return managed image provenance or the external engine's declared version."""
    if spec.external is not None:
        return {"name": spec.external.name, "version": spec.external.version, "notes": spec.external.notes}
    if image is None:
        return None
    return {"image": image["image"], "digest": image["digest"]}


# ---------------------------------------------------------------------------
# The derived figures
# ---------------------------------------------------------------------------


def freshness_figures(freshness: dict[str, object]) -> dict[str, object]:
    """Return window and full-run lag quantiles, plus clock-skew diagnostics."""
    return {
        "window": freshness["window"],
        "full": freshness["full"],
        "clock_skew_suspected": freshness["clock_skew_suspected"],
        "min_lag_s": freshness["min_lag_s"],
    }


def exactness_figures(exactness: dict[str, object]) -> dict[str, object]:
    """The tally's figures, without the violation list."""
    return {key: value for key, value in exactness.items() if key != _VIOLATIONS}


def keepup_figures(summary: dict[str, object]) -> dict[str, object]:
    """Copy keep-up metrics from the scorer summary.

    Offer-end absorption and drain time cannot be reconstructed exactly from the
    periodic sample series.
    """
    keepup = cast(dict[str, object], summary["keepup"])
    return {name: keepup[name] for name in _KEEPUP_FIGURES}


def publish_log_summary(path: Path, records: list[PublishRecord]) -> dict[str, object]:
    """Summarize a producer shard's acknowledgements, totals, and completion status.

    The raw per-batch logs remain under the runs prefix. A missing ``done`` trailer
    means the shard did not finish its selected batches.
    """
    return {
        "shard": publish_log.shard_index(path.name),
        "batches": len(records),
        "first_scheduled_ms": min((record.scheduled_ms for record in records), default=None),
        "first_ack_ms": min((record.first_ack_ms for record in records), default=None),
        "last_ack_ms": max((record.last_ack_ms for record in records), default=None),
        "bytes": sum(record.bytes for record in records),
        "rows": sum(record.rows for record in records),
        "behind_ms_max": publish_log.behind_ms(records),
        "errors": sum(record.errors for record in records),
        "done": publish_log.shard_done(path),
    }


def producer_figures(records: list[PublishRecord], summary: dict[str, object] | None) -> dict[str, object]:
    """Summarize offered throughput and delivery failures from publish logs.

    Measure rate over the acknowledgement interval. Without logs, use the scorer's
    lag and error totals and leave rate unknown. Always retain the scorer's
    ``producer_bound`` verdict.
    """
    bound = None if summary is None else summary["producer_bound"]
    if records:
        acked_ms = max(record.last_ack_ms for record in records) - min(record.first_ack_ms for record in records)
        offered_bytes = sum(record.bytes for record in records)
        return {
            "behind_ms_max": publish_log.behind_ms(records),
            "errors": sum(record.errors for record in records),
            "effective_offered_rate_bytes_per_s": offered_bytes / (acked_ms / 1000) if acked_ms > 0 else None,
            "producer_bound": bound,
        }
    scorer = None if summary is None else cast(dict[str, object], summary["producer"])
    return {
        "behind_ms_max": None if scorer is None else scorer["behind_ms"],
        "errors": None if scorer is None else scorer["errors"],
        "effective_offered_rate_bytes_per_s": None,
        "producer_bound": bound,
    }


def usd_per_hour(fleet: Sequence[FleetRole], site: SiteConfig) -> float:
    return sum(
        role.count * (role.vcpu * site.pricing_vcpu_hour_usd + role.gib * site.pricing_gib_hour_usd) for role in fleet
    )


def run_end_ms(records: list[PublishRecord], snapshots: list[dict[str, object]]) -> int | None:
    """Return the latest acknowledgement or table commit, including drain time.

    Use whichever records are available; return ``None`` if neither is present.
    """
    candidates = [record.last_ack_ms for record in records]
    candidates.extend(int(cast(int, row["timestamp_ms"])) for row in snapshots)
    return max(candidates) if candidates else None


def cost_figures(
    fleet: Sequence[FleetRole], site: SiteConfig, *, epoch_ms: int | None, end_ms: int | None
) -> dict[str, object]:
    """Calculate fleet cost using site prices.

    Hourly cost depends only on fleet size. Total cost is unknown unless both
    start and end times are available.
    """
    per_hour = usd_per_hour(fleet, site)
    hours = None if epoch_ms is None or end_ms is None else (end_ms - epoch_ms) / _MS_PER_HOUR
    return {
        "usd_per_hour": per_hour,
        "run_hours": hours,
        "usd": None if hours is None else per_hour * hours,
    }


# ---------------------------------------------------------------------------
# The document
# ---------------------------------------------------------------------------


def build_run_json(
    run_dir: Path,
    site: SiteConfig,
    *,
    harness_version: str,
    collected_at: datetime,
    variant: str,
) -> dict[str, object]:
    """Build a publishable document from ``run_dir``.

    Read prices and redaction roots from ``site``; read run facts and measurements
    from the directory.
    """
    inputs = _Inputs(run_dir)
    spec_path = inputs.required(SPEC_FILE)
    spec = load_run_spec(spec_path)
    # Read the staged YAML directly so omitted defaults remain omitted.
    spec_document = cast(dict[str, object], yaml.safe_load(spec_path.read_text(encoding="utf-8")))
    facts = _read_json(inputs.required(FACTS_FILE))

    timeline_path = inputs.optional(TIMELINE_FILE)
    image_path = inputs.optional(ENGINE_IMAGE_FILE)
    summary_path = inputs.optional(_score_path(score.SUMMARY_FILE))
    freshness_path = inputs.optional(_score_path(score.FRESHNESS_FILE))
    exactness_path = inputs.optional(_score_path(score.EXACTNESS_FILE))
    geometry_path = inputs.optional(_score_path(GEOMETRY_FILE))
    keepup_samples_path = inputs.optional(_score_path(score.KEEPUP_SAMPLES_FILE))
    snapshots_path = inputs.optional(_score_path(score.SNAPSHOTS_FILE))
    log_paths = inputs.publish_logs()

    image = None if image_path is None else _read_json(image_path)
    summary = None if summary_path is None else _read_json(summary_path)
    freshness = None if freshness_path is None else _read_json(freshness_path)
    exactness = None if exactness_path is None else _read_json(exactness_path)
    geometry = None if geometry_path is None else _read_json(geometry_path)
    snapshots = [] if snapshots_path is None else _read_jsonl(snapshots_path)
    publish_logs: list[dict[str, object]] = []
    records: list[PublishRecord] = []
    for log_path in log_paths:
        shard_records = publish_log.read(log_path)
        publish_logs.append(publish_log_summary(log_path, shard_records))
        records.extend(shard_records)

    epoch = facts["epoch"]
    # A staged but unlaunched run has no epoch and can still be collected.
    epoch_ms = None if epoch is None else round(float(cast(float, epoch)) * 1000)
    fleet = run_fleet(spec)

    artifacts: dict[str, object] = {"spec": SPEC_FILE, "facts": FACTS_FILE}
    for name, path in (
        ("timeline", timeline_path),
        ("engine_image", image_path),
        ("summary", summary_path),
        ("freshness", freshness_path),
        ("exactness", exactness_path),
        ("geometry", geometry_path),
        ("keepup_samples", keepup_samples_path),
    ):
        if path is not None:
            artifacts[name] = str(path.relative_to(run_dir))
    # Embed the compact snapshot history so readers can re-derive measurements.
    artifacts["snapshots"] = snapshots
    # Summarize the larger per-batch logs; retain raw logs under the runs prefix.
    artifacts["publish_logs"] = publish_logs

    document: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "collected_at": collected_at.astimezone(UTC).strftime(_COLLECTED_AT_FORMAT),
        "harness_version": harness_version,
        "run": {
            "run_id": facts["run_id"],
            "variant": variant,
            "spec": spec_document,
            "engine": spec.engine,
            "engine_versions": engine_versions(spec, image),
            "fleet": [asdict(role) for role in fleet],
            "site_pricing": {
                "vcpu_hour_usd": site.pricing_vcpu_hour_usd,
                "gib_hour_usd": site.pricing_gib_hour_usd,
            },
            "catalog_props": redact_props(cast(dict[str, str], facts["catalog_props"]), site),
            "epoch_ms": epoch_ms,
            "corpus_hash": None if summary is None else summary["corpus_hash"],
            "table": facts["table"],
            "topic": facts["topic"],
            # Use the facts that configured consumers for this run.
            "value_encoding": facts["value_encoding"],
            # Include the resolved codec even when the source YAML omitted the default.
            "compression": spec.producer.compression,
        },
        "artifacts": artifacts,
        "data": {"summary": summary, "freshness": freshness, "exactness": exactness},
        "derived": {
            "freshness": None if freshness is None else freshness_figures(freshness),
            "exactness": None if exactness is None else exactness_figures(exactness),
            "keepup": None if summary is None else keepup_figures(summary),
            "producer": producer_figures(records, summary),
            "cost": cost_figures(fleet, site, epoch_ms=epoch_ms, end_ms=run_end_ms(records, snapshots)),
        },
        "geometry": geometry,
        "missing": inputs.missing,
    }
    return cast(dict[str, object], redact_document(document, site))
