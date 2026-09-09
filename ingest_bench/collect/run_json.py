# SPDX-License-Identifier: Apache-2.0
"""One finished run as the document a result is published as.

A run directory is the raw record: the spec that was asked for, the facts an
engine was pointed at, the scorer's artifacts, the producer's logs. This turns
that into one document — every figure a reader compares runs by, in one place,
with every trace of the operator's site removed.

Two things it deliberately does not do. It measures nothing of its own: every
figure here is read from an artifact the scorer or the producer wrote, so a
published result and the run directory behind it cannot disagree. And it
refuses nothing that is merely absent: a run torn down before its geometry was
measured, or whose publish logs were never fetched, is collected with those
inputs named in `missing`. The alternative is a driver that cannot publish a
run the fleet has already been paid for.
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

# The publish logs are one file per producer shard, so they are named by pattern
# rather than by name — and the pattern is what `missing` reports, since a run
# whose logs were never fetched has no file name to name.
PUBLISH_LOG_GLOB = f"{publish_log.LOG_NAME_PREFIX}*{publish_log.LOG_NAME_SUFFIX}"

# Seconds resolution and an explicit Z, the same stamp the run's timeline uses,
# so the two sort against each other.
_COLLECTED_AT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_MS_PER_HOUR = 3_600_000

# The keep-up scalars a result is read by, out of the summary's keepup block.
# Named rather than copied whole so a scalar the scorer adds is a deliberate
# addition here too.
_KEEPUP_FIGURES = ("absorbed_at_offer_end", "drain_s", "backlog_rows_max", "backlog_rows_p50")

# The one part of the tally that is evidence rather than a figure. It stays in
# `data`, where the artifact is published whole.
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
    """The run directory being read, and the optional files it did not hold.

    The absences accumulate rather than raise, so one pass over the directory
    produces both the document and the list of what could not go into it.
    """

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
    """The compute the run was given.

    An external engine reports its own, because the harness never saw it. A
    managed one is sized by its knobs, so the roles are derived from the same
    block the engine was started with and cannot describe a different fleet.
    """
    if spec.is_external():
        return list(spec.fleet)
    return cast(list[FleetRole], fleet_for(spec.engine).fleet(spec))


def engine_versions(spec: RunSpec, image: dict[str, object] | None) -> dict[str, object] | None:
    """What was running: the image the harness started, or the engine declared.

    A managed run is identified by the image reference and its digest, which is
    the only statement about the engine's version that cannot have drifted from
    what actually ran. An external run is identified by what its operator said,
    since nothing else about it is observable from here.
    """
    if spec.external is not None:
        return {"name": spec.external.name, "version": spec.external.version, "notes": spec.external.notes}
    if image is None:
        return None
    return {"image": image["image"], "digest": image["digest"]}


# ---------------------------------------------------------------------------
# The derived figures
# ---------------------------------------------------------------------------


def freshness_figures(freshness: dict[str, object]) -> dict[str, object]:
    """The lag quantiles both ways round, and the clock the run was judged on.

    Window and full run side by side because they answer different questions: a
    window that passes only because the warmup swallowed a ten-minute cold
    start is a different result from a run that was fresh throughout.
    """
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
    """The keep-up scalars, as the scorer computed them.

    Not recomputed from the sample series: `absorbed_at_offer_end` is read at
    the instant the offer stopped and `drain_s` from the instant the prefix
    reached the last batch, and neither instant is recoverable from the samples
    alone.
    """
    keepup = cast(dict[str, object], summary["keepup"])
    return {name: keepup[name] for name in _KEEPUP_FIGURES}


def publish_log_summary(path: Path, records: list[PublishRecord]) -> dict[str, object]:
    """One producer shard's log reduced to the totals a result needs of it.

    The per-batch records are the offer's raw history, one entry per batch and
    so thousands of them for an hour run. What a reader compares runs by is the
    interval a shard was acking over and how much it got through, so that is
    what is published; the logs themselves stay under the runs prefix for
    anyone re-deriving the rest.

    ``done`` is the shard's trailer: a shard that stopped early never writes
    one, so a result whose shards are not all done describes a partial offer
    whatever its other figures say.
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
    """What the offer did, from the publish logs where they were fetched.

    The rate is the bytes the logs acknowledged over the interval they were
    acknowledged in, so it needs no epoch and describes the offer rather than
    the run: a producer that started late is not credited with a higher rate
    for having had less time.

    Where the logs are absent, the scorer's own reading of them stands in for
    the two scalars it published, and the rate is left unmeasured — the bytes it
    divides are in the logs alone. ``producer_bound`` is always the scorer's: it
    is an input to `run_valid`, and a second implementation of it here could
    disagree with the verdict it was drawn from.
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
    """When the run last did anything, which is what the fleet was paid until.

    The later of the producer's last acknowledgement and the table's last
    commit, because a fleet is not released when the offer stops: it is still
    draining, and the drain is on the bill. Either side alone answers where the
    other was not fetched.
    """
    candidates = [record.last_ack_ms for record in records]
    candidates.extend(int(cast(int, row["timestamp_ms"])) for row in snapshots)
    return max(candidates) if candidates else None


def cost_figures(
    fleet: Sequence[FleetRole], site: SiteConfig, *, epoch_ms: int | None, end_ms: int | None
) -> dict[str, object]:
    """What the fleet cost, from the site's own prices.

    The hourly figure stands even for a run that never started, since it is a
    property of the fleet the spec asked for; the total needs the run to have
    both a beginning and an end, and is null where either is unknown.
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
    """The publishable document for the run in ``run_dir``.

    Everything is read from the run directory except the prices and the roots,
    which come from the site — and the site is also what the whole document is
    redacted against on the way out, so a path any input carried is a path with
    the operator's bucket taken out of it.
    """
    inputs = _Inputs(run_dir)
    spec_path = inputs.required(SPEC_FILE)
    spec = load_run_spec(spec_path)
    # The spec is embedded as the YAML that was staged rather than as the loaded
    # dataclass: it is the published record of what was asked for, and a round
    # trip through the loader would print every default as if it had been chosen.
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
    # The epoch is the run's own record of its time origin, and it is null until
    # the run is launched — a staged run that was never started is collectable
    # and says so by having no epoch rather than by being refused.
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
    # The commit series is embedded rather than pointed at: it is a few hundred
    # records for an hour run, every freshness and geometry figure was drawn
    # from it, and a result that only names it is a result nobody can re-derive.
    artifacts["snapshots"] = snapshots
    # The offer's history is not, because it is one record per batch. Each
    # shard's totals stand in, and the logs stay under the runs prefix.
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
