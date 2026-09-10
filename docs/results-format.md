# The result document (`run.json`, `schema_version: 2`)

`collect` reads a finished run directory and writes one JSON document: every
figure the run is compared by, in one place, with the operator's site taken out
of it. It is what `results/` holds and what `results-table` renders.

```
collect --run-dir runs/<run_id> --site site.yaml            # → runs/<run_id>/run.json
collect --run-dir runs/<run_id> --site site.yaml \
        --out results/flink --variant hash                   # → results/flink/<name>.json
```

`--out` at a directory is filled with
`<collected date>-<engine>-<corpus>-<variant>.json`; at a file it is taken as
given; left out it writes `run.json` beside the run.

## Redaction

Three rules, applied to the whole document on the way out — so an artifact that
grows a new field carrying a path is covered without an edit here.

1. **Site roots are substituted** by the root's own name in angle brackets.
   Five roots: `site.corpus_root` → `<corpus_root>`, `site.runs_root` →
   `<runs_root>`, `site.warehouse` → `<warehouse>`,
   `site.kubernetes.registry` → `<registry>`, and the catalog's own
   `warehouse` property → `<catalog_warehouse>` where it differs from the
   three before it. The last two are roots because neither is a path under the
   object-store three: an image reference starts with the registry host, and a
   Glue REST catalog's warehouse is a bare account id. The longest matching
   root wins, and a root matches only at a path boundary, so a sibling prefix
   (`corpus-archive` beside `corpus`) is left alone. Paths under nobody's root
   are published as they stand.
2. **Credential-shaped properties are replaced** by `"<redacted>"`: any
   catalog property whose key contains `token`, `credential`, `secret` or
   `password`. A value written as `${env:NAME}` survives — it names a variable
   rather than holding a secret, and a reader needs to know which one to set.
3. **`site.kafka.security` is never read**, so neither its keys nor its values
   reach the document under any name.

Left in by design: the catalog `uri`, which says which catalog implementation a
result was measured against, and the prices.

## Top level

| Field | What it holds |
|---|---|
| `schema_version` | `2` |
| `collected_at` | when `collect` ran, `YYYY-MM-DDTHH:MM:SSZ` |
| `harness_version` | the installed `lakehouse-ingest-bench` version |
| `run` | what the run was |
| `artifacts` | where each input came from, and the two series embedded whole |
| `data` | the scorer's documents verbatim |
| `derived` | the figures a result is read by |
| `geometry` | `geometry.json` as `file-sizes` wrote it, or `null` |
| `missing` | every optional input `collect` could not read |

## `run`

| Field | Source |
|---|---|
| `run_id`, `table`, `topic` | `facts.json` |
| `variant` | `--variant` (default `hash`) — the tuning this result stands for |
| `spec` | `spec.yaml` copied verbatim, so defaults are not printed as choices |
| `engine` | `spec.engine` |
| `engine_versions` | `{image, digest}` from `engine-image.json` for a managed engine; the spec's `external` block otherwise; `null` when neither was recorded |
| `fleet` | `[{role, count, vcpu, gib, machine_type}]` — the engine's own `fleet(spec)` for a managed run, `spec.fleet` for an external one. What the run asked for; see [`methodology.md`](methodology.md) §Cost. A managed run whose knobs named no machine type reports `unspecified`, which is not publishable |
| `site_pricing` | `{vcpu_hour_usd, gib_hour_usd}` from the site |
| `catalog_props` | `facts.catalog_props`, redacted |
| `epoch_ms` | the run's time origin; `null` for a run that was staged but never launched |
| `corpus_hash` | from `summary.json` |
| `compression` | `spec.producer.compression` — the codec the offer crossed the link with, resolved rather than left to the copied spec's defaults |

## `artifacts`

Relative paths for the inputs `collect` found: `spec`, `facts`, `timeline`,
`engine_image`, `summary`, `freshness`, `exactness`, `geometry`,
`keepup_samples`. A path is absent exactly when the file is named in `missing`.

`snapshots` is embedded whole rather than pointed at — the parsed
`snapshots.jsonl`, one record per commit, a few hundred for an hour run. Every
freshness and geometry figure was drawn from it, and a result nobody can
re-derive is not evidence.

`publish_logs` is **summarized per shard**, because the offer's history is one
record per batch and runs to thousands. One entry per
`producer/publish_log-<i>.jsonl`:

| Field | What it holds |
|---|---|
| `shard` | the index in the file's name |
| `batches` | how many records the log holds |
| `first_scheduled_ms` | when the shard's earliest batch was due |
| `first_ack_ms`, `last_ack_ms` | the interval the shard was acknowledging over |
| `bytes`, `rows` | what the shard published in total |
| `behind_ms_max` | the worst gap between a batch being due and first acked |
| `errors` | delivery errors across the shard |
| `done` | whether the log carries its trailer. A shard that stopped early never writes one, so a result whose shards are not all `done` describes a partial offer whatever its other figures say |

The full per-batch logs stay in the object store under
`<runs_root>/<run_id>/producer/`; that is where to go to re-derive the offer
itself rather than the totals.

## `data`

`summary`, `freshness` and `exactness` as the scorer wrote them, redacted but
otherwise untouched. `freshness` carries the full `lag_series` on both clocks,
which is what lets another bound be evaluated offline from a published result.
`exactness` carries the (capped) violation list.

## `derived`

| Field | What it holds |
|---|---|
| `freshness` | `window` and `full` lag quantiles side by side, plus `clock_skew_suspected` and `min_lag_s` |
| `exactness` | the tally's figures — `expected_rows`, `rows`, `scored_batches`, `loss_rows`, `duplicate_rows`, `duplicate_ppm`, `corrupt_batches`, `exact` — without the violation list, which stays in `data` |
| `keepup` | `absorbed_at_offer_end`, `drain_s`, `backlog_rows_max`, `backlog_rows_p50`, as the scorer computed them |
| `producer` | `behind_ms_max`, `errors`, `effective_offered_rate_bytes_per_s`, `producer_bound` |
| `cost` | `usd_per_hour`, `run_hours`, `usd` |

`collect` measures nothing itself: every figure above is read from an artifact,
so a published result and the run directory behind it cannot disagree.

**`producer`.** The rate is the bytes the publish logs acknowledged over the
interval they were acknowledged in, so a producer that started late is not
credited with a higher rate for having had less time. Without the logs the
scorer's own reading stands in for `behind_ms_max` and `errors`, and the rate is
`null`. `producer_bound` is always the scorer's — it is an input to `run_valid`,
and a second implementation here could disagree with the verdict.

**`cost`.** `usd_per_hour` over the fleet, `run_hours` from the epoch to the
run's real end, and their product — `null` where the run has no epoch or no end.
[`methodology.md`](methodology.md) §Cost is how each is defined.

## `missing`

Names, as relative paths, every optional input that was absent —
`engine-image.json`, `scores/geometry.json`,
`producer/publish_log-*.jsonl`, and so on. `spec.yaml` and `facts.json` are the
two `collect` refuses without: they are what names the run, and staging writes
them together.

An absence is not a failure. `teardown.sh` collects a run before its geometry
has been measured, and publish logs are often left in the object store, so both
are routinely missing from the first document and present in the second. Which
input is absent is what decides whether the document is publishable: a result
with no geometry is still a result, one with no scores is not. `run_valid` in
`data.summary` is the field that decides publication — see
[`methodology.md`](methodology.md) §The verdict, and the rules in
[`../results/README.md`](../results/README.md).
