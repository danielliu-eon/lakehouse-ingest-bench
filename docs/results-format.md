# The result document (`run.json`, `schema_version: 2`)

`collect` combines a run directory's measurements and configuration into one
redacted JSON document. These documents are stored in `results/` and rendered
by `results-table`.

```
collect --run-dir runs/<run_id> --site site.yaml            # → runs/<run_id>/run.json
collect --run-dir runs/<run_id> --site site.yaml \
        --out results/flink --variant hash                   # → results/flink/<name>.json
```

If `--out` names a directory, the filename is
`<collected date>-<engine>-<corpus>-<variant>.json`. A file path is used as given.
The default is `run.json` in the run directory.

## Redaction

Site roots are replaced throughout the document, including mapping keys:

| Site value | Replacement |
|---|---|
| `corpus_root` | `<corpus_root>` |
| `runs_root` | `<runs_root>` |
| `warehouse` | `<warehouse>` |
| `kubernetes.registry` | `<registry>` |
| `catalog.props.warehouse`, when distinct from those roots | `<catalog_warehouse>` |

The longest match wins. Matches require a path boundary, so `corpus-archive`
is not treated as a child of `corpus`. Unmatched strings remain unchanged.

Catalog properties with `token`, `credential`, `secret`, `password` or
`auth_user_info` in their names become `"<redacted>"`. Environment references
such as `${env:NAME}` remain intact so readers know which variables to supply.
`site.kafka.security` is omitted entirely. The catalog `uri` and prices remain
available for comparison.

## Top level

| Field | Description |
|---|---|
| `schema_version` | `2`. Additive `run` fields do not change this version. A missing field in an older document means it was not yet recorded, which differs from an explicit `null` |
| `collected_at` | when `collect` ran, `YYYY-MM-DDTHH:MM:SSZ` |
| `harness_version` | the installed `lakehouse-ingest-bench` version |
| `run` | run identity and configuration |
| `artifacts` | input paths, snapshot history and publish-log summaries |
| `data` | the scorer's documents, with redaction |
| `derived` | comparison metrics derived from recorded artifacts |
| `geometry` | `geometry.json` as `file-sizes` wrote it, or `null` |
| `missing` | absent optional inputs |

## `run`

| Field | Source |
|---|---|
| `run_id`, `table`, `topic` | `facts.json` |
| `variant` | `--variant` (default `hash`) — the named tuning configuration |
| `spec` | unchanged `spec.yaml`; omitted defaults remain omitted |
| `engine` | `spec.engine` |
| `engine_versions` | `{image, digest}` from `engine-image.json` for a managed engine; the spec's `external` block otherwise; `null` when neither was recorded |
| `fleet` | requested resources as `[{role, count, vcpu, gib, machine_type}]`, from managed `fleet(spec)` or external `spec.fleet`. A missing managed machine type becomes `unspecified` and prevents publication; see [Cost](methodology.md#cost) |
| `site_pricing` | `{vcpu_hour_usd, gib_hour_usd}` from the site |
| `catalog_props` | `facts.catalog_props`, redacted |
| `epoch_ms` | the run's time origin; `null` for a run that was staged but never launched |
| `corpus_hash` | from `summary.json` |
| `value_encoding` | `facts.value_encoding`: `avro` or `confluent`, as supplied to consumers |
| `compression` | resolved `spec.producer.compression`, including its default |

## `artifacts`

Relative paths for the inputs `collect` found: `spec`, `facts`, `timeline`,
`engine_image`, `summary`, `freshness`, `exactness`, `geometry`,
`keepup_samples`. A path is absent exactly when the file is named in `missing`.

`snapshots` embeds the parsed `snapshots.jsonl`, with one record per commit.
This history lets readers inspect the commits behind the measurements.

`publish_logs` contains a **summary per shard** to avoid embedding thousands of
per-batch records. There is one entry per
`producer/publish_log-<i>.jsonl`:

| Field | Description |
|---|---|
| `shard` | the index in the file's name |
| `batches` | how many records the log holds |
| `first_scheduled_ms` | when the shard's earliest batch was due |
| `first_ack_ms`, `last_ack_ms` | first and last acknowledgement times |
| `bytes`, `rows` | total published bytes and rows |
| `behind_ms_max` | maximum delay from a batch's due time to its first acknowledgement |
| `errors` | delivery errors across the shard |
| `done` | whether the log has a completion trailer. Every shard must be done to establish a complete offer |

Full per-batch logs remain under `<runs_root>/<run_id>/producer/` in object
storage for detailed inspection.

## `data`

The original `summary`, `freshness` and `exactness` documents, with redaction.
`freshness` includes `lag_series` on both clocks so readers can evaluate other
bounds offline. `exactness` includes the capped violation list.

## `derived`

| Field | Description |
|---|---|
| `freshness` | `window` and `full` lag quantiles side by side, plus `clock_skew_suspected` and `min_lag_s` |
| `exactness` | the tally's figures — `expected_rows`, `rows`, `scored_batches`, `loss_rows`, `duplicate_rows`, `duplicate_ppm`, `corrupt_batches`, `exact` — without the violation list, which stays in `data` |
| `keepup` | `absorbed_at_offer_end`, `drain_s`, `backlog_rows_max`, `backlog_rows_p50`, as the scorer computed them |
| `producer` | `behind_ms_max`, `errors`, `effective_offered_rate_bytes_per_s`, `producer_bound` |
| `cost` | `usd_per_hour`, `run_hours`, `usd` |

`collect` derives these fields from recorded artifacts; it does not run new
measurements.

**`producer`.** Effective rate is acknowledged bytes divided by the recorded
acknowledgement interval. Without publish logs, `behind_ms_max` and `errors`
come from the scorer and rate is `null`. `producer_bound` always comes from the
scorer to remain consistent with `run_valid`.

**`cost`.** Records the fleet's hourly cost, run duration and total cost. Duration
and total cost are `null` without both an epoch and an end time. See
[Cost](methodology.md#cost) for the formula.

## `missing`

This list names absent optional inputs by relative path, such as
`engine-image.json`, `scores/geometry.json` or `producer/publish_log-*.jsonl`.
`spec.yaml` and `facts.json` are required; collection fails if either is missing.

Some inputs may arrive later: `teardown.sh` collects before geometry is
measured, and publish logs may not yet have been fetched from object storage.
A later collection can include them. Publication requires valid scores but
permits missing geometry. See [the verdict](methodology.md#the-verdict) for
`data.summary.run_valid` and the [publication rules](../results/README.md).
