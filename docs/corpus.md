# The corpus

A corpus is an immutable workload reused across runs. It includes each batch's
row count and row-id checksum, plus verified partition and column statistics.
See the [methodology glossary](methodology.md#glossary) for definitions.

Two files define the workload: a **schema** under `workloads/schemas/` defines
columns, and a **preset** under `workloads/presets/` defines the stream. The
effective preset, including the schema, is hashed to name the corpus directory.
Changing either produces a new corpus.

## Shipped presets

| Preset | Rate | Duration | Keys | Skew |
|---|---|---|---|---|
| `smoke` | 5 MB/s | 300 s | 64 | `alpha: 1.0` |
| `events-100mbs-uniform` | 100 MB/s | 3600 s | 512 | `alpha: 0.0` |
| `events-100mbs-skew` | 100 MB/s | 3600 s | 512 | `alpha: 1.0` |
| `events-600mbs-uniform` | 600 MB/s | 3600 s | 512 | `alpha: 0.0` |
| `events-600mbs-skew` | 600 MB/s | 3600 s | 512 | `alpha: 1.0` |

All five use the `events` schema, 256-byte target rows and one-second batches.

## Preset keys

Every key is required; presets have no implicit defaults.

| Key | What it sets |
|---|---|
| `schema` | the schema file under `workloads/schemas/`, by name |
| `offered_bytes_per_s` | the offered rate, as a byte quantity (`5MB`, `100MB`, `1GiB`) |
| `duration_s` | stream duration; must be a whole number of batches |
| `partition_count` | the key space: how many distinct `partition_key` values exist |
| `alpha` | the Zipf exponent over those keys. `0.0` is uniform; higher is more skewed |
| `target_row_bytes` | the encoded row size the payload column is calibrated to |
| `batch_interval_ms` | duration of one batch; sets the workload's time granularity |
| `corpus_epoch` | start of the first batch's event-time range; requires an explicit UTC offset, since the offset is not part of the hash |
| `kafka_key_columns` | which columns get a key sidecar. Must be string columns; `partition_key` is added if left out, because the Iceberg partition column is written from its sidecar |
| `column_overrides` | per-column changes to the schema's declarations, keyed by column name |

Use repeatable `--set KEY=VALUE` flags to override preset keys, or
`--set column_overrides.<column>.<attribute>=VALUE` for a column attribute.
Overrides change the corpus hash. Runs using them are probes; publishable
results must match a shipped preset.

## Shape axes

A campaign can vary four workload dimensions:

- **Rate** (`offered_bytes_per_s`) — the load. It sets the batch size, so it
  also sets the generator's memory needs.
- **Skew** (`alpha` over `partition_count`) — how unevenly rows are distributed
  across keys. This reveals how an engine handles unequal load per key.
- **Row size** (`target_row_bytes`) — rows per second at a fixed byte rate.
  Smaller rows require more per-row work for the same volume of data.
- **Batch interval** (`batch_interval_ms`) — the granularity of the coverage
  check used to measure freshness.

Per-column `cardinality` and `alpha` also affect compressibility.
`cardinality: 0` generates a fresh value per row, limiting dictionary reuse.

## Schemas

Every schema starts with two implicit reserved columns: `id` (long), used for
exactness, and `partition_key` (string), used for partition statistics and table
partitioning. Schemas cannot rename or retype them.

Declare all other columns with a **kind**, which determines their type, and an
optional **role**, which identifies their purpose independently of their name.

| Kind | Avro type | Iceberg type |
|---|---|---|
| `categorical` | `string` | `string` |
| `integer` | `long` | `long` |
| `decimal` | `double` | `double` |
| `boolean` | `boolean` | `boolean` |
| `timestamp` | `long` / `timestamp-millis` | `timestamp` (microsecond, zoneless) |
| `blob` | `bytes` | `binary` |

| Role | Exactly one column, of kind | What reads it |
|---|---|---|
| `event_time` | `timestamp` | the arrival timeline |
| `entity` | `categorical` | point lookups |
| `sum_measure` | `integer` or `decimal` | aggregate queries |
| `payload` | `blob` | row-size calibration; payload fills the remaining byte budget |

Any other column is `generic`.

### Declaring a column

A declaration is `{"name": ..., "kind": ...}` with optional attributes:
`role`, `cardinality`, `alpha`, `vocabulary`, `minimum`, `maximum` and `width`.
Omitted attributes use defaults. See `workloads/schemas/events.json` for
an example.

The loader rejects declarations whose value ranges cannot support the requested
distribution:

- `alpha` above zero requires bounded cardinality of at most 1,000,000. The
  generator samples skewed ranks from a cumulative distribution stored in memory.
  Unbounded columns instead derive values from the row identity.
- A **bounded** categorical needs a `vocabulary` and no `width`; an **unbounded**
  one needs `width` of at least 16 hex characters and no vocabulary, to
  make token collisions unlikely across the corpus.
- A numeric column needs a `[minimum, maximum]` range holding at least as many
  values as it has ranks, after rounding to a whole number or two decimals.
- A boolean's cardinality is 1 or 2. A timestamp may only be the `event_time`
  column, since arrival order belongs to the stream and not to a column.
- The `payload` column omits `width` because calibration determines it.
  Every other blob column requires a positive width.

Before publication, the generator requires mean encoded row size within 2% of
`target_row_bytes`, checked column cardinalities within 10% of their targets, and
unbounded blob entropy of at least 7.5 bits per byte.

For a skewed corpus (`alpha > 0`), each key's byte share must be within 5% of
its Zipf weight. This check uses bytes to reflect write volume and applies only
when the least frequent key expects at least 10,000 rows, limiting sampling
noise. For a uniform corpus, the check requires every key to receive rows.

**Ship the schema and the preset before publishing any result from them.**
`validate-results.py` refuses a result whose `corpus_hash` does not match a
shipped preset's. Local-only workload definitions are not publishable.

## What a corpus directory holds

`<corpus_root>/<preset>-<hash>/`:

| Path | What it is |
|---|---|
| `batches/NNNNNN.bin.zst` | one batch's Avro rows, length-framed and compressed |
| `batches/NNNNNN.key.<column>.zst` | that batch's key sidecar, one per `kafka_key_columns` entry |
| `manifest.jsonl` | one record per batch: rows, the id range, the checksum, the encoded and stored bytes, a sha256 of the file. The frozen scoring input |
| `schema.avsc` | the Avro schema every value is encoded with |
| `partition_truth.json` | rows, checksum and encoded bytes per partition key |
| `column_stats.json` | sampled per-column statistics used to recompute statistics during a merge |
| `corpus.json` | corpus metadata: effective preset, schema, column roles and Iceberg types, row count, rates and generator version |

`corpus.json` is authoritative. The producer, table creator, scorer and external
engines read it through the corpus URI. Use its recorded values rather than
recomputing them from a preset.

## Generating

```bash
gen-corpus --preset smoke --out s3://<corpus_root> --seed 1
gen-corpus --preset events-100mbs-skew --out s3://<corpus_root> --plan   # writes nothing
```

`--plan` prints the preset's hash, the batch count and size, the estimated rows,
the mean row size and the estimated encoded and stored bytes. It writes no files.
Row estimates use the generator's calibration and are lower bounds: batches
contain whole row blocks and may slightly exceed their byte budget. Preview a
large corpus before generating it.

### Memory

The generator holds a whole batch in memory while encoding it. Measured peak
resident memory is roughly ten times the batch's encoded size; treat this as
an estimate. A batch is
`offered_bytes_per_s × batch_interval_ms / 1000`:

| Preset | Batch | Peak memory |
|---|---|---|
| `smoke` | 5 MB | tens of MB |
| `events-100mbs-{uniform,skew}` | 100 MB | about 1 GB |
| `events-600mbs-{uniform,skew}` | 600 MB | about 6 GB |

`--shard-index` / `--shard-count` split the batches across processes, which is
how large corpora are generated in parallel. Each shard still builds whole
batches, so the 600 MB/s presets need about 6 GB free per process.

On a cluster, set the generator pod's memory request with `GEN_MEMORY`, for
example `GEN_MEMORY=8Gi scripts/gen-corpus.sh events-600mbs-skew --shards 8`.
The default is `2Gi`, which fits the smoke preset. Producer shards also read and
decompress whole batches; set their memory request through `PRODUCER_MEMORY`
when calling `launch.sh`.

### Sharding and the merge

Each shard writes a directory containing its batches and their metadata.
`merge-corpus <shard uri>… --out <corpus_root>` combines the metadata into one
corpus but leaves the batch files in place. **The shard directories remain part
of the corpus; do not delete them after merging.** The harness rejects shard
names as corpus inputs because their metadata describes only part of the workload.

On a cluster, `scripts/gen-corpus.sh <preset> --shards N` runs generation and
merging as Jobs. This keeps the tens to hundreds of gigabytes of corpus data off
the operator's laptop and uses the pods' identity to write to the run's bucket.

## Sizing the offer

`scripts/measure-producer.sh` times one producer shard against the local broker
with an epoch an hour in the past, so every batch is already due and the wall
clock measures the producer rather than the corpus's pacing.

Choose a shard count using the measured throughput:

```
shards = ceil(offered_bytes_per_s / measured_bytes_per_s * 1.5)
```

**Measure on the machine that will run the producer.** Throughput depends on
the machine, broker and encoder. Shipped shard counts are starting points for
capacity probes; measure your own setup and repeat after producer or encoder
changes.
