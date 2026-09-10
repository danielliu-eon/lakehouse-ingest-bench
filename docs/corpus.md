# The corpus

A corpus is an immutable workload reused across runs. It includes each batch's
row count and row-id checksum, plus verified partition and column statistics.
Terms are defined in
[`methodology.md`](methodology.md).

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
| `duration_s` | how long the stream covers. Must be a whole number of batches |
| `partition_count` | the key space: how many distinct `partition_key` values exist |
| `alpha` | the Zipf exponent over those keys. `0.0` is uniform; higher is more skewed |
| `target_row_bytes` | the encoded row size the payload column is calibrated to |
| `batch_interval_ms` | duration of one batch; sets the workload's time granularity |
| `corpus_epoch` | the instant the first batch's event times start from. Needs an explicit UTC offset, since the offset is not part of the hash |
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
- **Skew** (`alpha` over `partition_count`) — how unevenly rows land on keys. A
  skewed corpus is what separates an engine that fans out per key from one that
  does not.
- **Row size** (`target_row_bytes`) — rows per second at a fixed rate, and so
  how much per-row work an engine does per byte.
- **Batch interval** (`batch_interval_ms`) — the granularity freshness is
  measured at. A batch is the unit that arrives or does not.

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
| `payload` | `blob` | the row-size calibration, which spends its remaining budget here |

Any other column is `generic`.

### Declaring a column

A declaration is `{"name": ..., "kind": ...}` plus any distribution key —
`role`, `cardinality`, `alpha`, `vocabulary`, `minimum`, `maximum`, `width` —
with defaults for omitted attributes. See `workloads/schemas/events.json` for
an example.

The loader rejects declarations whose value ranges cannot support the requested
distribution:

- `alpha` above zero needs a bounded cardinality, at most 1,000,000: a skewed
  rank is drawn through a materialized CDF, and an unbounded column's values
  come from the row identity rather than from ranks.
- A **bounded** categorical needs a `vocabulary` and no `width`; an **unbounded**
  one needs `width` of at least 16 hex characters and no vocabulary, so its
  tokens stay effectively injective over the corpus.
- A numeric column needs a `[minimum, maximum]` range holding at least as many
  values as it has ranks, after rounding to a whole number or two decimals.
- A boolean's cardinality is 1 or 2. A timestamp may only be the `event_time`
  column, since arrival order belongs to the stream and not to a column.
- The `payload` column declares no `width` — its width is the calibrated
  remainder — and any other blob declares a positive one.

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
| `column_stats.json` | the sampled per-column statistics, in the form a merge re-derives from |
| `corpus.json` | what the corpus publishes about itself: the effective preset, the schema, the column roles, the Iceberg type of every column, the row count, the rates, the generator version |

`corpus.json` is authoritative. The producer, table creator, scorer and external
engines read it through the corpus URI. Use its recorded values rather than
recomputing them from a preset.

## Generating

```bash
gen-corpus --preset smoke --out s3://<corpus_root> --seed 1
gen-corpus --preset events-100mbs-skew --out s3://<corpus_root> --plan   # writes nothing
```

`--plan` prints the preset's hash, the batch count and size, the estimated rows,
the mean row size and the estimated encoded and stored bytes without writing a
byte. Row estimates use the generator's calibration and are lower bounds: batches
contain whole row blocks and may slightly exceed their byte budget. Preview a
large corpus before generating it.

### Memory

The generator holds a whole batch in memory while it encodes one, so its peak
resident memory is roughly ten times the batch's encoded bytes — a measured
factor, not a property of the code. A batch is
`offered_bytes_per_s × batch_interval_ms / 1000`:

| Preset | Batch | Peak memory |
|---|---|---|
| `smoke` | 5 MB | tens of MB |
| `events-100mbs-{uniform,skew}` | 100 MB | about 1 GB |
| `events-600mbs-{uniform,skew}` | 600 MB | about 6 GB |

`--shard-index` / `--shard-count` split the batches across processes, which is
how large corpora are generated in parallel. Each shard still builds whole
batches, so the 600 MB/s presets need about 6 GB free per process.

On a cluster the same figure is a pod's memory request, and `gen-corpus.sh` takes
it as `GEN_MEMORY` — `GEN_MEMORY=8Gi scripts/gen-corpus.sh events-600mbs-skew
--shards 8`. It defaults to `2Gi`, which fits the smoke preset. A producer shard
has the same shape, since it reads one whole batch object and decompresses it
whole, and `launch.sh` takes `PRODUCER_MEMORY` for it.

### Sharding and the merge

Each shard writes its own directory, and each publishes metadata describing its
own batches alone. `merge-corpus <shard uri>… --out <corpus_root>` combines them
into one corpus: it writes one metadata set beside the shard prefixes and leaves
every batch where its shard wrote it, so **the shard directories are part of the
corpus** and must not be cleaned up afterwards. Reading a shard as a corpus is
refused by name, because its corpus-wide figures cover a fraction of what was
sent.

On a cluster, `scripts/gen-corpus.sh <preset> --shards N` does both as Jobs —
generation on the cluster rather than a laptop, because a corpus is tens to
hundreds of gigabytes written into the same bucket the run reads it from, and the
pods already hold the identity that may write there.

## Sizing the offer

`scripts/measure-producer.sh` times one producer shard against the local broker
with an epoch an hour in the past, so every batch is already due and the wall
clock measures the producer rather than the corpus's pacing.

Size a shard count from the figure it prints:

```
shards = ceil(offered_bytes_per_s / measured_bytes_per_s * 1.5)
```

**Measure on the machine that will run the producer.** Throughput depends on
the machine, broker and encoder. Shipped shard counts are starting points for
capacity probes; measure your own setup and repeat after producer or encoder
changes.
