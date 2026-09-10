# The corpus

A corpus is the workload, frozen. It is generated once, read by every run scored
against it, and it carries its own ground truth: the row count and the row-id
checksum of every batch, plus the partition and column statistics the generator
verified before publishing. Terms are defined in
[`methodology.md`](methodology.md).

A corpus shape is two files: a **schema** under `workloads/schemas/` naming the
columns, and a **preset** under `workloads/presets/` naming the stream. The
preset is hashed in full and the hash names the corpus directory, so two shapes
can never share one — and a change to either produces a new corpus rather than
silently rescoring an old one.

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

Every key is required — there are no defaults, so a preset states its whole
shape and a reader of one does not have to know the loader.

| Key | What it sets |
|---|---|
| `schema` | the schema file under `workloads/schemas/`, by name |
| `offered_bytes_per_s` | the offered rate, as a byte quantity (`5MB`, `100MB`, `1GiB`) |
| `duration_s` | how long the stream covers. Must be a whole number of batches |
| `partition_count` | the key space: how many distinct `partition_key` values exist |
| `alpha` | the Zipf exponent over those keys. `0.0` is uniform; higher is more skewed |
| `target_row_bytes` | the encoded row size the payload column is calibrated to |
| `batch_interval_ms` | one batch's width, and so the commit cadence a reader sees |
| `corpus_epoch` | the instant the first batch's event times start from. Needs an explicit UTC offset, since the offset is not part of the hash |
| `kafka_key_columns` | which columns get a key sidecar. Must be string columns; `partition_key` is added if left out, because the Iceberg partition column is written from its sidecar |
| `column_overrides` | per-column changes to the schema's declarations, keyed by column name |

`--set KEY=VALUE` overrides one key on the command line, repeatable, and
`--set column_overrides.<column>.<attribute>=VALUE` reaches one column. An
override changes the hash, so it names a different corpus — which is why a run
against an overridden preset is a probe and not a publishable result.

## Shape axes

The four axes a campaign varies, and what each one is for:

- **Rate** (`offered_bytes_per_s`) — the load. It sets the batch size, so it
  also sets the generator's memory needs.
- **Skew** (`alpha` over `partition_count`) — how unevenly rows land on keys. A
  skewed corpus is what separates an engine that fans out per key from one that
  does not.
- **Row size** (`target_row_bytes`) — rows per second at a fixed rate, and so
  how much per-row work an engine does per byte.
- **Batch interval** (`batch_interval_ms`) — the granularity freshness is
  measured at. A batch is the unit that arrives or does not.

Beyond those, a column's own `cardinality` and `alpha` shape what compresses:
`cardinality: 0` means a fresh value per row, which denies Parquet a dictionary
and makes that column's bytes survive compression.

## Schemas

The two reserved columns lead every schema and are implicit: `id` (long) carries
row identity, and `partition_key` (string) carries partition truth. Exactness is
scored off the first and the table is partitioned on the second, so a schema may
neither rename nor retype them.

Everything else is declared. A column declares a **kind**, which fixes its Avro
type, and optionally a **role**, which is what the benchmark asks of it
irrespective of its name.

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

### Declaring one

A declaration is `{"name": ..., "kind": ...}` plus any distribution key —
`role`, `cardinality`, `alpha`, `vocabulary`, `minimum`, `maximum`, `width` —
and everything unstated falls back to the default, so a schema says only what it
means to bend. `workloads/schemas/events.json` is the worked example.

The loader refuses a declaration whose value space cannot realize what it
declares, because `corpus.json` would otherwise publish an axis the corpus then
flattened:

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

Generation then gates the corpus it produced, and refuses to publish one that
missed its own declaration: the mean encoded row size within 2% of
`target_row_bytes`, each checked column's realized cardinality within 10%, and
an unbounded blob column's entropy at or above 7.5 bits per byte — a payload a
codec could fold away is not the incompressibility axis it claims to be.

The key space is gated too, differently under skew and without it. A skewed
corpus (`alpha` above zero) is held to its Zipf weights: the **worst** key's
realized byte share may not deviate from the weight it was asked for by more
than 5%. Bytes rather than rows, because what a skewed key costs an engine is
the data it has to write for it. That comparison only means anything once there
are enough rows behind the thinnest key to out-weigh sampling noise, so it is
enforced only when the coldest key expects at least 10,000 rows. Under no skew
the shares carry no information, and what is checked instead is that every key
received some rows at all.

**Ship the schema and the preset before publishing any result from them.**
`validate-results.py` refuses a result whose `corpus_hash` does not match a
shipped preset's, so a shape that lives only on one machine cannot be published
from.

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

`corpus.json` is the only authority for what was built. Everything downstream —
the producer, the table creator, the scorer, an engine on the external tier —
takes a corpus URI and reads that document; a consumer that re-derived a figure
from the preset could disagree with the corpus it is scoring.

## Generating

```bash
gen-corpus --preset smoke --out s3://<corpus_root> --seed 1
gen-corpus --preset events-100mbs-skew --out s3://<corpus_root> --plan   # writes nothing
```

`--plan` prints the preset's hash, the batch count and size, the estimated rows,
the mean row size and the estimated encoded and stored bytes without writing a
byte. The row figures come from the same calibration the generator runs, and are
lower bounds: a batch is filled in whole row blocks and stops on the first block
that crosses its byte budget, so it overshoots slightly. Run it before committing
hours to a full-scale corpus.

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
how a large corpus is generated in parallel. Every shard still builds whole
batches, so sharding buys throughput and not headroom: either 600 MB/s preset
needs about 6 GB free per process.

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

**Measure on the machine that will offer.** One process's rate is a property of
that machine, its broker and the encoder, so no figure recorded elsewhere sizes
your offer. The shipped cluster specs state the shard count one node class's
measured rate implied for their own corpus, as the starting point a probe ladder
needs and not as a figure to reuse. Re-measure after any change to the producer
or the encoder as well.
