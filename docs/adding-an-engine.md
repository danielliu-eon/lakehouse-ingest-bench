# Adding an engine

The harness can score any engine that consumes Kafka and appends to Iceberg.
Choose one of two integration modes.

**Tier 1, external.** The harness stages the run, prints connection facts and
waits for you to start the engine. Use this mode first: it works with your
existing cluster, service or local process.

**Tier 2, managed.** An integration under `engines/<name>/` lets the harness
size and start the engine from a run spec. Use this when repeated runs and
capacity sweeps justify the additional integration code.

## The tier 1 contract

Comparable runs must satisfy these six rules.

1. Consume the topic from the earliest offset. Values are Avro binary
   single-record encoding per `schema.avsc`, or that same encoding behind the
   Confluent header when the run asks for it (see "Confluent values" below);
   keys are UTF-8 strings or null.
2. Append every row to the table, mapping columns one to one by name, and
   declare every corpus column `NOT NULL`. Extra columns are allowed and may be
   nullable; a corpus column that is dropped, renamed, retyped or optional voids
   the run. `corpus.json` publishes the Iceberg type of every column, and the
   scorer checks the table's columns, their types and their required-ness
   against it when first loading the table. Convert the event-time column from
   Avro `timestamp-millis` to Iceberg `timestamp` (microsecond precision, without
   a time zone) without rounding. Nullability affects encoding and file geometry,
   so it must match even when the engine creates the table.
3. Append-only: no delete files, no upserts, no overwrites.
4. Data files in Parquet or ORC.
5. Commit through the catalog the harness names.
6. No snapshot expiry during the run. Compaction and other rewrites only when
   declared as a named variant in the result.

Start at the earliest offset even if the engine starts before the producer.
Partition assignment happens after consumer startup; `latest` can skip records
published before assignment or during a restart without committed offsets.

Snapshot expiry can remove commit history before the scorer reads it.
Compaction is allowed because the tally counts only `append` commits, but it
changes file geometry. Declare it as a named variant and compare it with
equivalent runs.

## What the harness hands you

Staging writes the engine connection contract to `runs/<run_id>/facts.json`:

| Key | What to do with it |
|---|---|
| `run_id` | the run's name; also the topic's, and a good consumer-group id |
| `bootstrap` | Kafka bootstrap servers |
| `topic` | the topic to consume, from its earliest offset |
| `corpus_uri` | the corpus directory. `<corpus_uri>/corpus.json` is where the column types and roles are published |
| `schema_avsc_uri` | the Avro schema the values are encoded with |
| `value_encoding` | `avro` for raw Avro binary, `confluent` for the Confluent wire format |
| `schema_registry_url` | the registry the schema was registered with, or `null` |
| `schema_subject` | the subject it was registered under, or `null` |
| `schema_id` | the id every record's header names, or `null` |
| `compression` | the codec every batch is compressed with on the wire: `zstd`, `lz4`, `snappy`, `gzip` or `none` |
| `catalog_props` | Iceberg catalog properties, credentials redacted |
| `table` | the `namespace.table` to append to |
| `partition` | how the table is partitioned |
| `ddl` | the exact statement your table must match, or `null` when the harness already created it. Ask for the statement with `table.managed_by: engine` in the spec |
| `key_column` | the corpus column sent as the message key, or `null` for unkeyed records |
| `epoch` | `null` until the run is launched; the time origin is chosen when the producer starts |

Check that the consumer supports `compression`; an unsupported codec can
prevent it from reading any records. Supply catalog credentials separately;
`facts.json` redacts them.

### Confluent values

`value_encoding` defaults to `avro`: each value is one Avro binary record
encoded against `schema.avsc`, without a header. The three schema-registry fields
are then `null`.

With `confluent`, a five-byte header precedes that record: one zero byte, then
`schema_id` as a four-byte big-endian integer. Staging registers the schema under
`schema_subject` at `schema_registry_url`. The schema and id stay fixed for the
run. Consumers can either resolve the schema through the Confluent API or strip
the header and use `schema.avsc`. Supply any registry credentials separately.

Nothing else about the run changes: the rows, the keys, the table and the
scoring are what a raw-Avro run's are. `runs/smoke-external-confluent.yaml` and
its `aws-` sibling are the shipped external specs, and both managed engines read
the framing too. The walk-through below is a raw-Avro run.

## Walk-through: an engine the harness does not manage

This example stages a local external run and starts Flink manually. The files
in `docs/examples/external-flink/` define a Kafka source, catalog and `INSERT`,
with `@PLACEHOLDER@` tokens for run-specific names. For another engine, create
equivalent configuration using the same facts and corpus schema.

In the first shell, stage the run and wait for a file to appear:

```bash
scripts/smoke.sh --engine external --set duration_s=30 \
  --external-ready-file /tmp/engine-ready
```

It prints `facts.json` and then waits. In a second shell, fill the run's names
into the example config and start the engine:

```bash
cd <this checkout>
COMPOSE="docker compose -f deploy/compose/local/docker-compose.yml"
RUN_ID=<the run_id the first shell printed>
export RUN_DIR="$PWD/runs/$RUN_ID"

TABLE=$(jq -r .table "$RUN_DIR/facts.json")
sed -e "s|@RUN_ID@|$RUN_ID|g" \
    -e "s|@TOPIC@|$(jq -r .topic "$RUN_DIR/facts.json")|g" \
    -e "s|@NAMESPACE@|${TABLE%%.*}|g" \
    -e "s|@TABLE@|${TABLE#*.}|g" \
    docs/examples/external-flink/job.sql > "$RUN_DIR/job.sql"
sed -e "s|@RUN_ID@|$RUN_ID|g" \
    docs/examples/external-flink/flink-conf.yaml > "$RUN_DIR/flink-conf.yaml"

set -a; source docs/examples/external-flink/flink.env; set +a
$COMPOSE --profile flink build flink-jobmanager
$COMPOSE --profile flink up -d --scale "flink-taskmanager=$TASKMANAGERS"
$COMPOSE --profile flink-job run --rm flink-job
touch /tmp/engine-ready
```

The first shell then offers the corpus, scores what lands in the table, and
prints the verdict. Drop `--external-ready-file` and it waits on a newline from
stdin instead — `scripts/run.sh <spec> --external-ready-file <path>` is the same
contract for a run on a cluster. `runs/<run_id>/scores/summary.json` is the whole
answer, and [`methodology.md`](methodology.md) §The verdict says what each field
means and when a result may be published from it.

## On a cluster

The same contract, driven by `scripts/` rather than the local stack.
`runs/aws-smoke-external.yaml` and its `-confluent` sibling are the specs to
copy; the `fleet` rows in them are placeholders, and a fleet is what the result
is costed on. The confluent one needs `site.kafka.schema_registry`, and staging
refuses it before it touches the cluster otherwise.

```bash
RUN_ID=$(scripts/stage.sh runs/aws-smoke-external-confluent.yaml | awk -F': ' '/^run_id: /{print $2}')
jq . "runs/$RUN_ID/facts.json"    # start your engine against these
scripts/launch.sh "$RUN_ID"       # once it is consuming: the scorer, then the offer
```

`scripts/run.sh <spec> --external-ready-file <path>` is the same sequence
unattended: it waits for `<path>` to appear rather than for you, and
[`running.md`](running.md) has every driver. Either way the ordering is the
invariant — staging creates the topic and the table, so an engine that resolves
either at startup cannot start until `stage.sh` has returned.

## Tier 2: a managed engine

`engines/<name>/` owns each engine's integration. The shipped integrations provide
these files and generated artifacts:

| File | Purpose |
|---|---|
| `README.md` | what the engine runs, knobs it honours, known traps |
| `Dockerfile` | stock upstream image plus connector jars, pinned; built locally, and pushed to the operator's own registry by `scripts/push-images.sh` |
| `compose.yaml`, `compose.sh` | the engine's services for the local stack, and the four `engine_compose_*` hooks `smoke.sh` starts, readies and reads them through |
| job source | the job the engine runs (SQL or Python) |
| `knobs.py` | run-spec keys the engine accepts, validation, rendering into the template and the DDL |
| `verify.py` | reads effective state from the running engine and fails staging on drift from the spec |
| `fleet.py` | requested vCPU and GiB per role from the spec, for `run.json` |
| *the cluster documents* | not a file here: the custom resource a run is, and the ConfigMap its pods mount, are `knobs.py`'s render output into the run directory |

Keep `knobs.py` independent of cluster access so operators can inspect and
compare rendered configuration before starting resources. Reject unknown keys
so misspelled settings cannot silently change a measured run.

Register it by adding its knobs module to `MANAGED` in
`ingest_bench/specs/engines.py`; `engine: <name>` plus a `<name>:` block of
knobs is then a usable spec. The harness imports three modules by name and rejects the engine if any is
unavailable:

- `knobs.py` provides `validate(block, spec, meta)`, which refuses a block that
  cannot describe a runnable engine, and
  `render(spec, site, derived, meta, *, image_tag)`, which returns the files to
  write into the run directory keyed by filename. `image_tag` is keyword-only,
  names the images a cluster run starts, and is `None` for a site with no
  cluster.
- `fleet.py` provides `fleet(spec)`: the compute the run asked for, in the vCPU and
  GiB a published result is costed in.
- `verify.py` provides `verify(spec, run_id, fetch, ...)`: one line per setting the
  running engine does not honour, empty for a run it does.

For cluster runs, `knobs.py` also provides `KUBERNETES`, an `EngineKubernetes`
descriptor. It defines the resource kind, status and error paths, lifecycle and
running states, API Service, pod labels and maximum object-name length. Drivers
read it through `engine-k8s`, so adding an engine requires no engine-specific
branches in `scripts/`.
