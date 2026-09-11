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
   scorer checks column names, types and nullability against this metadata
   when first loading the table. Convert the event-time column from
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

Confluent framing leaves the rows, keys, table and scoring unchanged. Use
`runs/smoke-external-confluent.yaml` locally or
`runs/aws-smoke-external-confluent.yaml` on a cluster. Both managed engines also
support this framing. The walk-through below uses raw Avro.

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

The first shell then replays the corpus, scores the table and prints the
verdict. Without `--external-ready-file`, it waits for a newline on stdin.
For cluster runs, `scripts/run.sh <spec> --external-ready-file <path>` provides
the same readiness check.

The scorer writes `runs/<run_id>/scores/summary.json`. See
[the verdict](methodology.md#the-verdict) for field definitions and
[publication rules](../results/README.md) before publishing a result.

## On a cluster

Cluster runs use the same engine contract through the shell drivers. Copy
`runs/aws-smoke-external.yaml` or `runs/aws-smoke-external-confluent.yaml` and
replace the placeholder `fleet` entries with the resources your engine requests;
they determine the reported cost. The Confluent spec also requires
`site.kafka.schema_registry`. Staging rejects a missing registry before changing
cluster resources.

```bash
RUN_ID=$(scripts/stage.sh runs/aws-smoke-external-confluent.yaml | awk -F': ' '/^run_id: /{print $2}')
jq . "runs/$RUN_ID/facts.json"    # start your engine against these
scripts/launch.sh "$RUN_ID"       # once it is consuming: the scorer, then the offer
```

To automate this sequence, use
`scripts/run.sh <spec> --external-ready-file <path>` and create `<path>` when the
engine is ready. See [running a benchmark](running.md) for the other drivers.
Always wait for `stage.sh` to return before starting an engine that resolves the
topic or table at startup: staging creates those resources.

## Tier 2: a managed engine

`engines/<name>/` owns each engine's integration. The shipped integrations provide
these files and generated artifacts:

| File | Purpose |
|---|---|
| `README.md` | runtime, supported settings and known limitations |
| `Dockerfile` | stock upstream image plus connector jars, pinned; built locally, and pushed to the operator's own registry by `scripts/push-images.sh` |
| `compose.yaml`, `compose.sh` | local services and the four `engine_compose_*` hooks used by `smoke.sh` |
| job source | the job the engine runs (SQL or Python) |
| `knobs.py` | run-spec keys the engine accepts, validation, rendering into the template and the DDL |
| `verify.py` | reads effective state from the running engine and fails staging on drift from the spec |
| `fleet.py` | declared role counts, machine types and sizing estimates; measured cluster resources come from pods |
| cluster manifests | custom resource and ConfigMap generated by `knobs.py` in the run directory |

Keep `knobs.py` independent of cluster access so operators can inspect and
compare rendered configuration before starting resources. Reject unknown keys
so misspelled settings cannot silently change a measured run.

Register the engine's knobs module in `MANAGED` in
`ingest_bench/specs/engines.py`. Run specs can then select it with
`engine: <name>` and a `<name>:` block of settings. The harness requires three
modules:

- `knobs.py` provides `validate(block, spec, meta)` to reject invalid settings
  and `render(spec, site, derived, meta, *, image_tag)` to return generated files
  keyed by filename. The keyword-only `image_tag` identifies the images for
  cluster runs and is `None` for a site with no cluster.
- `fleet.py` provides `fleet(spec)` to declare role counts, machine types and
  sizing estimates. Set `KUBERNETES.pod_role_label` and `KUBERNETES.fleet_selector`
  beside the engine renderer to capture actual cluster requests for cost.
- `verify.py` provides `verify(spec, run_id, fetch, ...)` to report one line per
  mismatch between the spec and the running engine, or an empty result when
  they agree.

For cluster runs, `knobs.py` also provides `KUBERNETES`, an `EngineKubernetes`
descriptor. It defines the resource kind, status and error paths, lifecycle and
running states, API Service, pod labels and maximum object-name length. Drivers
read it through `engine-k8s`, so adding an engine requires no engine-specific
branches in `scripts/`.
