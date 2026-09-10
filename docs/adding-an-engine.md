# Adding an engine

Anything that consumes a Kafka topic and appends to an Iceberg table can be
scored here. There are two ways in.

**Tier 1, external.** The harness creates the topic and the table, prints the
facts, and waits. You start your engine however you already start it — a
cluster, a hosted service, a laptop process — and the harness never touches it.
This is the primary contract and the one to reach for first.

**Tier 2, managed.** The engine lives in `engines/<name>/`, the harness sizes
and starts it from a run spec, and a sweep can vary its knobs. Worth the extra
code only when you want many runs of the same engine at different sizes.

## The tier 1 contract

Six rules. Break one and the run is not comparable to any other.

1. Consume the topic from the earliest offset. Values are Avro binary
   single-record encoding per `schema.avsc`, or that same encoding behind the
   Confluent header when the run asks for it (see "Confluent values" below);
   keys are UTF-8 strings or null.
2. Append every row to the table, mapping columns one to one by name, and
   declare every corpus column `NOT NULL`. Extra columns are allowed and may be
   nullable; a corpus column that is dropped, renamed, retyped or optional voids
   the run. `corpus.json` publishes the Iceberg type of every column, and the
   scorer checks the table's columns, their types and their required-ness
   against it the first time it loads the table. One column needs a word of its
   own: the event-time column is Avro `timestamp-millis` on the wire and Iceberg
   `timestamp` — zoneless, microsecond — in the table, so an engine widens the
   millisecond it read and never rounds it. Nullability counts because an
   optional column is encoded differently, so the file geometry two runs are
   compared on would stop being a fact about their engines. This is the rule
   that matters when the engine creates the table, since the harness then never
   sees its DDL.
3. Append-only: no delete files, no upserts, no overwrites.
4. Data files in Parquet or ORC.
5. Commit through the catalog the harness names.
6. No snapshot expiry during the run. Compaction and other rewrites only when
   declared as a named variant in the result.

Rule 1 is the one engines get wrong most often. The engine is running before
the producer publishes, but running is not assigned: a consumer gets its
partitions from a group rebalance after it starts, and one that starts at the
latest offset skips whatever landed before that assignment — and, after a
restart with no committed offsets, whatever landed while it was down. Either
way the run is scored as having lost those rows.

Rule 6 has two halves with different reasons. Snapshot expiry is off because
the scorer reads every figure from the table's snapshot log and each snapshot's
manifests; an expiry that drops a snapshot the scorer has not read yet takes
those rows' commit time with it. Compaction is legitimate and does not disturb
the tally — the scorer counts row ids from `append` commits only, so a rewrite's
new files add nothing — but it changes the file geometry the result reports, so
a run that compacts says so as a named variant and is compared with its like.

## What the harness hands you

Staging writes `runs/<run_id>/facts.json`. It is the whole interface:

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

Configure your consumer's decompression from `compression` before anything else:
a client that cannot decode the codec reads no records at all, which looks like
an engine that never started rather than a wire it cannot read. Catalog
credentials are redacted because `facts.json` is meant to be publishable; your
own come from wherever you keep them.

### Confluent values

The framing contract; each engine's README carries only its own consequence.
`value_encoding` is `avro` unless the run's spec says otherwise, and then the
four keys after it are `null`: every value is the Avro binary of one record
against `schema.avsc`, with nothing in front of it.

Where it is `confluent`, each value is that same binary behind five bytes — a
zero byte, then `schema_id` as a big-endian four-byte integer — and the schema
was registered before the first record, under `schema_subject` at
`schema_registry_url`. One schema and one id for the whole run, so a reader
that resolves the writer schema by the id in each header and a reader that
strips five bytes and uses `schema.avsc` both read every record correctly. The
registry serves the Confluent API, so any client that speaks it will do; the
credential, where the registry needs one, is the operator's and is not in
`facts.json`.

Nothing else about the run changes: the rows, the keys, the table and the
scoring are what a raw-Avro run's are. `runs/smoke-external-confluent.yaml` is
the shipped external spec, and both managed engines read the framing too. The
walk-through below is a raw-Avro run.

## Walk-through: an engine the harness does not manage

This runs the local stack, stages an external run, and starts Flink by hand as
the stand-in for "your engine". Nothing after staging knows what is consuming
the topic. The config in `docs/examples/external-flink/` is what a Flink user
would write from the facts above — a Kafka source declaring the corpus columns,
a catalog pointed at the local stack, and one `INSERT` — with the run's names
left as `@PLACEHOLDER@` tokens. If your engine is not Flink, that directory is
the shape of the thing you have to produce, not something to copy.

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
stdin instead. `runs/<run_id>/scores/summary.json` is the whole answer, and
[`methodology.md`](methodology.md) §The verdict says what each field means and
when a result may be published from it.

## Tier 2: a managed engine

`engines/<name>/` holds everything engine-specific, and the harness contains no
engine branches outside it. Both shipped engines carry all eight of its files:

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

Two rules in them are worth copying. **Render, never reach:** nothing in
`knobs.py` touches a cluster, so a run's whole configuration can be read, and
diffed against another run's, before any compute is paid for. **Refuse unknown
keys:** a misspelled knob costs one error message rather than a published result
whose tuning silently did not apply.

Register it by adding its knobs module to `MANAGED` in
`ingest_bench/specs/engines.py`; `engine: <name>` plus a `<name>:` block of
knobs is then a usable spec. Three modules are read by name, and an engine whose
module does not import is refused as unregistered rather than failing later as a
missing verdict or a missing cost:

- `knobs.py` owes `validate(block, spec, meta)`, which refuses a block that
  cannot describe a runnable engine, and
  `render(spec, site, derived, meta, *, image_tag)`, which returns the files to
  write into the run directory keyed by filename. `image_tag` is keyword-only,
  names the images a cluster run starts, and is `None` for a site with no
  cluster.
- `fleet.py` owes `fleet(spec)`: the compute the run asked for, in the vCPU and
  GiB a published result is costed in.
- `verify.py` owes `verify(spec, run_id, fetch, ...)`: one line per setting the
  running engine does not honour, empty for a run it does.

For a run on a cluster `knobs.py` owes one more thing: `KUBERNETES`, an
`EngineKubernetes` naming the kind of object a run is, where its state sits in
the status, what running is called there, which Service carries its HTTP API,
the labels its pods carry and the longest object name its operator takes. The
drivers read all but that length through `engine-k8s` and staging checks it, so
a third engine adds no line to `scripts/` bar the default `--engine` name.
