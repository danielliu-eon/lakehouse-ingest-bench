# Adding an engine

Anything that consumes a Kafka topic and appends to an Iceberg table can be
scored here. There are two ways in.

**Tier 1, external.** The harness creates the topic and the table, prints the
facts, and waits. You start your engine however you already start it — a
cluster, a hosted service, a laptop process. The harness never touches it.
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
   against it the first time it loads the table. Nullability counts because an
   optional column is encoded differently, so the file geometry two runs are
   compared on would stop being a fact about their engines. This is the rule
   that matters when the engine creates the table, since the harness then never
   sees its DDL.
3. Append-only: no delete files, no upserts, no overwrites.
4. Data files in Parquet or ORC.
5. Commit through the catalog the harness names.
6. No compaction, snapshot expiry or other table maintenance during the run
   unless declared as a named variant in the result.

Rule 1 is the one engines get wrong most often: a reader that starts at the
latest offset skips the head of the topic, which the producer wrote before
your engine was asked to consume, and the run is scored as having lost it.

Rule 6 is about the scoring window only. Compaction is a legitimate thing to
measure; it just cannot run unannounced inside a freshness measurement, since
a rewrite re-adds rows the scorer has already tallied.

## What the harness hands you

Staging writes `runs/<run_id>/facts.json`. It is the whole interface:

| Key | What to do with it |
|---|---|
| `run_id` | the run's name; also the topic's, and a good consumer-group id |
| `bootstrap` | Kafka bootstrap servers |
| `topic` | the topic to consume, from its earliest offset |
| `corpus_uri` | the corpus directory, for reference |
| `schema_avsc_uri` | the Avro schema the values are encoded with |
| `value_encoding` | `avro` for raw Avro binary, `confluent` for the Confluent wire format |
| `schema_registry_url` | the registry the schema was registered with, or `null` |
| `schema_subject` | the subject it was registered under, or `null` |
| `schema_id` | the id every record's header names, or `null` |
| `catalog_props` | Iceberg catalog properties, credentials redacted |
| `table` | the `namespace.table` to append to |
| `partition` | how the table is partitioned |
| `ddl` | the exact statement your table must match, or `null` when the harness already created it |
| `key_column` | the corpus column sent as the message key, or `null` for unkeyed records |
| `epoch` | `null` until the run is launched; the time origin is chosen when the producer starts |

The credentials are redacted because `facts.json` is meant to be publishable.
Your own catalog credentials come from wherever you keep them — `site.yaml`
holds the harness's copy.

### Confluent values

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
scoring are what a raw-Avro run's are.

The shipped external spec for this is `runs/smoke-external-confluent.yaml`.
Flink cannot be the engine on it — its registry format cannot plan the
corpus's microsecond timestamp column, and `engines/flink/README.md` has the
two upstream reasons — so the walk-through below is a raw-Avro run.

## Walk-through: an engine the harness does not manage

This runs the local stack, stages an external run, and starts Flink by hand as
the stand-in for "your engine". Nothing after staging knows what is consuming
the topic.

The engine config in `docs/examples/external-flink/` is the config a Flink user
would write from the facts above: a Kafka source declaring the corpus columns,
a catalog pointed at the local stack, and one `INSERT`. The run's names are
left as `@PLACEHOLDER@` tokens. If your engine is not Flink, this directory is
the shape of the thing you have to produce for it — not something to copy.

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
stdin instead, which is what you want when you are driving it by hand.

## Reading the verdict

`runs/<run_id>/scores/summary.json` is the whole answer; the script prints the
part that matters.

| Field | Meaning |
|---|---|
| `run_valid` | the only field that decides whether a result may be published: the table held the corpus's columns and drained, freshness held, exactness was clean and the producer kept its schedule |
| `state` | `drained`, `idle_stop`, `producer_bound` or `void` |
| `producer_bound` | the offer, not your engine, set the rate — the run says nothing about the engine |
| `prefix` / `last_batch` | the newest fully-arrived batch, against the last one offered; equal means drained |
| `freshness.window` | p50/p95/p99/max lag in seconds, over the run after the warmup |
| `exactness.exact` | no loss, no duplication, no corruption in any offered batch |
| `keepup.absorbed_at_offer_end` | committed rows over offered rows at the instant the last batch was acked; `1.0` is no standing debt |
| `keepup.drain_s` | seconds from that instant until the table caught up |

`exactness.violations` names the first faults when `exact` is false, and
`freshness.lag_series` in `freshness.json` carries the whole lag curve on both
clocks. `gate` reads the same artifacts while a run is still going and answers
`PASS`, `UNDERSIZED` or `VOID`.

## Tier 2: a managed engine

`engines/<name>/` holds everything engine-specific. The harness contains no
engine branches outside it.

| File | Purpose |
|---|---|
| `README.md` | what the engine runs, knobs it honours, known traps |
| `Dockerfile` | stock upstream image plus connector jars; pinned versions; built by CI to GHCR and buildable locally |
| the cluster documents | the custom resource a run is, and the ConfigMap its pods mount, both rendered from a run spec by `knobs.py` |
| `compose.yaml` | the engine's services for the local stack |
| job source | the job the engine runs (SQL or Python) |
| `knobs.py` | run-spec keys the engine accepts, validation, rendering into the template and the DDL |
| `verify.py` | reads effective state from the running engine and fails staging on drift from the spec |
| `fleet.py` | requested vCPU and GiB per role from the spec, for `run.json` |

That table is the contract, not the current state: phase 1 ships only the
README, the Dockerfile, `compose.yaml`, the job source and `knobs.py`, and
`engines/flink/` is the worked example of those five. Two rules in it are worth
copying:

- **Render, never reach.** Nothing in `knobs.py` touches a cluster, so a run's
  whole configuration can be read, and diffed against another run's, before any
  compute is paid for.
- **Refuse unknown keys.** A misspelled knob costs one error message instead of
  a published result whose tuning silently did not apply.

Register it by adding its knobs module to `MANAGED` in
`ingest_bench/specs/engines.py`, and `engine: <name>` plus a `<name>:` block of
knobs becomes a usable spec. The module owes two functions:
`validate(block, spec, meta)`, which refuses a block that cannot describe a
runnable engine, and `render(spec, site, derived, meta)`, which returns the files
to write into the run directory keyed by filename.

For a run on a cluster it owes one more thing: `KUBERNETES`, an
`EngineKubernetes` naming the kind of object a run is, where its state sits in
the status, what running is called there, the Service that carries its HTTP API
and the labels its pods carry. The cluster drivers read those through
`engine-k8s` and hold no engine's names of their own, so a third engine adds no
line to `scripts/`.
