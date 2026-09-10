# The run spec and the site config

Two YAML files. A **run spec** under `runs/` is the run: the workload, the table,
the offer and the engine's own knobs, and it is copied verbatim into a published
result. A **site config** (`./site.yaml`, `--site` elsewhere) is one operator's
storage, broker, catalog, cluster and prices, and never leaves their machine.

Both loaders refuse a key they do not recognise. A misspelled knob would
otherwise run one thing and publish a spec claiming another, and the two would
never disagree loudly.

Terms below are defined in [`methodology.md`](methodology.md).

## Run spec

| Key | Default | Effect |
|---|---|---|
| `name` | *required* | the run's base name. It becomes a topic, a table and a Kubernetes object, so it must match `^[a-z0-9][a-z0-9-]{2,34}$` — 35 characters is what survives a run id's stamp and a driver's longest prefix inside the 63-character label a Job stamps on its pods |
| `engine` | *required* | `external`, or a managed engine — `flink` or `spark` |
| `corpus` | *required* | a shipped preset name, or a path to a preset file. See [`corpus.md`](corpus.md) |
| `<engine>:` | *required* for a managed engine | that engine's knobs; see `engines/<name>/README.md`. Refused for `engine: external` |
| `external:` | *required* for `engine: external` | `{name, version, notes}`, all strings. What its operator says ran, since the harness never ran it |
| `fleet:` | *required* for `engine: external` | a non-empty list of `{role, count, vcpu, gib, machine_type}`. The compute the run is costed against. A managed engine derives its own |

### `table`

| Key | Default | Effect |
|---|---|---|
| `managed_by` | `harness` | `harness` creates the table, so the run's properties are the ones the writer sees. `engine` hands your engine the equivalent `CREATE TABLE` in `facts.ddl` and creates nothing — the scorer then reads a table that does not exist yet as empty and keeps polling, since such an engine creates it from its first record |
| `partition` | `identity(partition_key)` | `identity(col)`, `bucket(N, col)` or `unpartitioned` |
| `properties` | `{}` | Iceberg table properties, set at creation. See [`pitfalls.md`](pitfalls.md) |

### `kafka` (required)

| Key | Default | Effect |
|---|---|---|
| `partitions` | *required* | topic partitions. Replication is not a knob: staging asks for `min(3, brokers)` |
| `key` | *required* | the corpus key column sent as the message key, or `none` for unkeyed records. The key decides how records distribute across partitions, so it belongs to the workload |
| `value_encoding` | `avro` | `avro` is the corpus's Avro binary as it stands; `confluent` is the same bytes behind the five-byte Confluent header, which needs `site.kafka.schema_registry` |

### `producer`

| Key | Default | Effect |
|---|---|---|
| `speed` | `1.0` | replay speed. `2.0` offers the corpus's rate twice over |
| `seconds` | whole corpus | replay only this many seconds of it. A shortened offer is a probe, and is not publishable |
| `shards` | `1` | processes the offer is split across. See [`corpus.md`](corpus.md) for the shard count a rate needs |
| `behind_max_ms` | `5000` | a batch acknowledged this far past its due time makes the run `producer_bound` |
| `compression` | `zstd` | the wire codec: `zstd`, `lz4`, `snappy`, `gzip` or `none`. Two results compare only at one codec |

### `scoring`

| Key | Default | Effect |
|---|---|---|
| `freshness_bound_s` | `180` | the window p95 the run is judged against; twice it is the max bound |
| `warmup_s` | `120` | excluded from the freshness window after the epoch |
| `geometry_offsets_s` | `[600, 1200, 1800, 2700, 3600]` | when geometry is measured. A rung the run never reached reads `absent` |
| `gate_adaptation_s` | the gate's own `120` | nothing is judged undersized before this |
| `gate_window_s` | the gate's own `60` | the width of the three backlog-floor windows |

The two gate keys are absent unless a run says otherwise, so the gate keeps its
own defaults rather than having them restated in every spec.

## Site config

| Key | Effect |
|---|---|
| `corpus_root`, `runs_root`, `warehouse` | *required*. Where corpora, run artifacts and table data live. All three are substituted out of a published result. The drivers reach storage through the `aws` CLI, so each must be an `s3://` URI and anything else is refused by name |
| `kafka.bootstrap_servers` | *required* |
| `kafka.security` | librdkafka `security.*` / `sasl.*` properties, passed to every client verbatim. Never read into a result |
| `kafka.schema_registry` | `{url, basic_auth_user_info?}`. Absent is the answer for a site whose runs are all raw Avro; a `confluent` run against such a site is refused at staging |
| `catalog.props` | *required*. pyiceberg catalog properties. An Iceberg **REST** catalog: any other `type` is refused at stage time |
| `kubernetes` | empty means no cluster and everything runs where it is started. A cluster sets `context`, `namespace`, `harness_service_account`, `flink_service_account` and `registry`, and may set `spark_service_account` (default `ingest-bench-spark`), `aws_region`, `secret_name` (§Secrets), `service_account_annotations`, `node_selector` and `tolerations` |
| `pricing` | *required*. `{vcpu_hour_usd, gib_hour_usd}`, the two rates a run's cost is computed from |

`site.example.yaml` and `site.aws.example.yaml` are annotated copies to fill in.
Every `YOUR_` placeholder must go: loading refuses one that survived a copy,
rather than sending it to a broker as a hostname.

### What an engine is given, and what is translated

`site.catalog.props` and `site.kafka.security` reach a managed engine's rendered
configuration almost verbatim. Four keys are translated, because the engine's
own name for the thing differs — three from the catalog, and one from Kafka:

| From the site | What an engine gets |
|---|---|
| `type` (`rest`, or absent) | the engine's own REST catalog binding |
| `s3.region` | `client.region` |
| `warehouse` on `s3://` or `gs://` | the matching `io-impl`. A catalog's own `warehouse` is not always a location — a Glue REST endpoint takes a catalog id there — so the scheme is read off `site.warehouse` instead |
| `sasl.mechanism: OAUTHBEARER` plus `aws.region` | the Java client's `AWS_MSK_IAM` login module and its callback handler |

`aws.region` is the harness's own pseudo-key, not a librdkafka property: Amazon
MSK's `OAUTHBEARER` wants a token signed from the caller's own credentials per
connection, which no property can express, so the region to sign in is stated
beside the mechanism and stripped before the properties reach a client. Install
the harness with its `aws` extra for it. A site that arranges its own tokens sets
any `sasl.oauthbearer.*` property instead and is passed through untouched.

**`site.kafka.security` and `--kafka-prop` may not carry a `compression.*`
property.** Those two are what reach a client over the producer's own
configuration, so one of them would decide the wire codec while the run's
published facts name `producer.compression`. Each is scanned where it is read —
the site as it loads, the flag before the producer opens a connection — and a
stated conflict is an error rather than a preference. The catalog properties are
not scanned, because nothing there reaches a Kafka client.

### Secrets

Write `${env:NAME}` for a credential in `site.kafka.security`, `site.catalog.props`,
`site.kafka.schema_registry.basic_auth_user_info`, a `--catalog-prop` or a
`--kafka-prop`. Nothing resolves at load: the variable is read inside the process
that uses it, at the call that needs it, and an unset one is refused by name
rather than substituted empty. The site config, the run's `facts.json`, an
engine's rendered files and a published result all keep the placeholder, since it
names the variable a reader has to set.

On a cluster the variable comes from one Secret in the run's namespace, created
once and named as `site.kubernetes.secret_name`:

```bash
kubectl --namespace ingest-bench create secret generic bench-env \
  --from-literal=IB_KAFKA_PASSWORD=... --from-literal=IB_REGISTRY_AUTH=...
```

Every key of it becomes an environment variable on every pod a run creates —
each harness Job, and both halves of the engine's fleet. One Secret rather than
a key per property, because a `${env:NAME}` names a variable and a Secret's keys
are already a set of variable names: nothing here holds a list of which of your
properties are credentials.

Which is why **a site declaring a cluster refuses a credential written out in
full**, naming the key: those properties are applied as a ConfigMap and uploaded
to the runs prefix, and no later redaction undoes a value that has been in
either. A site with no cluster is the local stack, whose credentials are a
container image's published defaults, and it keeps its literals — redacted out
of `facts.json` and out of a result by property name.

One limit. A Flink run resolves its whole rendered script and settings inside
its own submitter, so any option in either may name a variable; a Spark run
resolves the Kafka source's options and nothing else, because a Spark setting is
read by the framework and substitutes nothing. A reference among those is
refused at render time rather than reaching the catalog as six literal
characters, so a credential a Spark run needs belongs in `site.kafka.security`.
MSK's IAM authentication needs no secret at all, on either engine.

Object storage and catalogs otherwise use the cloud SDK's default credential
chain: pod identity or an instance role in a cluster, an ambient profile on a
laptop. Static keys are for the local stack, where they are its published
defaults.
