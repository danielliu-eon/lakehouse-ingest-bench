# The run spec and the site config

Two YAML files configure a run. A **run spec** under `runs/` defines the workload,
table, producer and engine settings; published results include an unchanged copy.
A **site config** (`./site.yaml`, or a path supplied with `--site`) defines the
operator's storage, broker, catalog, cluster and prices. It is not published.

Both loaders reject unknown keys so misspelled settings cannot be silently ignored.
The run-spec loader checks numeric ranges before staging creates resources.

See the [methodology glossary](methodology.md#glossary) for measurement terms.

## Run spec

| Key | Default | Effect |
|---|---|---|
| `name` | *required* | base name for the topic, table and Kubernetes objects. Must match `^[a-z0-9][a-z0-9-]{2,34}$` (3–35 characters), leaving room for timestamps and prefixes within Kubernetes limits. Engine limits may be stricter: Flink allows at most **28** characters |
| `engine` | *required* | `external`, or a managed engine — `flink` or `spark` |
| `corpus` | *required* | a shipped preset name, or a path to a preset file. See [`corpus.md`](corpus.md) |
| `<engine>:` | *required* for a managed engine | that engine's knobs; see `engines/<name>/README.md`. Refused for `engine: external` |
| `external:` | *required* for `engine: external` | `{name, version, notes}`, all strings. Operator-supplied engine identity and tuning notes |
| `fleet:` | *required* for `engine: external` | a non-empty list of `{role, count, vcpu, gib, machine_type}` for cluster cost. Managed cluster costs use captured pod requests; local runs have no resource cost |

### `table`

| Key | Default | Effect |
|---|---|---|
| `managed_by` | `harness` | `harness` creates the table with the specified properties. `engine` supplies equivalent DDL in `facts.ddl` for your engine to execute; the scorer treats an absent table as empty while waiting |
| `partition` | `identity(partition_key)` | `identity(col)`, `bucket(N, col)` or `unpartitioned` |
| `properties` | `{}` | Iceberg table properties, set at creation. See [`pitfalls.md`](pitfalls.md) |

### `kafka` (required)

| Key | Default | Effect |
|---|---|---|
| `partitions` | *required* | topic partitions. Replication is not a knob: staging asks for `min(3, brokers)` |
| `key` | *required* | corpus column used as the Kafka message key, or `none` for unkeyed records; controls distribution across topic partitions |
| `value_encoding` | `avro` | `avro` sends raw binary records; `confluent` adds a five-byte header and requires `site.kafka.schema_registry` |

### `producer`

| Key | Default | Effect |
|---|---|---|
| `speed` | `1.0` | replay speed multiplier; `2.0` doubles the offered rate |
| `seconds` | whole corpus | limit the replay to this many corpus seconds; shortened offers cannot be published |
| `shards` | `1` | producer process count; see [sizing guidance](corpus.md#sizing-the-offer) |
| `behind_max_ms` | `5000` | maximum permitted batch acknowledgement delay after its due time; exceeding it makes the run `producer_bound` |
| `compression` | `zstd` | the wire codec: `zstd`, `lz4`, `snappy`, `gzip` or `none`. Use the same codec for compared runs |

Replay speed must be finite and positive. Supplied `seconds` and `shards` must
be positive integers; `behind_max_ms` must be nonnegative.

### `scoring`

| Key | Default | Effect |
|---|---|---|
| `freshness_bound_s` | `180` | maximum window p95 lag; maximum window lag must not exceed twice this bound |
| `warmup_s` | `120` | excluded from the freshness window after the epoch |
| `geometry_offsets_s` | `[600, 1200, 1800, 2700, 3600]` | geometry snapshot offsets from the epoch; unreached offsets are `absent` |
| `gate_adaptation_s` | `120` (gate default) | seconds after the epoch before capacity checks begin |
| `gate_window_s` | `60` (gate default) | the width of the three backlog-floor windows |

Omit the gate keys to use the gate's defaults. `freshness_bound_s` must be finite
and positive; `gate_window_s` must be positive. `warmup_s` and
`gate_adaptation_s` must be nonnegative. Geometry offsets must be nonnegative
integers in strictly increasing order.

## Site config

| Key | Effect |
|---|---|
| `corpus_root`, `runs_root`, `warehouse` | *required*. Storage roots for corpora, run artifacts and table data; redacted in results. Shell drivers use the AWS CLI and require `s3://` URIs |
| `kafka.bootstrap_servers` | *required* |
| `kafka.deployment` | `managed`, `in-cluster`, or `external`. Required by AWS and in-cluster stack setup and teardown; optional for run drivers and existing sites. On AWS, only `managed` provisions MSK |
| `kafka.security` | librdkafka `security.*` / `sasl.*` properties; managed engines apply the translations below. Omitted from results |
| `kafka.schema_registry` | `{url, basic_auth_user_info?}`; optional for raw Avro, required for Confluent framing |
| `catalog.props` | *required*. PyIceberg catalog properties. Managed engines require a REST catalog; external runs may use any catalog PyIceberg can open |
| `kubernetes` | empty for local runs. Cluster runs require `context`, `namespace`, `harness_service_account`, `flink_service_account` and `registry`, and may set `spark_service_account` (default `ingest-bench-spark`), `aws_region`, `secret_name` (§Secrets), `service_account_annotations`, `node_selector` and `tolerations` |
| `pricing` | *required*. `{vcpu_hour_usd, gib_hour_usd}` rates used to calculate cost |

Start from `site.example.yaml`, `site.aws.example.yaml`, or `site.k8s.example.yaml`.
Replace every `YOUR_` placeholder; the loader rejects any that remain.

### Engine configuration translation

`site.catalog.props` and `site.kafka.security` are copied into managed-engine
configuration, with these translations:

| From the site | What an engine gets |
|---|---|
| `type` (`rest`, or absent) | the engine's own REST catalog binding |
| `s3.region` | `client.region` |
| `warehouse` on `s3://` or `gs://` | the matching `io-impl`, selected from `site.warehouse`. The catalog warehouse may be an ID, as in Glue, rather than a storage URI |
| `sasl.mechanism: OAUTHBEARER` plus `aws.region`, without `sasl.oauthbearer.*` | the Java client's `AWS_MSK_IAM` login module and its callback handler |

`aws.region` configures MSK token signing and is removed before passing
properties to librdkafka. Install the harness with the `aws` extra to use it.
If the site supplies a `sasl.oauthbearer.*` property, the harness leaves that
OAuth configuration unchanged for its own clients. Managed Spark and Flink runs
reject these librdkafka-specific OAuth properties before staging creates resources;
they are not translated into Java OAuth settings. Use an external engine for
explicit OAuth, or omit these properties when using Amazon MSK IAM.

**Set compression only through `producer.compression`.** The site loader and
producer reject `compression.*` keys in `site.kafka.security` and `--kafka-prop`
to prevent the actual codec from differing from the published spec. Catalog
properties do not reach Kafka clients and are not subject to this check.

### Secrets

Use `${env:NAME}` for credentials in Kafka security, catalog properties,
schema-registry authentication, `--catalog-prop`, or `--kafka-prop`.
References resolve only in the process that uses them; an unset variable is
an error. Site configs, staged facts, rendered files and results retain the
reference so operators can identify which variables to supply.

On a cluster the variable comes from one Secret in the run's namespace, created
once and named as `site.kubernetes.secret_name`:

```bash
kubectl --namespace ingest-bench create secret generic bench-env \
  --from-literal=IB_KAFKA_PASSWORD=... --from-literal=IB_REGISTRY_AUTH=...
```

Every key in that Secret becomes an environment variable on every harness and
engine pod. The keys must match the names used in `${env:NAME}` references.

**Cluster sites reject literal credentials** because rendered properties are
stored in ConfigMaps and uploaded to the runs prefix. Use environment references
to keep secrets out of those artifacts. Local sites permit literals for the
stack's public default credentials; credential-named properties are redacted
from `facts.json` and results.

Flink resolves environment references throughout its rendered script and
settings. Spark resolves them only in Kafka source options and rejects them in
other settings at render time. Put referenced Spark credentials in
`site.kafka.security`. Both engines support MSK IAM authentication without a
Secret.

Object storage and catalogs otherwise use the cloud SDK's default credential
chain, such as pod identity, an instance role or an ambient profile. The local
stack uses public default credentials.
