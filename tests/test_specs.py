# SPDX-License-Identifier: Apache-2.0
import json
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path

import pytest
import yaml

from ingest_bench import catalog as cat
from ingest_bench import kafka_admin, stage, uri
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.specs import derive, engines, model

ROOT = Path(__file__).resolve().parents[1]


def test_smoke_specs_load() -> None:
    flink = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    assert flink.engine == "flink" and flink.table.managed_by == "harness" and flink.producer.speed == 1.0
    assert flink.scoring.warmup_s == 60 and flink.scoring.geometry_offsets_s == (600, 1200, 1800, 2700, 3600)
    assert flink.engine_block["taskmanagers"] == 2
    ext = model.load_run_spec(ROOT / "runs" / "smoke-external.yaml")
    assert ext.external is not None and ext.external.name == "your-engine" and ext.fleet[0].vcpu == 2


def test_spec_refusals(tmp_path: Path) -> None:
    base = yaml.safe_load((ROOT / "runs" / "smoke-external.yaml").read_text())
    mutations: tuple[tuple[Callable[[dict[str, object]], object], str], ...] = (
        (lambda d: d.pop("kafka"), "kafka"),
        (lambda d: d.__setitem__("colour", "blue"), "unknown"),
        (lambda d: d.pop("fleet"), "fleet"),
        (lambda d: d.__setitem__("name", "Bad Name"), "name"),
        (lambda d: d.__setitem__("engine", "unicorn"), "unicorn"),
    )
    for mutate, message in mutations:
        d = yaml.safe_load(yaml.safe_dump(base))
        mutate(d)
        p = tmp_path / "s.yaml"
        p.write_text(yaml.safe_dump(d))
        with pytest.raises(ValueError, match=message):
            model.load_run_spec(p)


def test_a_name_too_long_to_reach_a_job_is_refused_where_it_is_written(tmp_path: Path) -> None:
    """The Job controller's 63-character `job-name` label is the binding limit.

    A run id is the name plus a 17-character stamp and the longest prefix a
    driver puts in front of one is `drop-topic-`, so a longer name fails after
    the topic, the table and the engine already exist.
    """
    base = yaml.safe_load((ROOT / "runs" / "smoke-external.yaml").read_text())
    path = tmp_path / "s.yaml"
    longest = "a" * 35
    base["name"] = longest
    path.write_text(yaml.safe_dump(base))
    spec = model.load_run_spec(path)
    run_id = derive.derive(spec, _bare_site(), stamp="20260908T120000Z", corpus_dir="smoke-1a2b3c4d").run_id
    assert len(f"drop-topic-{run_id}") == 63, run_id

    base["name"] = "a" * 36
    path.write_text(yaml.safe_dump(base))
    with pytest.raises(ValueError, match="spec.name names a topic"):
        model.load_run_spec(path)


def test_the_value_encoding_defaults_to_avro_and_refuses_a_name_it_does_not_know(tmp_path: Path) -> None:
    """The wire format is the corpus's own unless a run asks for the other one."""
    assert model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml").kafka.value_encoding == "avro"
    confluent = model.load_run_spec(ROOT / "runs" / "smoke-external-confluent.yaml")
    assert confluent.kafka.value_encoding == "confluent"

    base = yaml.safe_load((ROOT / "runs" / "smoke-external.yaml").read_text())
    path = tmp_path / "s.yaml"
    for encoding, message in (("protobuf", "value_encoding"), (7, "must be a string")):
        base["kafka"] = {**base["kafka"], "value_encoding": encoding}
        path.write_text(yaml.safe_dump(base))
        with pytest.raises(ValueError, match=message):
            model.load_run_spec(path)


def test_the_producer_compression_defaults_to_zstd_and_refuses_a_codec_it_does_not_know(tmp_path: Path) -> None:
    """The wire codec is the run's own, and only ever one a client can be handed."""
    assert model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml").producer.compression == "zstd"

    base = yaml.safe_load((ROOT / "runs" / "smoke-external.yaml").read_text())
    path = tmp_path / "s.yaml"
    for codec in sorted(model.COMPRESSIONS):
        base["producer"] = {"compression": codec}
        path.write_text(yaml.safe_dump(base))
        assert model.load_run_spec(path).producer.compression == codec

    base["producer"] = {"compression": "brotli"}
    path.write_text(yaml.safe_dump(base))
    with pytest.raises(ValueError, match=r"\['gzip', 'lz4', 'none', 'snappy', 'zstd'\], got 'brotli'"):
        model.load_run_spec(path)
    base["producer"] = {"compression": 7}
    path.write_text(yaml.safe_dump(base))
    with pytest.raises(ValueError, match="must be a string"):
        model.load_run_spec(path)


def test_a_site_may_not_choose_the_wire_codec_with_a_client_property(tmp_path: Path) -> None:
    """A `compression.*` client property is refused, and named, at site load.

    Site properties are applied over the producer's own configuration, so such a
    key would decide the wire while `facts.json` and `run.json` publish the
    codec the spec asked for. The codec belongs to the run; a site that states
    one is a conflict rather than a preference.
    """
    with pytest.raises(ValueError, match=r"compression\.type.*producer\.compression"):
        model.load_site(_site_file(tmp_path, "file:///corpus", security={"compression.type": "gzip"}))
    with pytest.raises(ValueError, match=r"compression\.level.*producer\.compression"):
        model.load_site(_site_file(tmp_path, "file:///corpus", security={"compression.level": "9"}))
    # The properties beside it are what the site is for, and still reach a client.
    loaded = model.load_site(_site_file(tmp_path, "file:///corpus", security={"security.protocol": "SASL_SSL"}))
    assert loaded.kafka_security == {"security.protocol": "SASL_SSL"}


def test_the_site_reads_a_schema_registry_and_keeps_its_reference(tmp_path: Path) -> None:
    """The registry is optional, and its credential stays the reference the file wrote.

    Resolving at load would put the value in the loaded config, which is what
    every rendered file and every published artifact is written from.
    """
    path = tmp_path / "site.yaml"

    def write(kafka: str) -> None:
        path.write_text(
            "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
            f"kafka: {kafka}\n"
            "catalog: {props: {type: sql}}\nkubernetes: {}\n"
            "pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}\n"
        )

    write("{bootstrap_servers: 'localhost:9092'}")
    assert model.load_site(path).schema_registry is None

    write(
        "{bootstrap_servers: 'localhost:9092', schema_registry: "
        "{url: 'http://registry:8080/apis/ccompat/v7', basic_auth_user_info: '${env:IB_REGISTRY_AUTH}'}}"
    )
    assert model.load_site(path).schema_registry == model.SchemaRegistryConfig(
        url="http://registry:8080/apis/ccompat/v7", basic_auth_user_info="${env:IB_REGISTRY_AUTH}"
    )

    write("{bootstrap_servers: 'localhost:9092', schema_registry: {url: 'http://registry:8080'}}")
    registry = model.load_site(path).schema_registry
    assert registry is not None and registry.basic_auth_user_info is None

    for kafka, message in (
        ("{bootstrap_servers: 'x:9092', schema_registry: {basic_auth_user_info: 'a:b'}}", "must set url"),
        ("{bootstrap_servers: 'x:9092', schema_registry: {url: 'u', token: 't'}}", "unknown keys"),
        ("{bootstrap_servers: 'x:9092', schema_registry: {url: 'u', basic_auth_user_info: ''}}", "is empty"),
        ("{bootstrap_servers: 'x:9092', registry: {url: 'u'}}", "site.kafka has unknown keys"),
    ):
        write(kafka)
        with pytest.raises(ValueError, match=message):
            model.load_site(path)


def test_the_gate_keys_are_optional_and_typed(tmp_path: Path) -> None:
    """A spec that says nothing about the gate leaves the gate its own defaults.

    Absent rather than a copy of the scorer's numbers: two files carrying one
    default drift, and the loser is the file nobody reread.
    """
    base = yaml.safe_load((ROOT / "runs" / "smoke-external.yaml").read_text())
    shipped = model.load_run_spec(ROOT / "runs" / "smoke-external.yaml").scoring
    assert shipped.gate_adaptation_s is None and shipped.gate_window_s is None

    path = tmp_path / "s.yaml"
    base["scoring"] = {**base["scoring"], "gate_adaptation_s": 300, "gate_window_s": 90}
    path.write_text(yaml.safe_dump(base))
    scoring = model.load_run_spec(path).scoring
    assert scoring.gate_adaptation_s == 300 and scoring.gate_window_s == 90

    base["scoring"] = {**base["scoring"], "gate_window_s": "90"}
    path.write_text(yaml.safe_dump(base))
    with pytest.raises(ValueError, match="gate_window_s"):
        model.load_run_spec(path)


def test_site_refuses_placeholders(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="YOUR_"):
        model.load_site(ROOT / "site.example.yaml")
    good = tmp_path / "site.yaml"
    good.write_text(
        "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
        "kafka: {bootstrap_servers: 'localhost:9092', security: {}}\n"
        "catalog: {props: {type: sql, uri: 'sqlite:////tmp/x.db', warehouse: 'file:///tmp/w'}}\n"
        "kubernetes: {}\npricing: {vcpu_hour_usd: 0.03, gib_hour_usd: 0.004}\n"
    )
    site = model.load_site(good)
    assert site.kafka_bootstrap == "localhost:9092" and site.catalog_props["type"] == "sql"


CLUSTER: dict[str, object] = {
    "context": "my-cluster",
    "namespace": "ingest-bench",
    "harness_service_account": "ingest-bench-harness",
    "flink_service_account": "ingest-bench-flink",
    "registry": "registry.example/ingest-bench",
    "aws_region": "eu-west-1",
}


def _cluster_site(tmp_path: Path, cluster: dict[str, object]) -> Path:
    path = tmp_path / "site.yaml"
    path.write_text(
        "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
        "kafka: {bootstrap_servers: 'localhost:9092'}\n"
        "catalog: {props: {type: sql}}\n"
        "pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}\n" + yaml.safe_dump({"kubernetes": cluster})
    )
    return path


def test_an_empty_kubernetes_block_means_no_cluster(tmp_path: Path) -> None:
    assert model.load_site(_cluster_site(tmp_path, {})).kubernetes is None


def test_the_kubernetes_block_loads_a_cluster(tmp_path: Path) -> None:
    cluster: dict[str, object] = {
        **CLUSTER,
        "service_account_annotations": {"example.com/role": "arn"},
        "node_selector": {"kubernetes.io/arch": "amd64"},
        "tolerations": [{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
    }
    loaded = model.load_site(_cluster_site(tmp_path, cluster)).kubernetes
    assert loaded == model.KubernetesConfig(
        context="my-cluster",
        namespace="ingest-bench",
        harness_service_account="ingest-bench-harness",
        flink_service_account="ingest-bench-flink",
        spark_service_account="ingest-bench-spark",
        service_account_annotations={"example.com/role": "arn"},
        registry="registry.example/ingest-bench",
        aws_region="eu-west-1",
        secret_name=None,
        node_selector={"kubernetes.io/arch": "amd64"},
        tolerations=[{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
    )


@pytest.mark.parametrize("path", sorted((ROOT / "runs").glob("*.yaml")), ids=lambda path: path.name)
def test_every_shipped_run_spec_loads(path: Path) -> None:
    """A spec that does not load is one an operator finds out about at stage time.

    Each of these is copied and edited rather than written from nothing, so the
    shipped ones are the shape every run of that engine starts from.
    """
    spec = model.load_run_spec(path)
    assert spec.name == path.stem
    assert spec.engine in {*engines.MANAGED, "external"}
    if spec.engine in engines.MANAGED:
        assert spec.engine_block, f"{path.name} names {spec.engine} and gives it no knobs"


@pytest.mark.parametrize("path", sorted((ROOT / "runs").glob("*.yaml")), ids=lambda path: path.name)
def test_every_shipped_run_spec_holds_knobs_its_engine_takes(path: Path, corpus: tuple[str, str]) -> None:
    """A knob a shipped spec's engine refuses is a spec nobody can stage.

    The loader carries the engine block through unread — the knobs belong to
    the engine that declares them — so loading a spec says nothing about
    whether the run it asks for is one the engine can be given. Neither
    validator reads the corpus, so the smoke one stands in for whichever the
    spec names.
    """
    spec = model.load_run_spec(path)
    if spec.engine not in engines.MANAGED:
        return
    corpus_root, corpus_dir = corpus
    engines.knobs_for(spec.engine).validate(spec.engine_block, spec, metadata.read(uri.join(corpus_root, corpus_dir)))


def test_a_full_scale_spec_ships_for_each_managed_engine() -> None:
    """The cloud sequence in docs/running.md generates a scale corpus and stages a spec.

    With only smoke specs shipped, the first thing a stranger following it had
    to do was author one and size a fleet with no example to copy. One spec per
    managed engine over the same preset, so the two are comparable.
    """
    preset_name = "events-100mbs-skew"
    assert (ROOT / "workloads" / "presets" / f"{preset_name}.yaml").exists()
    scale = {
        spec.engine: spec
        for spec in (model.load_run_spec(path) for path in sorted((ROOT / "runs").glob("*.yaml")))
        if spec.corpus == preset_name
    }
    assert set(scale) == set(engines.MANAGED), f"{preset_name} has no shipped spec for every managed engine"
    for engine, spec in scale.items():
        assert spec.kafka.partitions == 32, engine
        assert spec.producer.shards == 2, engine
        assert spec.producer.compression == "lz4", engine
        assert spec.scoring.freshness_bound_s == 180.0 and spec.scoring.warmup_s == 120, engine


def test_the_spark_account_is_the_one_setup_creates_unless_the_site_renames_it(tmp_path: Path) -> None:
    """Defaulted, unlike the two beside it, so an older site config still loads.

    A site written before Spark could be staged on a cluster names two accounts
    and not three, and `deploy/aws/setup.sh` creates this one under exactly
    this name.
    """
    loaded = model.load_site(_cluster_site(tmp_path, dict(CLUSTER))).kubernetes
    assert loaded is not None and loaded.spark_service_account == "ingest-bench-spark"
    renamed = model.load_site(_cluster_site(tmp_path, {**CLUSTER, "spark_service_account": "sparky"})).kubernetes
    assert renamed is not None and renamed.spark_service_account == "sparky"


def test_the_placement_keys_are_optional(tmp_path: Path) -> None:
    """A cluster that places workloads nowhere in particular says nothing about it."""
    loaded = model.load_site(_cluster_site(tmp_path, dict(CLUSTER))).kubernetes
    assert loaded is not None
    assert loaded.service_account_annotations == {} and loaded.node_selector == {} and loaded.tolerations == []


def test_the_aws_region_is_optional_but_never_empty(tmp_path: Path) -> None:
    """A cluster on another cloud leaves the key out; an empty one names no region at all."""
    elsewhere = {key: value for key, value in CLUSTER.items() if key != "aws_region"}
    loaded = model.load_site(_cluster_site(tmp_path, elsewhere)).kubernetes
    assert loaded is not None and loaded.aws_region is None
    on_aws = model.load_site(_cluster_site(tmp_path, dict(CLUSTER))).kubernetes
    assert on_aws is not None and on_aws.aws_region == "eu-west-1"
    with pytest.raises(ValueError, match="aws_region is empty"):
        model.load_site(_cluster_site(tmp_path, {**CLUSTER, "aws_region": ""}))


def test_the_kubernetes_block_refuses_what_it_does_not_recognise(tmp_path: Path) -> None:
    for cluster, message in (
        ({**CLUSTER, "zone": "eu-west-1a"}, "unknown keys"),
        ({key: value for key, value in CLUSTER.items() if key != "namespace"}, "namespace"),
        ({**CLUSTER, "tolerations": {"key": "bench"}}, "list of mappings"),
        ({**CLUSTER, "node_selector": {"arch": 64}}, "node_selector.arch"),
    ):
        with pytest.raises(ValueError, match=message):
            model.load_site(_cluster_site(tmp_path, cluster))


def test_the_cluster_names_the_secret_its_pods_read(tmp_path: Path) -> None:
    """One optional name, which is how a `${env:NAME}` in the file is answered.

    Absent where nothing references a variable; never empty, because an
    `envFrom` naming no Secret is refused by the API server at apply time
    instead of here, where the file that asked for it is still in hand.
    """
    absent = model.load_site(_cluster_site(tmp_path, dict(CLUSTER))).kubernetes
    assert absent is not None and absent.secret_name is None
    named = model.load_site(_cluster_site(tmp_path, {**CLUSTER, "secret_name": "ingest-bench-env"})).kubernetes
    assert named is not None and named.secret_name == "ingest-bench-env"
    with pytest.raises(ValueError, match="secret_name is empty"):
        model.load_site(_cluster_site(tmp_path, {**CLUSTER, "secret_name": ""}))


def test_a_cluster_site_refuses_a_credential_it_would_render_into_the_cluster(tmp_path: Path) -> None:
    """A run on a cluster renders these properties into a ConfigMap and the bucket.

    So a literal is refused where a reference is the only safe form, and the
    refusal names the key and the form. A site with no cluster is the local
    stack: its credentials are an image's published defaults and never leave
    the machine, so the same value loads there.
    """
    path = tmp_path / "site.yaml"

    def written(kafka: str = "", catalog: str = "", cluster: dict[str, object] | None = None) -> Path:
        path.write_text(
            "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
            f"kafka: {{bootstrap_servers: 'localhost:9092', security: {{{kafka}}}}}\n"
            f"catalog: {{props: {{type: sql{catalog}}}}}\n"
            "pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}\n"
            + yaml.safe_dump({"kubernetes": {} if cluster is None else cluster})
        )
        return path

    on_cluster = dict(CLUSTER)
    with pytest.raises(ValueError, match=r"sasl\.password.*\$\{env:NAME\}"):
        model.load_site(written(kafka="sasl.password: hunter2", cluster=on_cluster))
    with pytest.raises(ValueError, match=r"rest\.token.*secret_name"):
        model.load_site(written(catalog=", rest.token: abc123", cluster=on_cluster))
    # A reference is what the cluster path takes, and it stays as it was written.
    referenced = model.load_site(
        written(kafka="sasl.password: '${env:IB_PASSWORD}'", cluster={**on_cluster, "secret_name": "env"})
    )
    assert referenced.kafka_security["sasl.password"] == "${env:IB_PASSWORD}"
    # The same literal, with no cluster to render it into.
    local = model.load_site(written(kafka="sasl.password: hunter2"))
    assert local.kafka_security["sasl.password"] == "hunter2"


def test_a_cluster_site_refuses_a_registry_credential_written_out(tmp_path: Path) -> None:
    """The registry's `user:password` is the same rule, under a key of its own.

    It reaches a Flink source as a format option and a Spark job as nothing at
    all, so where it is written out in full it lands in the ConfigMap that
    carries the rendered script.
    """
    path = tmp_path / "site.yaml"

    def written(user_info: str, cluster: dict[str, object]) -> Path:
        path.write_text(
            "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
            "kafka:\n  bootstrap_servers: 'localhost:9092'\n  schema_registry:\n"
            "    url: http://registry:8080/apis/ccompat/v7\n"
            f"    basic_auth_user_info: '{user_info}'\n"
            "catalog: {props: {type: sql}}\n"
            "pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}\n" + yaml.safe_dump({"kubernetes": cluster})
        )
        return path

    with pytest.raises(ValueError, match=r"basic_auth_user_info.*\$\{env:NAME\}"):
        model.load_site(written("svc:hunter2", dict(CLUSTER)))
    loaded = model.load_site(written("${env:IB_REGISTRY_AUTH}", {**CLUSTER, "secret_name": "env"}))
    assert loaded.schema_registry is not None
    assert loaded.schema_registry.basic_auth_user_info == "${env:IB_REGISTRY_AUTH}"


def _bare_site() -> model.SiteConfig:
    """A site with nothing in it but roots, for deriving a run's names."""
    return model.SiteConfig(
        "s3://b/corpus", "s3://b/runs", "s3://b/wh", "k:9092", {}, None, {"uri": "u"}, None, 0.0, 0.0
    )


def test_derive_ids() -> None:
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = _bare_site()
    d = derive.derive(spec, site, stamp="20260908T120000Z", corpus_dir="smoke-1a2b3c4d")
    assert d.run_id == "smoke-flink-20260908T120000Z" and d.topic == d.run_id
    assert d.table == "ingest_bench.t_smoke_flink_20260908T120000Z"
    assert d.run_root == "s3://b/runs/smoke-flink-20260908T120000Z" and d.corpus_uri == "s3://b/corpus/smoke-1a2b3c4d"


class FakeAdmin:
    """A cluster admin that records what `stage` asked of it instead of reaching a broker."""

    def __init__(self, present: tuple[str, ...] = (), brokers: int = 1) -> None:
        self.present = set(present)
        self.brokers = brokers
        self.created: list[tuple[str, int, int, dict[str, str]]] = []
        self.deleted: list[str] = []
        # The client properties of every call, which is where a run's Kafka
        # credentials would appear.
        self.clients: list[dict[str, str]] = []

    def exists(self, bootstrap: str, name: str, client: dict[str, str]) -> bool:
        self.clients.append(client)
        return name in self.present

    def broker_count(self, bootstrap: str, client: dict[str, str]) -> int:
        self.clients.append(client)
        return self.brokers

    def create(
        self,
        bootstrap: str,
        name: str,
        partitions: int,
        replication_factor: int,
        topic_config: dict[str, str],
        client: dict[str, str],
    ) -> None:
        self.clients.append(client)
        self.created.append((name, partitions, replication_factor, topic_config))
        self.present.add(name)

    def delete(self, bootstrap: str, name: str, client: dict[str, str]) -> None:
        self.clients.append(client)
        self.deleted.append(name)
        self.present.discard(name)


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, str]:
    """A generated smoke corpus, as the root it lives under and its directory name."""
    p = preset.load_preset(
        "smoke",
        workloads_dir=ROOT / "workloads",
        overrides=["offered_bytes_per_s=200KB", "duration_s=1", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("corpus"))
    generate.generate(p, out, seed=1, row_block=64)
    return out, preset.corpus_dir_name(p)


def _site_file(
    tmp_path: Path,
    corpus_root: str,
    security: dict[str, str] | None = None,
    token: str = "shh",
    cluster: dict[str, object] | None = None,
    warehouse: str | None = None,
    registry: dict[str, str] | None = None,
) -> Path:
    path = tmp_path / "site.yaml"
    kafka: dict[str, object] = {"bootstrap_servers": "localhost:9092", "security": security or {}}
    if registry is not None:
        kafka["schema_registry"] = registry
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": corpus_root,
                "runs_root": f"file://{tmp_path}/published",
                # The storage warehouse, which is not the catalog's `warehouse`
                # property: a Glue catalog reads an account id there.
                "warehouse": warehouse or f"file://{tmp_path}/wh",
                "kafka": kafka,
                "catalog": {
                    "props": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path}/catalog.db",
                        "warehouse": f"file://{tmp_path}/wh",
                        "token": token,
                    }
                },
                "kubernetes": cluster or {},
                "pricing": {"vcpu_hour_usd": 0.03, "gib_hour_usd": 0.004},
            }
        )
    )
    return path


REGISTRY = {"url": "http://registry:8080/apis/ccompat/v7"}


@dataclass
class FakeRegistration:
    """What `stage` asked the registry for, instead of asking one."""

    calls: list[tuple[str, str | None, str, str]] = field(default_factory=list)
    schema_id: int = 7
    refuse: bool = False

    def register(self, url: str, basic_auth_user_info: str | None, subject: str, schema_text: str) -> int:
        self.calls.append((url, basic_auth_user_info, subject, schema_text))
        if self.refuse:
            raise ValueError("the registry refused the schema")
        return self.schema_id


def _confluent_spec(tmp_path: Path) -> Path:
    """The shipped external spec, offered in the Confluent wire format."""
    raw = yaml.safe_load((ROOT / "runs" / "smoke-external.yaml").read_text())
    raw["kafka"] = {**raw["kafka"], "value_encoding": "confluent"}
    path = tmp_path / "confluent.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_stage_registers_the_corpus_schema_for_a_confluent_run(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """One registration per run, of the document `schema_avsc_uri` names.

    The id it returns is the run's: the producer puts it in every header and a
    reader resolves the writer schema by it, so `facts.json` has to carry it
    for any engine that never sees the corpus.
    """
    corpus_root, corpus_dir = corpus
    registry = FakeRegistration()
    monkeypatch.setattr(stage, "register_schema", registry.register)
    admin = FakeAdmin()
    staged = stage.stage(
        _confluent_spec(tmp_path),
        _site_file(tmp_path, corpus_root, registry=REGISTRY),
        tmp_path / "runs",
        admin,
        stamp="20260909T100000Z",
    )
    schema_text = uri.read_text(uri.join(corpus_root, corpus_dir, "schema.avsc"))
    assert registry.calls == [(REGISTRY["url"], None, f"{staged.derived.topic}-value", schema_text)]
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["value_encoding"] == "confluent" and facts["schema_id"] == 7
    assert facts["schema_registry_url"] == REGISTRY["url"]
    assert facts["schema_subject"] == f"{staged.derived.topic}-value"
    assert admin.deleted == []


def test_stage_registers_nothing_for_a_raw_avro_run(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default encoding needs no registry, and says so in the facts."""
    corpus_root, _ = corpus
    registry = FakeRegistration()
    monkeypatch.setattr(stage, "register_schema", registry.register)
    staged = stage.stage(
        ROOT / "runs" / "smoke-external.yaml",
        _site_file(tmp_path, corpus_root, registry=REGISTRY),
        tmp_path / "runs",
        FakeAdmin(),
        stamp="20260909T101000Z",
    )
    assert registry.calls == []
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["value_encoding"] == "avro"
    assert facts["schema_registry_url"] is None and facts["schema_subject"] is None and facts["schema_id"] is None


def test_the_facts_state_the_codec_a_consumer_has_to_decode(tmp_path: Path, corpus: tuple[str, str]) -> None:
    """Every run says which codec its values are compressed with, default or not.

    An engine the harness never runs is configured from the facts alone, and a
    consumer that cannot decode the codec reads no records at all — so the
    codec is a fact about the run rather than a detail of the producer.
    """
    corpus_root, _ = corpus
    site = _site_file(tmp_path, corpus_root)

    staged = stage.stage(
        ROOT / "runs" / "smoke-external.yaml", site, tmp_path / "runs", FakeAdmin(), stamp="20260909T102000Z"
    )
    assert json.loads((staged.run_dir / "facts.json").read_text())["compression"] == "zstd"

    raw = yaml.safe_load((ROOT / "runs" / "smoke-external.yaml").read_text())
    raw["producer"] = {"compression": "lz4"}
    spec_path = tmp_path / "lz4.yaml"
    spec_path.write_text(yaml.safe_dump(raw))
    lz4 = stage.stage(spec_path, site, tmp_path / "runs", FakeAdmin(), stamp="20260909T102100Z")
    assert json.loads((lz4.run_dir / "facts.json").read_text())["compression"] == "lz4"


def test_stage_refuses_a_confluent_run_on_a_site_with_no_registry(tmp_path: Path, corpus: tuple[str, str]) -> None:
    """Refused before the topic exists: the run has nowhere to register."""
    corpus_root, _ = corpus
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="kafka.schema_registry"):
        stage.stage(
            _confluent_spec(tmp_path),
            _site_file(tmp_path, corpus_root),
            tmp_path / "runs",
            admin,
            stamp="20260909T102000Z",
        )
    assert admin.created == [] and admin.deleted == []


def test_stage_drops_the_topic_when_the_registration_fails(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A registry that refuses leaves no topic behind, like every other failure."""
    corpus_root, _ = corpus
    monkeypatch.setattr(stage, "register_schema", FakeRegistration(refuse=True).register)
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="the registry refused"):
        stage.stage(
            _confluent_spec(tmp_path),
            _site_file(tmp_path, corpus_root, registry=REGISTRY),
            tmp_path / "runs",
            admin,
            stamp="20260909T103000Z",
        )
    assert admin.deleted == ["smoke-external-20260909T103000Z"]


def test_the_registrys_credential_is_a_reference_until_the_call(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolved at the registration and nowhere else, like every other secret."""
    corpus_root, _ = corpus
    monkeypatch.setenv("IB_TEST_REGISTRY_AUTH", "key:s3cret")
    registry = FakeRegistration()
    monkeypatch.setattr(stage, "register_schema", registry.register)
    site_path = _site_file(
        tmp_path,
        corpus_root,
        registry={**REGISTRY, "basic_auth_user_info": "${env:IB_TEST_REGISTRY_AUTH}"},
    )
    staged = stage.stage(_confluent_spec(tmp_path), site_path, tmp_path / "runs", FakeAdmin(), stamp="20260909T104000Z")
    assert registry.calls[0][1] == "key:s3cret"
    loaded = model.load_site(site_path).schema_registry
    assert loaded is not None and loaded.basic_auth_user_info == "${env:IB_TEST_REGISTRY_AUTH}"
    assert "s3cret" not in (staged.run_dir / "facts.json").read_text()

    monkeypatch.delenv("IB_TEST_REGISTRY_AUTH")
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="IB_TEST_REGISTRY_AUTH"):
        stage.stage(_confluent_spec(tmp_path), site_path, tmp_path / "runs", admin, stamp="20260909T105000Z")
    assert admin.created == [] and admin.deleted == []


def test_resolve_corpus_dir(tmp_path: Path) -> None:
    for directory, name in (("smoke-aaa", "smoke"), ("other-bbb", "other")):
        uri.write_text(uri.join(str(tmp_path), directory, "corpus.json"), json.dumps({"name": name}))
    assert stage.resolve_corpus_dir(str(tmp_path), "smoke") == "smoke-aaa"
    with pytest.raises(ValueError, match="exactly one corpus"):
        stage.resolve_corpus_dir(str(tmp_path), "absent")
    uri.write_text(uri.join(str(tmp_path), "smoke-ccc", "corpus.json"), json.dumps({"name": "smoke"}))
    with pytest.raises(ValueError, match="smoke-aaa"):
        stage.resolve_corpus_dir(str(tmp_path), "smoke")


def test_redact() -> None:
    redacted = stage.redact(
        {
            "uri": "http://catalog:8181",
            "token": "t",
            "s3.secret-access-key": "s",
            "credential": "c",
            "PASSWORD": "p",
        }
    )
    assert redacted == {
        "uri": "http://catalog:8181",
        "token": "<redacted>",
        "s3.secret-access-key": "<redacted>",
        "credential": "<redacted>",
        "PASSWORD": "<redacted>",
    }


def test_harness_table_properties() -> None:
    spec = model.load_run_spec(ROOT / "runs" / "smoke-external.yaml")
    assert stage.harness_table_properties(spec) == {"format-version": "2"}
    codec = model.TableSpec("harness", "unpartitioned", {"write.parquet.compression-codec": "zstd"})
    assert stage.harness_table_properties(replace(spec, table=codec)) == {
        "write.parquet.compression-codec": "zstd",
        "format-version": "2",
    }


def test_stage_writes_the_run_directory(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, corpus_dir = corpus
    spec_path = ROOT / "runs" / "smoke-external.yaml"
    admin = FakeAdmin()
    site_path = _site_file(tmp_path, corpus_root)
    staged = stage.stage(spec_path, site_path, tmp_path / "runs", admin, stamp="20260908T120000Z")

    assert staged.derived.run_id == "smoke-external-20260908T120000Z"
    assert admin.created == [(staged.derived.topic, 4, 1, {"retention.ms": "172800000", "retention.bytes": "-1"})]
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert list(facts)[0] == "run_id" and facts["run_id"] == staged.derived.run_id
    assert facts["topic"] == staged.derived.topic and facts["bootstrap"] == "localhost:9092"
    assert facts["table"] == staged.derived.table == "ingest_bench.t_smoke_external_20260908T120000Z"
    assert facts["corpus_uri"] == uri.join(corpus_root, corpus_dir)
    assert facts["schema_avsc_uri"] == uri.join(corpus_root, corpus_dir, "schema.avsc")
    assert facts["key_column"] == "user_id" and facts["partition"] == "identity(partition_key)"
    assert facts["catalog_props"]["token"] == "<redacted>" and facts["catalog_props"]["type"] == "sql"
    assert facts["ddl"] is None and facts["epoch"] is None
    assert (staged.run_dir / "spec.yaml").read_text() == spec_path.read_text()
    assert (staged.run_dir / "timeline.log").read_text().splitlines()[0].endswith(" staged")
    assert stage.facts_lines(staged.facts)[0] == f"run_id: {staged.derived.run_id}"
    table = cat.open_catalog(model.load_site(site_path).catalog_props).load_table(str(facts["table"]))
    assert [field.name for field in table.schema().fields] == metadata.read(str(facts["corpus_uri"])).field_names()
    assert table.spec().fields[0].name == "partition_key"


def test_stage_gives_the_table_and_its_namespace_a_location_under_the_warehouse(
    tmp_path: Path, corpus: tuple[str, str]
) -> None:
    """The location comes from `site.warehouse`, never from the catalog's own property.

    A Glue Iceberg REST catalog reads an account id in `warehouse`, so a table
    created without a location lands nowhere a bucket can hold — and the
    failure surfaces from inside the first writer rather than from the create.
    """
    corpus_root, _ = corpus
    warehouse = f"file://{tmp_path}/explicit"
    site_path = _site_file(tmp_path, corpus_root, warehouse=warehouse)
    staged = stage.stage(
        ROOT / "runs" / "smoke-external.yaml", site_path, tmp_path / "runs", FakeAdmin(), stamp="20260908T190000Z"
    )
    namespace, name = cat.table_identifier(staged.derived.table)
    catalog = cat.open_catalog(model.load_site(site_path).catalog_props)
    assert catalog.load_table(staged.derived.table).location() == uri.join(warehouse, namespace, name)
    assert catalog.load_namespace_properties(namespace)["location"] == uri.join(warehouse, namespace)


def test_stage_refuses_a_cluster_run_with_no_image_tag(tmp_path: Path, corpus: tuple[str, str]) -> None:
    """The tag is refused before the topic exists, not at the render that needs it.

    Staging a managed run on a cluster ends in two documents naming an image,
    and there is no image to name without the tag that was pushed.
    """
    corpus_root, _ = corpus
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="image-tag"):
        stage.stage(
            ROOT / "runs" / "smoke-flink.yaml",
            # A reference and not the fixture's literal: a site declaring a
            # cluster renders its properties into a ConfigMap, and a written-out
            # credential is refused there before anything else is read.
            _site_file(tmp_path, corpus_root, cluster=dict(CLUSTER), token="${env:IB_TEST_CATALOG_TOKEN}"),
            tmp_path / "runs",
            admin,
            stamp="20260908T191000Z",
        )
    assert admin.created == [] and admin.deleted == []


def _rest_cluster_site(tmp_path: Path, corpus_root: str) -> Path:
    """A site on a cluster with a REST catalog, which is what a Flink run reads through."""
    path = tmp_path / "site-cluster.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": corpus_root,
                "runs_root": f"file://{tmp_path}/published",
                "warehouse": f"file://{tmp_path}/wh",
                "kafka": {"bootstrap_servers": "localhost:9092"},
                "catalog": {"props": {"type": "rest", "uri": "http://catalog:8181", "warehouse": "123456789012"}},
                "kubernetes": dict(CLUSTER),
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    return path


def _engine_owned_flink_spec(tmp_path: Path) -> Path:
    """The shipped Flink spec with the table left to the engine.

    Staging it reaches the renderers without a catalog to create a table in,
    which is what this file can exercise without a REST catalog running.
    """
    spec = yaml.safe_load((ROOT / "runs" / "smoke-flink.yaml").read_text())
    spec["table"] = {**spec["table"], "managed_by": "engine"}
    path = tmp_path / "flink-cluster.yaml"
    path.write_text(yaml.safe_dump(spec))
    return path


def test_stage_on_a_cluster_renders_its_documents_and_uploads_the_run_directory(
    tmp_path: Path, corpus: tuple[str, str]
) -> None:
    """Both Kubernetes documents are written, and every file is published.

    The upload is what lets staging run as a Job: the pod that wrote the run
    directory is gone by the time an operator wants it, so the directory has
    to outlive the pod somewhere the operator can read.
    """
    corpus_root, _ = corpus
    uploads = f"file://{tmp_path}/uploads"
    staged = stage.stage(
        _engine_owned_flink_spec(tmp_path),
        _rest_cluster_site(tmp_path, corpus_root),
        tmp_path / "runs",
        FakeAdmin(),
        stamp="20260908T192000Z",
        image_tag="abc1234",
        upload_prefix=uploads,
    )
    written = sorted(path.name for path in staged.run_dir.iterdir())
    assert written == [
        "facts.json",
        "flink-conf.yaml",
        "flink-job-configmap.yaml",
        "flink.env",
        "flinkdeployment.yaml",
        "job.sql",
        "spec.yaml",
        "timeline.log",
    ]
    published = uri.join(uploads, staged.derived.run_id, "stage")
    assert sorted(uri.listdir(published)) == written
    assert uri.read_text(uri.join(published, "facts.json")) == (staged.run_dir / "facts.json").read_text()
    assert ":abc1234" in (staged.run_dir / "flinkdeployment.yaml").read_text()


def test_replication_factor_follows_the_broker_count(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    site_path = _site_file(tmp_path, corpus_root)
    for index, (brokers, expected) in enumerate(((1, 1), (2, 2), (5, 3))):
        admin = FakeAdmin(brokers=brokers)
        staged = stage.stage(
            ROOT / "runs" / "smoke-external.yaml",
            site_path,
            tmp_path / "runs",
            admin,
            stamp=f"20260908T1600{index:02d}Z",
        )
        assert admin.created == [(staged.derived.topic, 4, expected, dict(kafka_admin.DEFAULT_TOPIC_CONFIG))]


def test_stage_drops_the_topic_when_staging_fails(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    spec_path = ROOT / "runs" / "smoke-external.yaml"
    site_path = _site_file(tmp_path, corpus_root)
    stage.stage(spec_path, site_path, tmp_path / "runs", FakeAdmin(), stamp="20260908T130000Z")
    # A fresh admin reports no topic, so staging reaches the table it already created.
    retry = FakeAdmin()
    with pytest.raises(Exception, match="t_smoke_external_20260908T130000Z"):
        stage.stage(spec_path, site_path, tmp_path / "runs", retry, stamp="20260908T130000Z")
    assert retry.deleted == ["smoke-external-20260908T130000Z"]


def test_stage_refuses_an_existing_topic(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    admin = FakeAdmin(present=("smoke-external-20260908T140000Z",))
    with pytest.raises(ValueError, match="already exists"):
        stage.stage(
            ROOT / "runs" / "smoke-external.yaml",
            _site_file(tmp_path, corpus_root),
            tmp_path / "runs",
            admin,
            stamp="20260908T140000Z",
        )
    assert admin.created == [] and admin.deleted == []


def test_a_secret_is_named_in_the_facts_and_resolved_at_the_cluster(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus_root, _ = corpus
    monkeypatch.setenv("IB_TEST_KAFKA_PASSWORD", "s3cret")
    monkeypatch.setenv("IB_TEST_CATALOG_TOKEN", "t0ken")
    site_path = _site_file(
        tmp_path,
        corpus_root,
        security={"security.protocol": "SASL_SSL", "sasl.password": "${env:IB_TEST_KAFKA_PASSWORD}"},
        token="${env:IB_TEST_CATALOG_TOKEN}",
    )
    admin = FakeAdmin()
    staged = stage.stage(
        ROOT / "runs" / "smoke-external.yaml", site_path, tmp_path / "runs", admin, stamp="20260908T170000Z"
    )
    # The site config keeps the reference, and so does every file staging wrote:
    # a reference is publishable, and it says which variable a reader must set.
    assert model.load_site(site_path).kafka_security["sasl.password"] == "${env:IB_TEST_KAFKA_PASSWORD}"
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["catalog_props"]["token"] == "${env:IB_TEST_CATALOG_TOKEN}"
    # The value exists only in the calls that need it.
    assert admin.clients and all(
        client == {"security.protocol": "SASL_SSL", "sasl.password": "s3cret"} for client in admin.clients
    )


def test_a_site_spelling_the_mechanism_in_the_plural_is_refused(tmp_path: Path, corpus: tuple[str, str]) -> None:
    """The site is where the spelling is written, so it is where it is refused.

    librdkafka takes `sasl.mechanisms` as readily as `sasl.mechanism`, and
    every reader in this repository — the token callback and the Java-client
    properties the engines render — reads the singular. A site carrying the
    plural would connect and be given none of the MSK IAM translation, so it
    is refused at load rather than at the first connection that needed it.
    """
    corpus_root, _ = corpus
    site_path = _site_file(
        tmp_path,
        corpus_root,
        security={"security.protocol": "SASL_SSL", "sasl.mechanisms": "OAUTHBEARER", "aws.region": "eu-west-1"},
    )
    with pytest.raises(ValueError, match="'sasl.mechanism'"):
        model.load_site(site_path)


def test_a_literal_credential_is_still_redacted(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    staged = stage.stage(
        ROOT / "runs" / "smoke-external.yaml",
        _site_file(tmp_path, corpus_root),
        tmp_path / "runs",
        FakeAdmin(),
        stamp="20260908T171000Z",
    )
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["catalog_props"]["token"] == "<redacted>"


def test_stage_refuses_an_unset_reference_before_it_creates_anything(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus_root, _ = corpus
    monkeypatch.delenv("IB_TEST_ABSENT", raising=False)
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="IB_TEST_ABSENT"):
        stage.stage(
            ROOT / "runs" / "smoke-external.yaml",
            _site_file(tmp_path, corpus_root, security={"sasl.password": "${env:IB_TEST_ABSENT}"}),
            tmp_path / "runs",
            admin,
            stamp="20260908T180000Z",
        )
    assert admin.created == [] and admin.deleted == []


def test_knobs_for_refuses_an_unregistered_engine() -> None:
    with pytest.raises(ValueError, match="unicorn"):
        engines.knobs_for("unicorn")
    assert engines.MANAGED["flink"] == "engines.flink.knobs"


def test_main_prints_the_run_id_first(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    corpus_root, _ = corpus
    monkeypatch.setattr(stage, "ClusterAdmin", FakeAdmin)
    code = stage.main(
        [
            "--spec",
            str(ROOT / "runs" / "smoke-external.yaml"),
            "--site",
            str(_site_file(tmp_path, corpus_root)),
            "--runs-dir",
            str(tmp_path / "runs"),
            "--stamp",
            "20260908T150000Z",
        ]
    )
    lines = capsys.readouterr().out.splitlines()
    assert code == 0 and lines[0] == "run_id: smoke-external-20260908T150000Z"
    assert lines[-1] == "Start your engine now; run launch when it is consuming."
    assert "catalog_props: {" in "\n".join(lines) and "epoch: null" in lines
