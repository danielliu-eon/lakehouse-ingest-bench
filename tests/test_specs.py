import json
from collections.abc import Callable
from dataclasses import replace
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
        service_account_annotations={"example.com/role": "arn"},
        registry="registry.example/ingest-bench",
        aws_region="eu-west-1",
        node_selector={"kubernetes.io/arch": "amd64"},
        tolerations=[{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
    )


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


def test_derive_ids() -> None:
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = model.SiteConfig("s3://b/corpus", "s3://b/runs", "s3://b/wh", "k:9092", {}, {"uri": "u"}, None, 0.0, 0.0)
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
) -> Path:
    path = tmp_path / "site.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": corpus_root,
                "runs_root": f"file://{tmp_path}/published",
                # The storage warehouse, which is not the catalog's `warehouse`
                # property: a Glue catalog reads an account id there.
                "warehouse": warehouse or f"file://{tmp_path}/wh",
                "kafka": {"bootstrap_servers": "localhost:9092", "security": security or {}},
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
            _site_file(tmp_path, corpus_root, cluster=dict(CLUSTER)),
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
