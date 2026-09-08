import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from ingest_bench import catalog as cat
from ingest_bench import stage, uri
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.specs import derive, engines, model

ROOT = Path(__file__).resolve().parents[1]


def test_smoke_specs_load() -> None:
    flink = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    assert flink.engine == "flink" and flink.table.managed_by == "harness" and flink.producer.speed == 1.0
    assert flink.scoring.warmup_s == 60 and flink.scoring.geometry_offsets_s == (600, 1200, 1800, 2700, 3600)
    assert flink.engine_block["taskmanagers"] == 1
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


def test_derive_ids() -> None:
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = model.SiteConfig("s3://b/corpus", "s3://b/runs", "s3://b/wh", "k:9092", {}, {"uri": "u"}, {}, 0.0, 0.0)
    d = derive.derive(spec, site, stamp="20260908T120000Z", corpus_dir="smoke-1a2b3c4d")
    assert d.run_id == "smoke-flink-20260908T120000Z" and d.topic == d.run_id
    assert d.table == "ingest_bench.t_smoke_flink_20260908T120000Z"
    assert d.run_root == "s3://b/runs/smoke-flink-20260908T120000Z" and d.corpus_uri == "s3://b/corpus/smoke-1a2b3c4d"


class FakeAdmin:
    """A topic admin that records what `stage` asked of it instead of reaching a broker."""

    def __init__(self, present: tuple[str, ...] = ()) -> None:
        self.present = set(present)
        self.created: list[tuple[str, int, int, dict[str, str]]] = []
        self.deleted: list[str] = []

    def exists(self, bootstrap: str, name: str) -> bool:
        return name in self.present

    def create(
        self, bootstrap: str, name: str, partitions: int, replication_factor: int, config: dict[str, str]
    ) -> None:
        self.created.append((name, partitions, replication_factor, config))
        self.present.add(name)

    def delete(self, bootstrap: str, name: str) -> None:
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


def _site_file(tmp_path: Path, corpus_root: str) -> Path:
    path = tmp_path / "site.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": corpus_root,
                "runs_root": f"file://{tmp_path}/published",
                "warehouse": f"file://{tmp_path}/wh",
                "kafka": {"bootstrap_servers": "localhost:9092", "security": {}},
                "catalog": {
                    "props": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path}/catalog.db",
                        "warehouse": f"file://{tmp_path}/wh",
                        "token": "shh",
                    }
                },
                "kubernetes": {},
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
