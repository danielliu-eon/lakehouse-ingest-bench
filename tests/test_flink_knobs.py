from dataclasses import replace
from pathlib import Path

import pytest

from engines.flink import job, knobs
from ingest_bench import uri
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.specs import derive, model

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def meta(tmp_path_factory: pytest.TempPathFactory) -> metadata.CorpusMetadata:
    p = preset.load_preset(
        "smoke",
        workloads_dir=ROOT / "workloads",
        overrides=["offered_bytes_per_s=200KB", "duration_s=1", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("c"))
    generate.generate(p, out, seed=1, row_block=64)
    return metadata.read(uri.join(out, preset.corpus_dir_name(p)))


def _site() -> model.SiteConfig:
    props = {
        "uri": "http://iceberg-rest:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }
    return model.SiteConfig("s3://corpus", "s3://runs", "s3://warehouse", "kafka:9092", {}, props, {}, 0.0, 0.0)


def test_validate(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    knobs.validate(spec.engine_block, spec, meta)
    with pytest.raises(ValueError, match="distribution_mode"):
        knobs.validate({**spec.engine_block, "distribution_mode": "sorted"}, spec, meta)
    with pytest.raises(ValueError, match="source_parallelism"):
        knobs.validate({**spec.engine_block, "source_parallelism": 99}, spec, meta)
    with pytest.raises(ValueError, match="unknown"):
        knobs.validate({**spec.engine_block, "tm_gpu": 1}, spec, meta)


def test_render_sql_and_conf(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    d = derive.derive(spec, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, _site(), d, meta)
    assert "id BIGINT NOT NULL" in sql and "event_time TIMESTAMP(6) NOT NULL" in sql and "payload BYTES NOT NULL" in sql
    assert (
        "'topic' = 'smoke-flink-20260908T000000Z'" in sql
        and "'format' = 'avro'" in sql
        and "'scan.startup.mode' = 'earliest-offset'" in sql
    )
    # Without this the legacy mapping caps SQL TIMESTAMP at milliseconds and
    # the TIMESTAMP(6) column above cannot be planned at all.
    assert "'avro.timestamp_mapping.legacy' = 'false'" in sql
    assert "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'" in sql and "'client.region' = 'us-east-1'" in sql
    assert "/*+ OPTIONS('distribution-mode' = 'hash') */" in sql
    assert sql.count("NOT NULL") == len(meta.field_names())
    conf = knobs.render_conf(spec, d)
    assert (
        conf["execution.checkpointing.interval"] == "10s"
        and conf["parallelism.default"] == "4"
        and conf["pipeline.max-parallelism"] == "16"
    )
    files = knobs.render(spec, _site(), d, meta)
    assert set(files) == {"job.sql", "flink-conf.yaml", "flink.env"}


def test_ddl_type_mapping() -> None:
    assert knobs.flink_ddl_type("timestamp") == "TIMESTAMP(6)" and knobs.flink_ddl_type("binary") == "BYTES"
    with pytest.raises(ValueError):
        knobs.flink_ddl_type("decimal")


def test_writers_spread_wider_than_readers(meta: metadata.CorpusMetadata) -> None:
    """A fleet larger than the reader count states the writer parallelism itself.

    The pinned Kafka connector takes no source-parallelism option, so the job
    default holds the readers down and only a sink hint lifts the writers back
    to the fleet — the one case where the two numbers disagree.
    """
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    override = {"execution.checkpointing.min-pause": "9s"}
    wider = replace(spec, engine_block={**spec.engine_block, "taskmanagers": 2, "extra_flink_conf": override})
    knobs.validate(wider.engine_block, wider, meta)
    d = derive.derive(wider, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(wider, _site(), d, meta)
    assert "/*+ OPTIONS('distribution-mode' = 'hash', 'write-parallelism' = '8') */" in sql
    conf = knobs.render_conf(wider, d)
    assert conf["parallelism.default"] == "4" and conf["pipeline.max-parallelism"] == "32"
    # extra_flink_conf is applied last, so it overrides a setting named above.
    assert conf["execution.checkpointing.min-pause"] == "9s"


def test_render_refuses_a_catalog_flink_cannot_read(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = _site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql_catalog = replace(site, catalog_props={**site.catalog_props, "type": "sql"})
    with pytest.raises(ValueError, match="REST catalog"):
        knobs.render_sql(spec, sql_catalog, d, meta)
    no_uri = replace(site, catalog_props={"warehouse": "gs://warehouse"})
    with pytest.raises(ValueError, match="uri"):
        knobs.render_sql(spec, no_uri, d, meta)
    gcs = replace(site, catalog_props={"uri": "http://c:8181", "warehouse": "gs://warehouse"})
    assert "'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO'" in knobs.render_sql(spec, gcs, d, meta)


def test_quotes_in_a_value_stay_inside_their_literal(meta: metadata.CorpusMetadata) -> None:
    """A credential holding a quote must not end the literal that carries it."""
    site = replace(_site(), kafka_security={"sasl.password": "pa's's"})
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    assert "'properties.sasl.password' = 'pa''s''s'" in knobs.render_sql(spec, site, d, meta)


def test_the_submitter_splits_what_the_renderer_joined(meta: metadata.CorpusMetadata) -> None:
    """The two halves of the script contract, checked against each other.

    The renderer runs in the harness and the submitter runs in the Flink
    image, so nothing at run time would report a disagreement about where one
    statement ends and the next begins.
    """
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    d = derive.derive(spec, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    statements = job.split_statements(knobs.render_sql(spec, _site(), d, meta))
    assert len(statements) == 3
    assert all(statement and not statement.endswith(";") for statement in statements)
    assert statements[0].startswith("CREATE TABLE kafka_source")
    assert statements[1].startswith("CREATE CATALOG ice")
    assert statements[2].startswith("INSERT INTO ice.")
    # A `;` inside a property value must not be read as a statement end.
    with_semicolons = replace(_site(), kafka_security={"sasl.jaas.config": "a=b;c=d;"})
    assert len(job.split_statements(knobs.render_sql(spec, with_semicolons, d, meta))) == 3


def test_the_external_example_is_what_the_renderer_produces(meta: metadata.CorpusMetadata) -> None:
    """The walk-through's checked-in engine config has to describe these rows.

    `docs/examples/external-flink/` is the config an operator starts by hand in
    the external walk-through, so it is this renderer's output for the local
    stack with the run's names left as placeholders. A column added to the
    schema would otherwise leave it declaring a source that no longer matches
    the bytes the producer writes, and the walk-through would fail as an Avro
    decode error rather than as a document nobody updated.
    """
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = model.load_site(ROOT / "deploy" / "compose" / "local" / "site.yaml")
    placeholders = derive.Derived(
        run_id="@RUN_ID@",
        topic="@TOPIC@",
        table="@NAMESPACE@.@TABLE@",
        namespace="@K8S_NAMESPACE@",
        run_root="@RUN_ROOT@",
        corpus_uri="@CORPUS_URI@",
    )
    example = ROOT / "docs" / "examples" / "external-flink"
    for name, content in knobs.render(spec, site, placeholders, meta).items():
        assert (example / name).read_text() == content, f"{example / name} is stale; re-render it"
