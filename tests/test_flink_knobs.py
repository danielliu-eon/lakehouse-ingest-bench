from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from engines.flink import job, knobs, script
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
    return model.SiteConfig("s3://corpus", "s3://runs", "s3://warehouse", "kafka:9092", {}, None, props, None, 0.0, 0.0)


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
    # Two taskmanagers of four slots against four readers, so the sink has to
    # state the writer parallelism rather than inherit the readers'.
    assert "/*+ OPTIONS('distribution-mode' = 'hash', 'write-parallelism' = '8') */" in sql
    assert sql.count("NOT NULL") == len(meta.field_names())
    conf = knobs.render_conf(spec, d)
    assert (
        conf["execution.checkpointing.interval"] == "10s"
        and conf["parallelism.default"] == "4"
        and conf["pipeline.max-parallelism"] == "32"
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
    to the fleet — the one case where the two numbers disagree. Both sides of
    that branch are checked here, because a hint emitted unconditionally would
    be indistinguishable from a working one on the shipped spec.
    """
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    override = {"execution.checkpointing.min-pause": "9s"}
    wider = replace(spec, engine_block={**spec.engine_block, "taskmanagers": 4, "extra_flink_conf": override})
    knobs.validate(wider.engine_block, wider, meta)
    d = derive.derive(wider, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(wider, _site(), d, meta)
    assert "/*+ OPTIONS('distribution-mode' = 'hash', 'write-parallelism' = '16') */" in sql
    conf = knobs.render_conf(wider, d)
    assert conf["parallelism.default"] == "4" and conf["pipeline.max-parallelism"] == "64"
    # extra_flink_conf is applied last, so it overrides a setting named above.
    assert conf["execution.checkpointing.min-pause"] == "9s"

    # One taskmanager gives four slots against four readers, so the numbers
    # coincide and there is nothing for the hint to say.
    level = replace(spec, engine_block={**spec.engine_block, "taskmanagers": 1})
    knobs.validate(level.engine_block, level, meta)
    assert "/*+ OPTIONS('distribution-mode' = 'hash') */" in knobs.render_sql(level, _site(), d, meta)


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
    # The FileIO follows the site's warehouse, so a GCS site declares one.
    gcs = replace(
        site, warehouse="gs://warehouse", catalog_props={"uri": "http://c:8181", "warehouse": "gs://warehouse"}
    )
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
        run_root="@RUN_ROOT@",
        corpus_uri="@CORPUS_URI@",
    )
    example = ROOT / "docs" / "examples" / "external-flink"
    for name, content in knobs.render(spec, site, placeholders, meta).items():
        assert (example / name).read_text() == content, f"{example / name} is stale; re-render it"


def test_a_secret_is_named_in_the_rendered_files_and_read_in_the_container(
    meta: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    spec = replace(
        spec, engine_block={**spec.engine_block, "extra_flink_conf": {"custom.secret": "${env:IB_TEST_FLINK_SECRET}"}}
    )
    site = replace(
        _site(),
        kafka_security={"security.protocol": "SASL_SSL", "sasl.password": "${env:IB_TEST_FLINK_SECRET}"},
    )
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    rendered = knobs.render(spec, site, d, meta)

    # The renderer runs in the harness and writes what a reader may keep: the
    # variable's name, never its value.
    assert "'properties.sasl.password' = '${env:IB_TEST_FLINK_SECRET}'" in rendered[knobs.SQL_FILE]
    assert "custom.secret: ${env:IB_TEST_FLINK_SECRET}" in rendered[knobs.CONF_FILE]

    conf_path = tmp_path / knobs.CONF_FILE
    conf_path.write_text(rendered[knobs.CONF_FILE])
    monkeypatch.setenv("IB_TEST_FLINK_SECRET", "s3cret")
    assert "'properties.sasl.password' = 's3cret'" in script.substitute_env(rendered[knobs.SQL_FILE])
    assert job.read_conf(conf_path)["custom.secret"] == "${env:IB_TEST_FLINK_SECRET}"
    assert script.substitute_env(job.read_conf(conf_path)["custom.secret"]) == "s3cret"

    monkeypatch.delenv("IB_TEST_FLINK_SECRET")
    with pytest.raises(ValueError, match=r"\$\{env:IB_TEST_FLINK_SECRET\} is not set in the environment"):
        script.substitute_env(rendered[knobs.SQL_FILE])


# The AWS shape: an MSK broker reached by IAM, a Glue REST catalog whose
# warehouse is an account id rather than a URI, and a cluster to submit to.
_MSK_SECURITY = {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER", "aws.region": "eu-west-1"}


def _cluster() -> model.KubernetesConfig:
    return model.KubernetesConfig(
        context="bench",
        namespace="ingest-bench",
        harness_service_account="ingest-bench-harness",
        flink_service_account="ingest-bench-flink",
        service_account_annotations={},
        registry="registry.example/ingest-bench",
        aws_region="eu-west-1",
        node_selector={"bench-pool": "engine"},
        tolerations=[{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
    )


def _aws_site() -> model.SiteConfig:
    props = {
        "type": "rest",
        "uri": "https://glue.eu-west-1.amazonaws.com/iceberg",
        "warehouse": "123456789012",
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "glue",
        "rest.signing-region": "eu-west-1",
    }
    return model.SiteConfig(
        "s3://bench-bucket/corpus",
        "s3://bench-bucket/runs",
        "s3://bench-bucket/warehouse",
        "b-1.example:9098",
        dict(_MSK_SECURITY),
        None,
        props,
        _cluster(),
        0.0,
        0.0,
    )


def test_msk_iam_replaces_the_signal_a_file_can_carry(meta: metadata.CorpusMetadata) -> None:
    """The Java client's IAM properties, from the pseudo-key librdkafka reads.

    The harness signals MSK IAM with `sasl.mechanism: OAUTHBEARER` plus its own
    `aws.region`, which is what librdkafka needs. Flink's client is the Java
    one, where the same authentication is a differently named mechanism and a
    login module — so the signal is translated rather than passed through.
    """
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = replace(_aws_site(), kafka_security={**_MSK_SECURITY, "ssl.endpoint.identification.algorithm": "https"})
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, site, d, meta)
    assert "'properties.security.protocol' = 'SASL_SSL'" in sql
    assert "'properties.sasl.mechanism' = 'AWS_MSK_IAM'" in sql
    assert "'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;'" in sql
    assert (
        "'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'"
        in sql
    )
    # The pseudo-key is the harness's own and means nothing to any client; the
    # region reaches the pod as AWS_REGION instead.
    assert "aws.region" not in sql
    # OAUTHBEARER is what the signal said, not what the Java client is told.
    assert "OAUTHBEARER" not in sql
    # A key that is not part of the signal still reaches the client.
    assert "'properties.ssl.endpoint.identification.algorithm' = 'https'" in sql
    # The login module's own `;` must not end the statement that carries it.
    assert len(job.split_statements(sql)) == 3
    # A key the translation answers for is not carried from the site as well:
    # a WITH clause cannot hold the same option twice.
    doubled = replace(_aws_site(), kafka_security={**_MSK_SECURITY, "sasl.jaas.config": "handmade;"})
    assert knobs.render_sql(spec, doubled, d, meta).count("'properties.sasl.jaas.config'") == 1


def test_a_site_that_is_not_on_msk_keeps_its_properties(meta: metadata.CorpusMetadata) -> None:
    """Half the signal is not the signal, so nothing is translated."""
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    d = derive.derive(spec, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    for security in (
        {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER"},
        {"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512", "aws.region": "eu-west-1"},
    ):
        sql = knobs.render_sql(spec, replace(_site(), kafka_security=security), d, meta)
        assert "AWS_MSK_IAM" not in sql
        for key, value in security.items():
            assert f"'properties.{key}' = '{value}'" in sql


def test_a_glue_catalog_reaches_storage_through_the_sites_warehouse(meta: metadata.CorpusMetadata) -> None:
    """A catalog whose warehouse is an account id still names a FileIO.

    Glue's REST endpoint takes the account as its warehouse, so the storage
    implementation cannot be read off that property — the site's own warehouse
    URI is what carries the scheme.
    """
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, site, d, meta)
    assert "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'" in sql
    assert "'warehouse' = '123456789012'" in sql
    for key in ("rest.sigv4-enabled", "rest.signing-name", "rest.signing-region"):
        assert f"'{key}' = '{site.catalog_props[key]}'" in sql
    # A site whose storage is on neither scheme names no implementation, and
    # the catalog's own warehouse cannot make it look as though it did.
    nowhere = replace(_aws_site(), warehouse="/mnt/warehouse")
    assert "io-impl" not in knobs.render_sql(spec, nowhere, d, meta)


def test_render_flinkdeployment(meta: metadata.CorpusMetadata) -> None:
    """The whole document the operator is handed, parsed rather than matched."""
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="0.1.0-abc1234"))
    assert document == {
        "apiVersion": "flink.apache.org/v1beta1",
        "kind": "FlinkDeployment",
        "metadata": {"name": "smoke-flink-20260908t000000z", "namespace": "ingest-bench"},
        "spec": {
            "image": "registry.example/ingest-bench/lakehouse-ingest-bench/flink:0.1.0-abc1234",
            "flinkVersion": "v1_20",
            "mode": "standalone",
            "serviceAccount": "ingest-bench-flink",
            "flinkConfiguration": {
                **knobs.render_conf(spec, d),
                "state.checkpoints.dir": "s3://bench-bucket/runs/smoke-flink-20260908T000000Z/checkpoints",
            },
            "jobManager": {"resource": {"memory": "1024m", "cpu": 1.0}},
            "taskManager": {"resource": {"memory": "2048m", "cpu": 2.0}, "replicas": 2},
            "job": {
                "jarURI": "local:///opt/flink/opt/flink-python-1.20.1.jar",
                "entryClass": "org.apache.flink.client.python.PythonDriver",
                "args": [
                    "-py",
                    "/opt/bench/engines/flink/job.py",
                    "--sql",
                    "/opt/bench/run/job.sql",
                    "--conf",
                    "/opt/bench/run/flink-conf.yaml",
                ],
                "parallelism": 4,
                "upgradeMode": "stateless",
                "state": "running",
            },
            "podTemplate": {
                "apiVersion": "v1",
                "kind": "Pod",
                "spec": {
                    "nodeSelector": {"bench-pool": "engine", "kubernetes.io/arch": "amd64"},
                    "tolerations": [{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
                    "volumes": [
                        {
                            "name": "job",
                            "configMap": {"name": "smoke-flink-20260908t000000z-flink-job"},
                        }
                    ],
                    "containers": [
                        {
                            "name": "flink-main-container",
                            "volumeMounts": [{"name": "job", "mountPath": "/opt/bench/run", "readOnly": True}],
                            "env": [
                                {"name": "AWS_REGION", "value": "eu-west-1"},
                                {"name": "AWS_DEFAULT_REGION", "value": "eu-west-1"},
                            ],
                        }
                    ],
                },
            },
        },
    }


def test_kubernetes_name_lowercases_a_run_id() -> None:
    assert knobs.kubernetes_name("smoke-flink-20260908T000000Z") == "smoke-flink-20260908t000000z"
    assert knobs.kubernetes_name("already-lower-1") == "already-lower-1"


def test_only_the_object_names_are_lowercased(meta: metadata.CorpusMetadata) -> None:
    """A run id reaches the two documents as itself everywhere it is not a name.

    An RFC 1123 name is lowercase and a run id's stamp is not, so the objects
    are named by the lowercased id. The settings carrying the id are not names
    Kubernetes reads, and lowercasing one of them would point a run's
    checkpoints at a prefix no other reader of the run addresses.
    """
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))
    conf = document["spec"]["flinkConfiguration"]
    assert conf["pipeline.name"] == d.run_id
    assert conf["state.checkpoints.dir"].endswith(f"/{d.run_id}/checkpoints")
    assert f"'topic' = '{d.run_id}'" in knobs.render_sql(spec, site, d, meta)


def test_the_amd64_pin_wins_and_a_cluster_off_aws_names_no_region(meta: metadata.CorpusMetadata) -> None:
    """PyFlink has no aarch64 wheel, so the pin is not a site's to override.

    The region is the other half: an SDK reads it when nothing else names one,
    and a cluster on another cloud has none to name — so a pod there carries
    neither of the two names it would otherwise be rendered under.
    """
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    cluster = replace(_cluster(), aws_region=None, node_selector={"kubernetes.io/arch": "arm64"}, tolerations=[])
    site = replace(_aws_site(), kubernetes=cluster)
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    pod = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))["spec"]["podTemplate"]
    assert pod["spec"]["nodeSelector"] == {"kubernetes.io/arch": "amd64"}
    assert pod["spec"]["tolerations"] == []
    assert "env" not in pod["spec"]["containers"][0]


def test_a_run_can_still_redirect_its_checkpoints(meta: metadata.CorpusMetadata) -> None:
    """`extra_flink_conf` is applied last, and the derived path is a default."""
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    elsewhere = {"state.checkpoints.dir": "s3://bench-bucket/checkpoints"}
    spec = replace(spec, engine_block={**spec.engine_block, "extra_flink_conf": elsewhere})
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))
    assert document["spec"]["flinkConfiguration"]["state.checkpoints.dir"] == "s3://bench-bucket/checkpoints"


def test_the_crd_fields_follow_the_effective_conf(meta: metadata.CorpusMetadata) -> None:
    """A setting the operator restates as a CRD field is read back out of the conf.

    The operator applies `job.parallelism` and the two `resource.memory`
    fields over `spec.flinkConfiguration`, so a run whose `extra_flink_conf`
    moved one of them would be honoured by the job it submitted and overruled
    by the cluster running it.
    """
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    override = {
        "parallelism.default": "3",
        "jobmanager.memory.process.size": "3072m",
        "taskmanager.memory.process.size": "6144m",
    }
    spec = replace(spec, engine_block={**spec.engine_block, "extra_flink_conf": override})
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    deployment = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))["spec"]
    assert deployment["job"]["parallelism"] == 3
    assert deployment["jobManager"]["resource"] == {"memory": "3072m", "cpu": 1.0}
    assert deployment["taskManager"]["resource"] == {"memory": "6144m", "cpu": 2.0}
    # The operator restates neither of these, so they stay the knobs' to set.
    assert deployment["taskManager"]["replicas"] == 2
    assert deployment["flinkConfiguration"]["taskmanager.numberOfTaskSlots"] == "4"


def test_render_job_configmap(meta: metadata.CorpusMetadata) -> None:
    """The two rendered files, as the pod reads them off a mount."""
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_job_configmap(spec, site, d, meta))
    assert document["apiVersion"] == "v1" and document["kind"] == "ConfigMap"
    assert document["metadata"] == {"name": "smoke-flink-20260908t000000z-flink-job", "namespace": "ingest-bench"}
    files = knobs.render(spec, site, d, meta, image_tag="t")
    assert document["data"] == {knobs.SQL_FILE: files[knobs.SQL_FILE], knobs.CONF_FILE: files[knobs.CONF_FILE]}


def test_a_cluster_run_is_two_more_files_and_needs_an_image(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "runs" / "smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    assert set(knobs.render(spec, site, d, meta, image_tag="t")) == {
        knobs.SQL_FILE,
        knobs.CONF_FILE,
        knobs.ENV_FILE,
        knobs.FLINKDEPLOYMENT_FILE,
        knobs.CONFIGMAP_FILE,
    }
    # The tag names the image a run is submitted as, so a cluster run without
    # one has no engine to start.
    with pytest.raises(ValueError, match="image_tag"):
        knobs.render(spec, site, d, meta)
    # No cluster, no Kubernetes documents — and nothing to refuse either.
    local = replace(_aws_site(), kubernetes=None)
    assert set(knobs.render(spec, local, d, meta)) == {knobs.SQL_FILE, knobs.CONF_FILE, knobs.ENV_FILE}
    with pytest.raises(ValueError, match="site.kubernetes"):
        knobs.render_flinkdeployment(spec, local, d, meta, image_tag="t")
    with pytest.raises(ValueError, match="site.kubernetes"):
        knobs.render_job_configmap(spec, local, d, meta)
