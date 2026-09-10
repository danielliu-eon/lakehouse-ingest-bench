# SPDX-License-Identifier: Apache-2.0
import json
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
import yaml

from engines.spark import fleet, knobs, stream_to_iceberg
from ingest_bench import uri
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.specs import derive, engines, model

ROOT = Path(__file__).resolve().parents[1]
STAMP = "20260908T000000Z"


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


def _spec() -> model.RunSpec:
    return model.load_run_spec(ROOT / "runs" / "smoke-spark.yaml")


def _derived(site: model.SiteConfig, meta: metadata.CorpusMetadata) -> derive.Derived:
    return derive.derive(_spec(), site, stamp=STAMP, corpus_dir=meta.name + "-x")


def test_spark_is_a_registered_managed_engine() -> None:
    assert engines.knobs_for("spark") is knobs


def test_validate(meta: metadata.CorpusMetadata) -> None:
    spec = _spec()
    knobs.validate(spec.engine_block, spec, meta)
    with pytest.raises(ValueError, match="distribution_mode"):
        knobs.validate({**spec.engine_block, "distribution_mode": "sorted"}, spec, meta)
    with pytest.raises(ValueError, match="executors must be at least 1"):
        knobs.validate({**spec.engine_block, "executors": 0}, spec, meta)
    with pytest.raises(ValueError, match="executor_cores must be at least 1"):
        knobs.validate({**spec.engine_block, "executor_cores": 0}, spec, meta)
    with pytest.raises(ValueError, match="unknown"):
        knobs.validate({**spec.engine_block, "executor_gpu": 1}, spec, meta)
    with pytest.raises(ValueError, match="must set"):
        knobs.validate({key: value for key, value in spec.engine_block.items() if key != "executors"}, spec, meta)
    with pytest.raises(ValueError, match="max_offsets_per_trigger must be at least 1"):
        knobs.validate({**spec.engine_block, "max_offsets_per_trigger": 0}, spec, meta)


@pytest.mark.parametrize("interval", ["10 seconds", "1 second", "500 milliseconds", "2 minutes", "1 hour"])
def test_a_spark_interval_is_a_count_and_a_whole_unit(interval: str, meta: metadata.CorpusMetadata) -> None:
    spec = _spec()
    knobs.validate({**spec.engine_block, "trigger_interval": interval}, spec, meta)


@pytest.mark.parametrize("interval", ["10s", "500ms", "1m", "1h", "10 sec", "10seconds", "ten seconds", ""])
def test_an_interval_spark_cannot_parse_is_refused_before_the_run(interval: str, meta: metadata.CorpusMetadata) -> None:
    """Spark parses SQL intervals; neither `10s` nor `10seconds` is accepted."""
    spec = _spec()
    with pytest.raises(ValueError, match="trigger_interval"):
        knobs.validate({**spec.engine_block, "trigger_interval": interval}, spec, meta)


def test_render_conf(meta: metadata.CorpusMetadata) -> None:
    site = _site()
    d = _derived(site, meta)
    conf = knobs.render_conf(_spec(), site, d)
    assert conf == {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.ice.type": "rest",
        "spark.sql.catalog.ice.uri": "http://iceberg-rest:8181",
        "spark.sql.catalog.ice.warehouse": "s3://warehouse/",
        "spark.sql.catalog.ice.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.ice.s3.access-key-id": "admin",
        "spark.sql.catalog.ice.s3.endpoint": "http://minio:9000",
        "spark.sql.catalog.ice.s3.path-style-access": "true",
        "spark.sql.catalog.ice.s3.secret-access-key": "password",
        # Translate the region key used by the Java Iceberg client.
        "spark.sql.catalog.ice.client.region": "us-east-1",
        "spark.executor.cores": "2",
        "spark.executor.memory": "2048m",
        "spark.driver.cores": "1",
        "spark.driver.memory": "2048m",
        "spark.sql.shuffle.partitions": "4",
        "spark.app.name": d.run_id,
        # Local checkpoints use Hadoop S3A, which needs its own storage settings.
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.access.key": "admin",
        "spark.hadoop.fs.s3a.secret.key": "password",
    }
    # The properties file is one `key value` per line, which is what
    # `spark-submit --properties-file` reads.
    lines = knobs.render_conf_file(_spec(), site, d).splitlines()
    assert lines[0] == "spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    assert [line.split(" ", 1)[0] for line in lines] == list(conf)


def test_extra_spark_conf_is_applied_last(meta: metadata.CorpusMetadata) -> None:
    site = _site()
    d = _derived(site, meta)
    override = {"spark.sql.shuffle.partitions": "16", "spark.executor.memoryOverhead": "1024m"}
    spec = replace(_spec(), engine_block={**_spec().engine_block, "extra_spark_conf": override})
    conf = knobs.render_conf(spec, site, d)
    assert conf["spark.sql.shuffle.partitions"] == "16"
    assert conf["spark.executor.memoryOverhead"] == "1024m"


def test_render_env(meta: metadata.CorpusMetadata) -> None:
    site = _site()
    files = knobs.render(_spec(), site, _derived(site, meta), meta)
    assert files[knobs.ENV_FILE] == "LOCAL_CORES=4\nDRIVER_MEM_MB=2048\n"


def test_the_reader_schema_is_the_corpus_schema_with_zoneless_timestamps(
    meta: metadata.CorpusMetadata,
) -> None:
    """Both Avro timestamp annotations encode the same long. The reader annotation
    must produce TimestampNTZ to match the Iceberg table.
    """
    published = {"type": "long", "logicalType": "timestamp-millis"}
    zoneless = {"type": "long", "logicalType": "local-timestamp-millis"}
    fields = cast(list[dict[str, object]], meta.schema["fields"])
    expected = [{**field, "type": zoneless} if field["type"] == published else field for field in fields]
    assert [field for field in expected if field["type"] == zoneless], (
        "the corpus publishes no millisecond timestamp, so the rewrite is untested"
    )
    assert json.loads(knobs.render_reader_schema(meta)) == {**meta.schema, "fields": expected}


def test_render_job(meta: metadata.CorpusMetadata) -> None:
    site = _site()
    d = _derived(site, meta)
    document = json.loads(knobs.render_job(_spec(), site, d, meta))
    assert document == {
        "topic": d.run_id,
        "bootstrap": "kafka:9092",
        "group_id": d.run_id,
        "value_encoding": "avro",
        "table": f"ice.ingest_bench.t_{d.run_id.replace('-', '_')}",
        "columns": meta.field_names(),
        "kafka_options": {},
        "write_options": {
            "distribution-mode": "hash",
            "fanout-enabled": "false",
            "check-nullability": "false",
            "checkpointLocation": f"s3a://runs/{d.run_id}/checkpoints",
        },
        "trigger_interval": "10 seconds",
        "max_offsets_per_trigger": None,
    }


def _confluent(spec: model.RunSpec) -> model.RunSpec:
    return replace(spec, kafka=replace(spec.kafka, value_encoding="confluent"))


def test_the_shipped_confluent_spec_is_the_raw_one_plus_its_encoding() -> None:
    raw = model.load_run_spec(ROOT / "runs" / "smoke-spark.yaml")
    framed = model.load_run_spec(ROOT / "runs" / "smoke-spark-confluent.yaml")
    assert raw.kafka.value_encoding == model.VALUE_ENCODING_AVRO
    assert framed.kafka.value_encoding == model.VALUE_ENCODING_CONFLUENT
    assert framed.engine_block == raw.engine_block
    assert framed.kafka.partitions == raw.kafka.partitions and framed.kafka.key == raw.kafka.key
    assert framed.producer == raw.producer and framed.scoring == raw.scoring
    assert framed.table == raw.table and framed.corpus == raw.corpus


def test_both_encodings_are_readable_and_a_third_one_is_refused(meta: metadata.CorpusMetadata) -> None:
    spec = _spec()
    knobs.validate(spec.engine_block, spec, meta)
    knobs.validate(spec.engine_block, _confluent(spec), meta)
    unreadable = replace(spec, kafka=replace(spec.kafka, value_encoding="protobuf"))
    with pytest.raises(ValueError, match="value_encoding"):
        knobs.validate(spec.engine_block, unreadable, meta)


def test_the_job_document_carries_the_encoding_and_nothing_else_changes_with_it(
    meta: metadata.CorpusMetadata,
) -> None:
    site = _site()
    d = _derived(site, meta)
    raw = json.loads(knobs.render_job(_spec(), site, d, meta))
    framed = json.loads(knobs.render_job(_confluent(_spec()), site, d, meta))
    assert raw["value_encoding"] == "avro" and framed["value_encoding"] == "confluent"
    assert {key: value for key, value in framed.items() if key != "value_encoding"} == {
        key: value for key, value in raw.items() if key != "value_encoding"
    }
    assert stream_to_iceberg.VALUE_ENCODING_AVRO == model.VALUE_ENCODING_AVRO
    assert stream_to_iceberg.VALUE_ENCODING_CONFLUENT == model.VALUE_ENCODING_CONFLUENT


def test_a_confluent_value_is_decoded_with_its_five_byte_header_dropped() -> None:
    """The header is one magic byte plus a four-byte schema id. Spark substring
    positions are 1-based, so the Avro record starts at position 6.
    """
    assert stream_to_iceberg.value_expression("avro") == "value"
    assert stream_to_iceberg.value_expression("confluent") == "substring(value, 6, length(value) - 5)"
    with pytest.raises(ValueError, match="protobuf"):
        stream_to_iceberg.value_expression("protobuf")


def _written(files: dict[str, str], name: str, run_dir: Path) -> Path:
    """``files[name]`` on disk, so the job reads it the way the container does."""
    path = run_dir / name
    path.write_text(files[name])
    return path


def test_the_job_reads_back_what_the_renderer_wrote(meta: metadata.CorpusMetadata, tmp_path: Path) -> None:
    site = _site()
    d = _derived(site, meta)
    files = knobs.render(_spec(), site, d, meta)
    assert set(files) == {knobs.CONF_FILE, knobs.ENV_FILE, knobs.SCHEMA_FILE, knobs.JOB_FILE}
    assert stream_to_iceberg.JOB_DOCUMENT.name == knobs.JOB_FILE
    assert stream_to_iceberg.READER_SCHEMA.name == knobs.SCHEMA_FILE

    parsed = stream_to_iceberg.read_job(_written(files, knobs.JOB_FILE, tmp_path))
    assert parsed.topic == d.topic and parsed.group_id == d.run_id
    assert parsed.value_encoding == "avro"
    assert parsed.columns == tuple(meta.field_names())
    assert parsed.write_options == {
        "distribution-mode": "hash",
        "fanout-enabled": "false",
        "check-nullability": "false",
        "checkpointLocation": f"s3a://runs/{d.run_id}/checkpoints",
    }
    assert parsed.trigger_interval == "10 seconds"
    assert stream_to_iceberg.source_options(parsed) == {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": d.topic,
        "startingOffsets": "earliest",
        "kafka.group.id": d.run_id,
    }


def test_a_limited_micro_batch_reaches_the_source(meta: metadata.CorpusMetadata, tmp_path: Path) -> None:
    site = _site()
    d = _derived(site, meta)
    spec = replace(_spec(), engine_block={**_spec().engine_block, "max_offsets_per_trigger": 5000, "fanout": True})
    document = json.loads(knobs.render_job(spec, site, d, meta))
    assert document["max_offsets_per_trigger"] == 5000
    assert document["write_options"]["fanout-enabled"] == "true"
    parsed = stream_to_iceberg.read_job(_written(knobs.render(spec, site, d, meta), knobs.JOB_FILE, tmp_path))
    assert stream_to_iceberg.source_options(parsed)["maxOffsetsPerTrigger"] == "5000"


def test_the_checkpoint_location_is_the_querys_own_and_not_a_parent(
    meta: metadata.CorpusMetadata, tmp_path: Path
) -> None:
    """The session checkpoint setting is a parent directory; an unnamed query gets
    a new child on restart. Set the writer option directly to reuse the same state.
    """
    site = _site()
    d = _derived(site, meta)
    conf = knobs.render_conf(_spec(), site, d)
    assert "spark.sql.streaming.checkpointLocation" not in conf
    written = json.loads(knobs.render_job(_spec(), site, d, meta))["write_options"]["checkpointLocation"]
    assert written == knobs.checkpoint_uri(site, d)
    parsed = stream_to_iceberg.read_job(_written(knobs.render(_spec(), site, d, meta), knobs.JOB_FILE, tmp_path))
    assert parsed.write_options["checkpointLocation"] == written


def test_the_checkpoint_path_is_the_scheme_spark_reaches_storage_by(meta: metadata.CorpusMetadata) -> None:
    """Stock Spark uses S3A for checkpoints; the s3 scheme has no bound filesystem."""
    site = _site()
    d = _derived(site, meta)
    assert knobs.checkpoint_uri(site, d) == f"s3a://runs/{d.run_id}/checkpoints"
    # Only S3 needs a scheme translation.
    for runs_root, expected in (("gs://bench/runs", "gs://bench/runs"), ("/mnt/runs", "/mnt/runs")):
        elsewhere = replace(site, runs_root=runs_root)
        assert knobs.checkpoint_uri(elsewhere, d) == f"{expected}/{d.run_id}/checkpoints"
        conf = knobs.render_conf(_spec(), elsewhere, d)
        assert not [key for key in conf if key.startswith("spark.hadoop.fs.s3a.")]


def test_render_refuses_a_catalog_spark_cannot_read(meta: metadata.CorpusMetadata) -> None:
    site = _site()
    d = _derived(site, meta)
    sql_catalog = replace(site, catalog_props={**site.catalog_props, "type": "sql"})
    with pytest.raises(ValueError, match="REST catalog"):
        knobs.render_conf(_spec(), sql_catalog, d)
    no_uri = replace(site, catalog_props={"warehouse": "gs://warehouse"})
    with pytest.raises(ValueError, match="uri"):
        knobs.render_conf(_spec(), no_uri, d)
    # The FileIO follows the site's warehouse, so a GCS site declares one.
    gcs = replace(
        site,
        runs_root="gs://runs",
        warehouse="gs://warehouse",
        catalog_props={"uri": "http://c:8181", "warehouse": "gs://warehouse"},
    )
    conf = knobs.render_conf(_spec(), gcs, d)
    assert conf["spark.sql.catalog.ice.io-impl"] == "org.apache.iceberg.gcp.gcs.GCSFileIO"


# The AWS shape: an MSK broker reached by IAM, a Glue REST catalog whose
# warehouse is an account id rather than a URI, and a cluster to submit to.
_MSK_SECURITY = {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER", "aws.region": "eu-west-1"}


def _cluster() -> model.KubernetesConfig:
    return model.KubernetesConfig(
        context="bench",
        namespace="ingest-bench",
        harness_service_account="ingest-bench-harness",
        flink_service_account="ingest-bench-flink",
        spark_service_account="ingest-bench-spark",
        service_account_annotations={},
        registry="registry.example/ingest-bench",
        aws_region="eu-west-1",
        secret_name=None,
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
    site = replace(_aws_site(), kafka_security={**_MSK_SECURITY, "ssl.endpoint.identification.algorithm": "https"})
    d = _derived(site, meta)
    options = json.loads(knobs.render_job(_spec(), site, d, meta))["kafka_options"]
    assert options == {
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "AWS_MSK_IAM",
        "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        # A key that is not part of the signal still reaches the client.
        "kafka.ssl.endpoint.identification.algorithm": "https",
    }
    # Remove the harness-only region key; pod environment supplies the SDK region.
    rendered = json.dumps(options)
    assert "aws.region" not in rendered and "OAUTHBEARER" not in rendered


def test_a_site_that_is_not_on_msk_keeps_its_properties() -> None:
    for security in (
        {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER"},
        {"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512", "aws.region": "eu-west-1"},
    ):
        options = knobs.kafka_options(security)
        assert "AWS_MSK_IAM" not in json.dumps(options)
        assert options == {f"kafka.{key}": value for key, value in security.items()}


def test_a_glue_catalog_reaches_storage_through_the_sites_warehouse(meta: metadata.CorpusMetadata) -> None:
    """Glue uses an account id as its catalog warehouse. Derive FileIO from the
    site storage URI instead.
    """
    site = _aws_site()
    d = _derived(site, meta)
    conf = knobs.render_conf(_spec(), site, d)
    assert conf["spark.sql.catalog.ice.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert conf["spark.sql.catalog.ice.warehouse"] == "123456789012"
    for key in ("rest.sigv4-enabled", "rest.signing-name", "rest.signing-region"):
        assert conf[f"spark.sql.catalog.ice.{key}"] == site.catalog_props[key]
    # Cluster storage uses pod identity, without rendered static keys.
    assert not [key for key in conf if key.startswith("spark.hadoop.fs.s3a.")]
    # An unsupported storage scheme must not select a FileIO implementation.
    nowhere = replace(site, warehouse="/mnt/warehouse")
    assert "spark.sql.catalog.ice.io-impl" not in knobs.render_conf(_spec(), nowhere, d)


def test_render_sparkapplication(meta: metadata.CorpusMetadata) -> None:
    spec = _spec()
    site = _aws_site()
    d = _derived(site, meta)
    document = yaml.safe_load(knobs.render_sparkapplication(spec, site, d, meta, image_tag="0.1.0-abc1234"))
    mount = [{"name": "job", "mountPath": "/opt/bench/run", "readOnly": True}]
    placement: dict[str, object] = {
        "nodeSelector": {"bench-pool": "engine"},
        "tolerations": [{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
    }
    region = [{"name": "AWS_REGION", "value": "eu-west-1"}, {"name": "AWS_DEFAULT_REGION", "value": "eu-west-1"}]
    assert document == {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": "smoke-spark-20260908t000000z", "namespace": "ingest-bench"},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": "registry.example/ingest-bench/lakehouse-ingest-bench/spark:0.1.0-abc1234",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/bench/engines/spark/stream_to_iceberg.py",
            "sparkVersion": knobs.SPARK_VERSION,
            "restartPolicy": {"type": "Never"},
            "sparkConf": knobs.render_conf(spec, site, d),
            "driver": {
                "cores": 1,
                "coreLimit": "1",
                "memory": "2048m",
                "serviceAccount": "ingest-bench-spark",
                "volumeMounts": mount,
                **placement,
                "env": region,
            },
            "executor": {
                "instances": 2,
                "cores": 2,
                "coreLimit": "2",
                "memory": "2048m",
                "volumeMounts": mount,
                **placement,
                "env": region,
            },
            "volumes": [{"name": "job", "configMap": {"name": "smoke-spark-20260908t000000z-spark-job"}}],
        },
    }


def test_both_halves_of_the_fleet_ask_for_as_much_cpu_as_they_cap_at(meta: metadata.CorpusMetadata) -> None:
    """The operator sets equal memory requests and limits. Matching CPU requests
    and limits completes the requirements for Guaranteed QoS.
    """
    site = _aws_site()
    d = _derived(site, meta)
    spec = replace(_spec(), engine_block={**_spec().engine_block, "driver_cores": 3, "executor_cores": 5})
    fleet = yaml.safe_load(knobs.render_sparkapplication(spec, site, d, meta, image_tag="t"))["spec"]
    assert (fleet["driver"]["cores"], fleet["driver"]["coreLimit"]) == (3, "3")
    assert (fleet["executor"]["cores"], fleet["executor"]["coreLimit"]) == (5, "5")


def test_the_executors_are_given_the_region_the_driver_is(meta: metadata.CorpusMetadata) -> None:
    """Executors create Kafka and S3 clients themselves, so they also need a region."""
    site = _aws_site()
    d = _derived(site, meta)
    elsewhere = replace(site, kubernetes=replace(_cluster(), aws_region=None))
    for half in ("driver", "executor"):
        named = yaml.safe_load(knobs.render_sparkapplication(_spec(), site, d, meta, image_tag="t"))["spec"][half]
        assert [entry["name"] for entry in named["env"]] == ["AWS_REGION", "AWS_DEFAULT_REGION"]
        off_aws = yaml.safe_load(knobs.render_sparkapplication(_spec(), elsewhere, d, meta, image_tag="t"))
        assert "env" not in off_aws["spec"][half]


def test_the_documents_repeat_a_shared_value_rather_than_pointing_at_it(meta: metadata.CorpusMetadata) -> None:
    """Shared Python objects can render as YAML anchors; repeat values for readability."""
    rendered = knobs.render_sparkapplication(_spec(), _aws_site(), _derived(_aws_site(), meta), meta, image_tag="t")
    assert "&id" not in rendered and "*id" not in rendered


def test_kubernetes_name_lowercases_a_run_id() -> None:
    assert knobs.kubernetes_name("smoke-spark-20260908T000000Z") == "smoke-spark-20260908t000000z"
    assert knobs.kubernetes_name("already-lower-1") == "already-lower-1"


def test_only_the_object_names_are_lowercased(meta: metadata.CorpusMetadata) -> None:
    site = _aws_site()
    d = _derived(site, meta)
    conf = yaml.safe_load(knobs.render_sparkapplication(_spec(), site, d, meta, image_tag="t"))["spec"]["sparkConf"]
    assert conf["spark.app.name"] == d.run_id
    job = json.loads(knobs.render_job(_spec(), site, d, meta))
    assert job["group_id"] == d.run_id
    assert job["write_options"]["checkpointLocation"].endswith(f"/{d.run_id}/checkpoints")


def test_render_job_configmap(meta: metadata.CorpusMetadata) -> None:
    spec = _spec()
    site = _aws_site()
    d = _derived(site, meta)
    document = yaml.safe_load(knobs.render_job_configmap(spec, site, d, meta))
    assert document["apiVersion"] == "v1" and document["kind"] == "ConfigMap"
    assert document["metadata"] == {"name": "smoke-spark-20260908t000000z-spark-job", "namespace": "ingest-bench"}
    files = knobs.render(spec, site, d, meta, image_tag="t")
    assert document["data"] == {
        name: files[name] for name in (knobs.CONF_FILE, knobs.ENV_FILE, knobs.SCHEMA_FILE, knobs.JOB_FILE)
    }


def test_a_cluster_run_is_two_more_files_and_needs_an_image(meta: metadata.CorpusMetadata) -> None:
    spec = _spec()
    site = _aws_site()
    d = _derived(site, meta)
    assert set(knobs.render(spec, site, d, meta, image_tag="t")) == {
        knobs.CONF_FILE,
        knobs.ENV_FILE,
        knobs.SCHEMA_FILE,
        knobs.JOB_FILE,
        knobs.SPARKAPPLICATION_FILE,
        knobs.CONFIGMAP_FILE,
    }
    # Cluster rendering requires an image tag.
    with pytest.raises(ValueError, match="image_tag"):
        knobs.render(spec, site, d, meta)
    # No cluster, no Kubernetes documents — and nothing to refuse either.
    local = _site()
    assert set(knobs.render(spec, local, _derived(local, meta), meta)) == {
        knobs.CONF_FILE,
        knobs.ENV_FILE,
        knobs.SCHEMA_FILE,
        knobs.JOB_FILE,
    }
    with pytest.raises(ValueError, match="site.kubernetes"):
        knobs.render_sparkapplication(spec, local, d, meta, image_tag="t")
    with pytest.raises(ValueError, match="site.kubernetes"):
        knobs.render_job_configmap(spec, local, d, meta)


def test_the_pinned_spark_is_the_one_the_image_carries(meta: metadata.CorpusMetadata) -> None:
    dockerfile = (ROOT / "engines" / "spark" / "Dockerfile").read_text()
    assert f"FROM apache/spark:{knobs.SPARK_VERSION}-" in dockerfile
    site = _aws_site()
    document = yaml.safe_load(knobs.render_sparkapplication(_spec(), site, _derived(site, meta), meta, image_tag="t"))
    path = str(document["spec"]["mainApplicationFile"]).removeprefix("local://")
    assert f"{path.rsplit('/', 1)[0]}/" in dockerfile, f"the Dockerfile copies the job nowhere near {path}"


def test_fleet(meta: metadata.CorpusMetadata) -> None:
    # An unspecified machine type is allowed for probes but rejected for publication.
    assert fleet.fleet(_spec()) == (
        model.FleetRole("driver", 1, 1.0, 2.0, model.MACHINE_TYPE_UNSPECIFIED),
        model.FleetRole("executor", 2, 2.0, 2.0, model.MACHINE_TYPE_UNSPECIFIED),
    )
    pinned = replace(
        _spec(),
        engine_block={**_spec().engine_block, "machine_type": "c7g.4xlarge", "driver_cores": 2, "driver_mem_mb": 4096},
    )
    assert fleet.fleet(pinned) == (
        model.FleetRole("driver", 1, 2.0, 4.0, "c7g.4xlarge"),
        model.FleetRole("executor", 2, 2.0, 2.0, "c7g.4xlarge"),
    )


def test_the_fleet_reads_the_secret_the_site_names_and_the_job_document_keeps_the_reference(
    meta: metadata.CorpusMetadata, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    security = {**_MSK_SECURITY, "sasl.password": "${env:IB_KAFKA_PASSWORD}"}
    site = replace(_aws_site(), kafka_security=security, kubernetes=replace(_cluster(), secret_name="bench-env"))
    d = derive.derive(_spec(), site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")

    document = yaml.safe_load(knobs.render_sparkapplication(_spec(), site, d, meta, image_tag="t"))
    for half in ("driver", "executor"):
        assert document["spec"][half]["envFrom"] == [{"secretRef": {"name": "bench-env"}}], half

    files = knobs.render(_spec(), site, d, meta, image_tag="t")
    written = files[knobs.JOB_FILE]
    assert '"kafka.sasl.password": "${env:IB_KAFKA_PASSWORD}"' in written
    assert "hunter2" not in written
    monkeypatch.setenv("IB_KAFKA_PASSWORD", "hunter2")
    parsed = stream_to_iceberg.read_job(_written(files, knobs.JOB_FILE, tmp_path))
    assert parsed.kafka_options["kafka.sasl.password"] == "hunter2"
    # Nothing resolved is written back: the document on disk still names it.
    assert '"kafka.sasl.password": "${env:IB_KAFKA_PASSWORD}"' in (tmp_path / knobs.JOB_FILE).read_text()


def test_a_cluster_that_names_no_secret_gives_neither_half_an_env_from(meta: metadata.CorpusMetadata) -> None:
    site = _aws_site()
    d = derive.derive(_spec(), site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_sparkapplication(_spec(), site, d, meta, image_tag="t"))
    assert "envFrom" not in document["spec"]["driver"] and "envFrom" not in document["spec"]["executor"]


def test_a_reference_spark_cannot_resolve_is_refused_rather_than_rendered(
    meta: metadata.CorpusMetadata,
) -> None:
    """Only the Kafka source resolves environment references; Spark settings do not."""
    props = {**_aws_site().catalog_props, "rest.token": "${env:IB_CATALOG_TOKEN}"}
    site = replace(_aws_site(), catalog_props=props)
    d = derive.derive(_spec(), site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    with pytest.raises(ValueError, match=r"spark\.sql\.catalog\.ice\.rest\.token.*site\.kafka\.security"):
        knobs.render_conf(_spec(), site, d)
