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
    """Spark's interval parser takes no abbreviation, so nor does the knob.

    `Trigger.ProcessingTime` parses the string as a SQL interval, which fails
    on `10s` and on `10seconds` alike. Accepted here, either would start a run
    whose query dies at its first micro-batch — with a topic and a table
    already created.
    """
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
        # The one property pyiceberg and Iceberg's Java library spell
        # differently; every other key above carries through untouched.
        "spark.sql.catalog.ice.client.region": "us-east-1",
        "spark.executor.cores": "2",
        "spark.executor.memory": "2048m",
        "spark.driver.cores": "1",
        "spark.driver.memory": "2048m",
        "spark.sql.shuffle.partitions": "4",
        "spark.app.name": d.run_id,
        # A site with no cluster is the local stack, whose object store has to
        # be described a second time for the Hadoop filesystem the checkpoint
        # path goes through.
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
    """A run overrides any rendered setting without this module growing a knob."""
    site = _site()
    d = _derived(site, meta)
    override = {"spark.sql.shuffle.partitions": "16", "spark.executor.memoryOverhead": "1024m"}
    spec = replace(_spec(), engine_block={**_spec().engine_block, "extra_spark_conf": override})
    conf = knobs.render_conf(spec, site, d)
    assert conf["spark.sql.shuffle.partitions"] == "16"
    assert conf["spark.executor.memoryOverhead"] == "1024m"


def test_render_env(meta: metadata.CorpusMetadata) -> None:
    """The submission line's shape, which is read before a session exists."""
    site = _site()
    files = knobs.render(_spec(), site, _derived(site, meta), meta)
    assert files[knobs.ENV_FILE] == "LOCAL_CORES=4\nDRIVER_MEM_MB=2048\n"


def test_the_reader_schema_is_the_corpus_schema_with_zoneless_timestamps(
    meta: metadata.CorpusMetadata,
) -> None:
    """`from_avro` has to yield the zoneless timestamp the table's column is.

    Avro's `timestamp-millis` and `local-timestamp-millis` annotate the same
    `long` and encode identically — the annotation is not on the wire — so the
    rewrite reads the corpus's bytes unchanged while making Spark produce a
    `TimestampNTZ` rather than a zoned instant, which would land in a
    `timestamptz` column the table does not have.
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
        # The run id, so a consumer group an abandoned run left behind names
        # the run that left it.
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
    """The two shipped Spark smokes differ in the framing and in nothing else.

    A knob that drifted between them would make the pair a comparison of two
    fleets rather than of two wire formats.
    """
    raw = model.load_run_spec(ROOT / "runs" / "smoke-spark.yaml")
    framed = model.load_run_spec(ROOT / "runs" / "smoke-spark-confluent.yaml")
    assert raw.kafka.value_encoding == model.VALUE_ENCODING_AVRO
    assert framed.kafka.value_encoding == model.VALUE_ENCODING_CONFLUENT
    assert framed.engine_block == raw.engine_block
    assert framed.kafka.partitions == raw.kafka.partitions and framed.kafka.key == raw.kafka.key
    assert framed.producer == raw.producer and framed.scoring == raw.scoring
    assert framed.table == raw.table and framed.corpus == raw.corpus


def test_both_encodings_are_readable_and_a_third_one_is_refused(meta: metadata.CorpusMetadata) -> None:
    """Spark reads either framing, so the encoding constrains the compute not at all.

    The refusal is for an encoding the spec surface grew without a branch in
    the job: it would otherwise reach the image and fail there, with a topic
    and a table already created.
    """
    spec = _spec()
    knobs.validate(spec.engine_block, spec, meta)
    knobs.validate(spec.engine_block, _confluent(spec), meta)
    unreadable = replace(spec, kafka=replace(spec.kafka, value_encoding="protobuf"))
    with pytest.raises(ValueError, match="value_encoding"):
        knobs.validate(spec.engine_block, unreadable, meta)


def test_the_job_document_carries_the_encoding_and_nothing_else_changes_with_it(
    meta: metadata.CorpusMetadata,
) -> None:
    """The framing is the only difference between the two runs.

    Both are decoded against the same reader schema and committed by the same
    writer, so a second difference here would be a difference in the run rather
    than in what the producer put in front of each value.
    """
    site = _site()
    d = _derived(site, meta)
    raw = json.loads(knobs.render_job(_spec(), site, d, meta))
    framed = json.loads(knobs.render_job(_confluent(_spec()), site, d, meta))
    assert raw["value_encoding"] == "avro" and framed["value_encoding"] == "confluent"
    assert {key: value for key, value in framed.items() if key != "value_encoding"} == {
        key: value for key, value in raw.items() if key != "value_encoding"
    }
    # And the names are the harness's own, not a second spelling of them.
    assert stream_to_iceberg.VALUE_ENCODING_AVRO == model.VALUE_ENCODING_AVRO
    assert stream_to_iceberg.VALUE_ENCODING_CONFLUENT == model.VALUE_ENCODING_CONFLUENT


def test_a_confluent_value_is_decoded_with_its_five_byte_header_dropped() -> None:
    """The strip is the whole of what the encoding costs the job.

    A Confluent value is a zero magic byte, then the schema's registry id as a
    four-byte big-endian integer, then the Avro binary a raw run carries. So
    the sixth byte is where the raw case starts, and `substring` is 1-based.
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
    """The two halves of the run directory's contract, checked against each other.

    The renderer runs in the harness and the job runs inside the Spark image,
    so nothing at run time would report a disagreement about a filename or a
    key — the job would start, find no document, and fail with the topic and
    the table already created.
    """
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
    """The knob is an option and not a default, so both branches are checked."""
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
    """One run has one checkpoint, whatever restarts it.

    Spark's `spark.sql.streaming.checkpointLocation` is a parent path:
    `createQuery` joins it with the query's name, and an unnamed query gets a
    fresh random one on every start. The query would then resume from no state
    after a driver restart, read the topic from `earliest` again, and duplicate
    every row already committed. The writer's own option is used as it stands,
    so it is where the location goes.
    """
    site = _site()
    d = _derived(site, meta)
    conf = knobs.render_conf(_spec(), site, d)
    assert "spark.sql.streaming.checkpointLocation" not in conf
    written = json.loads(knobs.render_job(_spec(), site, d, meta))["write_options"]["checkpointLocation"]
    assert written == knobs.checkpoint_uri(site, d)
    # And the job hands every write option to the writer, so it arrives there.
    parsed = stream_to_iceberg.read_job(_written(knobs.render(_spec(), site, d, meta), knobs.JOB_FILE, tmp_path))
    assert parsed.write_options["checkpointLocation"] == written


def test_the_checkpoint_path_is_the_scheme_spark_reaches_storage_by(meta: metadata.CorpusMetadata) -> None:
    """`s3://` is a vendor alias a stock Spark leaves unbound; S3A is the one it has."""
    site = _site()
    d = _derived(site, meta)
    assert knobs.checkpoint_uri(site, d) == f"s3a://runs/{d.run_id}/checkpoints"
    # Any other store keeps its own scheme: there is no second name for it.
    for runs_root, expected in (("gs://bench/runs", "gs://bench/runs"), ("/mnt/runs", "/mnt/runs")):
        elsewhere = replace(site, runs_root=runs_root)
        assert knobs.checkpoint_uri(elsewhere, d) == f"{expected}/{d.run_id}/checkpoints"
        # And no S3A filesystem to configure for it either.
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
    `aws.region`, which is what librdkafka needs. Spark's client is the Java
    one, where the same authentication is a differently named mechanism and a
    login module — so the signal is translated rather than passed through.
    """
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
    # The pseudo-key is the harness's own and means nothing to any client; the
    # region reaches the pod as AWS_REGION instead. OAUTHBEARER is what the
    # signal said, not what the Java client is told.
    rendered = json.dumps(options)
    assert "aws.region" not in rendered and "OAUTHBEARER" not in rendered


def test_a_site_that_is_not_on_msk_keeps_its_properties() -> None:
    """Half the signal is not the signal, so nothing is translated."""
    for security in (
        {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER"},
        {"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512", "aws.region": "eu-west-1"},
    ):
        options = knobs.kafka_options(security)
        assert "AWS_MSK_IAM" not in json.dumps(options)
        assert options == {f"kafka.{key}": value for key, value in security.items()}


def test_a_glue_catalog_reaches_storage_through_the_sites_warehouse(meta: metadata.CorpusMetadata) -> None:
    """A catalog whose warehouse is an account id still names a FileIO.

    Glue's REST endpoint takes the account as its warehouse, so the storage
    implementation cannot be read off that property — the site's own warehouse
    URI is what carries the scheme.
    """
    site = _aws_site()
    d = _derived(site, meta)
    conf = knobs.render_conf(_spec(), site, d)
    assert conf["spark.sql.catalog.ice.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert conf["spark.sql.catalog.ice.warehouse"] == "123456789012"
    for key in ("rest.sigv4-enabled", "rest.signing-name", "rest.signing-region"):
        assert conf[f"spark.sql.catalog.ice.{key}"] == site.catalog_props[key]
    # A cluster reaches storage as the pod's own identity, so no static key is
    # rendered for it however the site declares its catalog.
    assert not [key for key in conf if key.startswith("spark.hadoop.fs.s3a.")]
    # A site whose storage is on neither scheme names no implementation, and
    # the catalog's own warehouse cannot make it look as though it did.
    nowhere = replace(site, warehouse="/mnt/warehouse")
    assert "spark.sql.catalog.ice.io-impl" not in knobs.render_conf(_spec(), nowhere, d)


def test_render_sparkapplication(meta: metadata.CorpusMetadata) -> None:
    """The whole document the operator is handed, parsed rather than matched."""
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
    """Guaranteed QoS, which is what makes a measured rate the engine's answer.

    Spark requests `cores` and limits at `coreLimit`, and a pod whose CPU
    request and limit differ is Burstable — cores the node may reclaim under
    pressure. It sets the memory limit equal to the request itself, so the
    core numbers are the whole of what this document decides.
    """
    site = _aws_site()
    d = _derived(site, meta)
    spec = replace(_spec(), engine_block={**_spec().engine_block, "driver_cores": 3, "executor_cores": 5})
    fleet = yaml.safe_load(knobs.render_sparkapplication(spec, site, d, meta, image_tag="t"))["spec"]
    assert (fleet["driver"]["cores"], fleet["driver"]["coreLimit"]) == (3, "3")
    assert (fleet["executor"]["cores"], fleet["executor"]["coreLimit"]) == (5, "5")


def test_the_executors_are_given_the_region_the_driver_is(meta: metadata.CorpusMetadata) -> None:
    """An executor reaches the broker and the table itself, so it needs one too.

    The Kafka client signs an MSK IAM token per connection and the table's data
    files are written through Iceberg's own S3 client, both inside the
    executors — and an SDK with no region resolves S3's global endpoint, which
    is refused for a bucket that lives anywhere else. A cluster off AWS has no
    region to name, and then neither half carries the variable.
    """
    site = _aws_site()
    d = _derived(site, meta)
    elsewhere = replace(site, kubernetes=replace(_cluster(), aws_region=None))
    for half in ("driver", "executor"):
        named = yaml.safe_load(knobs.render_sparkapplication(_spec(), site, d, meta, image_tag="t"))["spec"][half]
        assert [entry["name"] for entry in named["env"]] == ["AWS_REGION", "AWS_DEFAULT_REGION"]
        off_aws = yaml.safe_load(knobs.render_sparkapplication(_spec(), elsewhere, d, meta, image_tag="t"))
        assert "env" not in off_aws["spec"][half]


def test_the_documents_repeat_a_shared_value_rather_than_pointing_at_it(meta: metadata.CorpusMetadata) -> None:
    """No YAML anchors: a manifest is read by people as well as by an API server.

    The mount and the placement are the same values on both halves of the
    fleet, and `yaml.safe_dump` renders one object reached twice as an anchor
    and an alias.
    """
    rendered = knobs.render_sparkapplication(_spec(), _aws_site(), _derived(_aws_site(), meta), meta, image_tag="t")
    assert "&id" not in rendered and "*id" not in rendered


def test_kubernetes_name_lowercases_a_run_id() -> None:
    assert knobs.kubernetes_name("smoke-spark-20260908T000000Z") == "smoke-spark-20260908t000000z"
    assert knobs.kubernetes_name("already-lower-1") == "already-lower-1"


def test_only_the_object_names_are_lowercased(meta: metadata.CorpusMetadata) -> None:
    """A run id reaches the two documents as itself everywhere it is not a name.

    An RFC 1123 name is lowercase and a run id's stamp is not. The settings
    carrying the id are not names Kubernetes reads, and lowercasing one of them
    would point a run's checkpoints or its consumer group at something no other
    reader of the run addresses.
    """
    site = _aws_site()
    d = _derived(site, meta)
    conf = yaml.safe_load(knobs.render_sparkapplication(_spec(), site, d, meta, image_tag="t"))["spec"]["sparkConf"]
    assert conf["spark.app.name"] == d.run_id
    job = json.loads(knobs.render_job(_spec(), site, d, meta))
    assert job["group_id"] == d.run_id
    assert job["write_options"]["checkpointLocation"].endswith(f"/{d.run_id}/checkpoints")


def test_render_job_configmap(meta: metadata.CorpusMetadata) -> None:
    """Every file the run rendered, as the pods read them off a mount."""
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
    # The tag names the image a run is submitted as, so a cluster run without
    # one has no engine to start.
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
    """Two pins drift, and the one nobody rereads is the one a cluster runs.

    `sparkVersion` is a required field of a SparkApplication and the job's path
    is inside the image, so both are statements about the Dockerfile — held to
    it here rather than reread by whoever next edits one of the two files.
    """
    dockerfile = (ROOT / "engines" / "spark" / "Dockerfile").read_text()
    assert f"FROM apache/spark:{knobs.SPARK_VERSION}-" in dockerfile
    site = _aws_site()
    document = yaml.safe_load(knobs.render_sparkapplication(_spec(), site, _derived(site, meta), meta, image_tag="t"))
    path = str(document["spec"]["mainApplicationFile"]).removeprefix("local://")
    assert f"{path.rsplit('/', 1)[0]}/" in dockerfile, f"the Dockerfile copies the job nowhere near {path}"


def test_fleet(meta: metadata.CorpusMetadata) -> None:
    assert fleet.fleet(_spec()) == (
        model.FleetRole("driver", 1, 1.0, 2.0, ""),
        model.FleetRole("executor", 2, 2.0, 2.0, ""),
    )
    pinned = replace(
        _spec(),
        engine_block={**_spec().engine_block, "machine_type": "c7g.4xlarge", "driver_cores": 2, "driver_mem_mb": 4096},
    )
    assert fleet.fleet(pinned) == (
        model.FleetRole("driver", 1, 2.0, 4.0, "c7g.4xlarge"),
        model.FleetRole("executor", 2, 2.0, 2.0, "c7g.4xlarge"),
    )
