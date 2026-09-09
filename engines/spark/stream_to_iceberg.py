"""Run a rendered streaming job on Spark, from inside the Spark image.

This is the whole of the engine's code. Everything the job then does — reading
Kafka, decoding Avro, encoding Parquet, committing to Iceberg — is done by
released connectors, so a result attributed to Spark is Spark's rather than
this harness's. The job holds no schema logic beyond handing `from_avro` the
reader schema `knobs.py` wrote.

Nothing is read from the command line: every setting arrives either in the
properties file `spark-submit` was given or in the two documents under
`RUN_DIR`. That keeps the submission line identical for every run, so two runs
differ only in files a reader can diff.

PySpark is installed in the image and not in this repository's environment, so
it is imported where it is used rather than at module scope: that keeps this
module importable — and therefore checkable against the renderer — without
Spark.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

# Where the run's rendered files are mounted. Under `/opt/bench` beside the job
# rather than at `/run`, which is the container's own runtime directory.
RUN_DIR = Path("/opt/bench/run")
JOB_DOCUMENT = RUN_DIR / "job.json"
READER_SCHEMA = RUN_DIR / "reader-schema.avsc"


@dataclass(frozen=True)
class Job:
    """One run's source, sink and cadence, as the renderer wrote them down."""

    topic: str
    bootstrap: str
    group_id: str
    table: str
    columns: tuple[str, ...]
    kafka_options: dict[str, str]
    write_options: dict[str, str]
    trigger_interval: str
    max_offsets_per_trigger: int | None


def _str_at(document: dict[str, object], key: str, path: Path) -> str:
    value = document[key]
    if not isinstance(value, str):
        raise ValueError(f"{path} key {key!r} must be a string, got {value!r}")
    return value


def _string_map_at(document: dict[str, object], key: str, path: Path) -> dict[str, str]:
    value = document[key]
    if not isinstance(value, dict):
        raise ValueError(f"{path} key {key!r} must be a mapping, got {value!r}")
    return {str(name): str(entry) for name, entry in cast(dict[object, object], value).items()}


def read_job(path: Path) -> Job:
    """The job document at ``path``, or a refusal to read it as one.

    Every key is read by name and none has a default: a key the renderer
    stopped writing has to surface here rather than as a job that quietly
    consumed from the wrong offset or committed to no table.
    """
    loaded = json.loads(path.read_text())
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must hold a mapping, got {type(loaded).__name__}")
    document = cast(dict[str, object], loaded)
    columns = document["columns"]
    if not isinstance(columns, list):
        raise ValueError(f"{path} key 'columns' must be a list of column names, got {columns!r}")
    limit = document["max_offsets_per_trigger"]
    if limit is not None and not isinstance(limit, int):
        raise ValueError(f"{path} key 'max_offsets_per_trigger' must be an integer or null, got {limit!r}")
    return Job(
        topic=_str_at(document, "topic", path),
        bootstrap=_str_at(document, "bootstrap", path),
        group_id=_str_at(document, "group_id", path),
        table=_str_at(document, "table", path),
        columns=tuple(str(name) for name in cast(list[object], columns)),
        kafka_options=_string_map_at(document, "kafka_options", path),
        write_options=_string_map_at(document, "write_options", path),
        trigger_interval=_str_at(document, "trigger_interval", path),
        max_offsets_per_trigger=limit,
    )


def source_options(job: Job) -> dict[str, str]:
    """The Kafka source's options, in the order they are applied.

    ``startingOffsets`` is the topic's head: the producer publishes before an
    engine is asked to consume, and a latest-offset reader would skip that head
    and be scored as having lost it.

    The site's own properties come last so that a run against a broker needing
    authentication is not overruled by a default above — and among them is the
    only place a value can arrive that this module did not choose.
    """
    options = {
        "kafka.bootstrap.servers": job.bootstrap,
        "subscribe": job.topic,
        "startingOffsets": "earliest",
        # The run id, so a consumer group an abandoned run left behind names
        # the run that left it.
        "kafka.group.id": job.group_id,
        **job.kafka_options,
    }
    if job.max_offsets_per_trigger is not None:
        options["maxOffsetsPerTrigger"] = str(job.max_offsets_per_trigger)
    return options


def main() -> int:
    job = read_job(JOB_DOCUMENT)
    schema = READER_SCHEMA.read_text()

    from pyspark.sql import SparkSession
    from pyspark.sql.avro.functions import from_avro
    from pyspark.sql.functions import col

    # No settings here: every one of them is in the properties file
    # `spark-submit` was given, which is the file a reader diffs between runs.
    spark = SparkSession.builder.getOrCreate()
    records = spark.readStream.format("kafka").options(**source_options(job)).load()
    # The value bytes are Avro's single-record encoding, so the reader schema is
    # the whole of the decoding. The columns are then named individually rather
    # than expanded, so a corpus column the schema stopped carrying fails here
    # instead of committing a table one column short.
    rows = records.select(from_avro(col("value"), schema).alias("record")).select(
        *(col(f"record.{name}").alias(name) for name in job.columns)
    )
    query = (
        rows.writeStream.format("iceberg")
        .outputMode("append")
        .options(**job.write_options)
        .trigger(processingTime=job.trigger_interval)
        .toTable(job.table)
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
