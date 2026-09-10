# SPDX-License-Identifier: Apache-2.0
"""Run a rendered Structured Streaming job inside the Spark image.

Released connectors handle Kafka, Avro, Parquet, and Iceberg. Settings come
from spark-submit's properties file and the documents under RUN_DIR.
Import PySpark inside main so this module remains testable outside the image.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from engines.spark.env import substitute_env_values

# Mount beside the job; /run is reserved for container runtime files.
RUN_DIR = Path("/opt/bench/run")
JOB_DOCUMENT = RUN_DIR / "job.json"
READER_SCHEMA = RUN_DIR / "reader-schema.avsc"

# Keep encoding names aligned with the spec; the image lacks the harness package.
VALUE_ENCODING_AVRO = "avro"
VALUE_ENCODING_CONFLUENT = "confluent"

# Confluent framing adds a magic byte and a four-byte schema ID. Drop those
# five bytes using Spark's 1-based substring. Each run uses one known schema,
# so the job needs no registry lookup.
VALUE_EXPRESSIONS = {
    VALUE_ENCODING_AVRO: "value",
    VALUE_ENCODING_CONFLUENT: "substring(value, 6, length(value) - 5)",
}


@dataclass(frozen=True)
class Job:
    """Rendered source, sink, and trigger settings for one run."""

    topic: str
    bootstrap: str
    group_id: str
    value_encoding: str
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
    """Read and validate the job document at ``path``.

    Require every key so renderer omissions fail explicitly. Resolve Kafka option
    placeholders using the container's environment.
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
        value_encoding=_str_at(document, "value_encoding", path),
        table=_str_at(document, "table", path),
        columns=tuple(str(name) for name in cast(list[object], columns)),
        # Keep resolved values in memory; archived documents retain their references.
        kafka_options=substitute_env_values(_string_map_at(document, "kafka_options", path)),
        write_options=_string_map_at(document, "write_options", path),
        trigger_interval=_str_at(document, "trigger_interval", path),
        max_offsets_per_trigger=limit,
    )


def source_options(job: Job) -> dict[str, str]:
    """Build Kafka source options, applying site properties last.

    Read from earliest because records may arrive before the engine starts.
    """
    options = {
        "kafka.bootstrap.servers": job.bootstrap,
        "subscribe": job.topic,
        "startingOffsets": "earliest",
        # Use the run ID to identify consumer groups left by abandoned runs.
        "kafka.group.id": job.group_id,
        **job.kafka_options,
    }
    if job.max_offsets_per_trigger is not None:
        options["maxOffsetsPerTrigger"] = str(job.max_offsets_per_trigger)
    return options


def value_expression(encoding: str) -> str:
    """Return the expression extracting Avro bytes for ``encoding``."""
    if encoding not in VALUE_EXPRESSIONS:
        raise ValueError(f"value encoding {encoding!r} is not one this job decodes: {sorted(VALUE_EXPRESSIONS)}")
    return VALUE_EXPRESSIONS[encoding]


def main() -> int:
    job = read_job(JOB_DOCUMENT)
    schema = READER_SCHEMA.read_text()
    # Reject unsupported encodings before starting Spark.
    value = value_expression(job.value_encoding)

    from pyspark.sql import SparkSession
    from pyspark.sql.avro.functions import from_avro
    from pyspark.sql.functions import col, expr

    # spark-submit supplies the complete session configuration.
    spark = SparkSession.builder.getOrCreate()
    records = spark.readStream.format("kafka").options(**source_options(job)).load()
    # Select columns explicitly so a missing schema field fails instead of
    # producing a table with fewer columns.
    rows = records.select(from_avro(expr(value), schema).alias("record")).select(
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
