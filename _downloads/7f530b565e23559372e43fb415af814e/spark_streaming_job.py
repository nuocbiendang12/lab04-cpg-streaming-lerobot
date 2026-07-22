"""Task 5: Source Metadata Ingestion into MongoDB via Spark Structured Streaming.

Reads the cpg.metadata Kafka topic and writes each file's metadata document
into MongoDB through the MongoDB Spark Connector. Uses a checkpoint location
so the job resumes from the last processed Kafka offset on restart (Task 6
relies on this: re-running after a single-file replay must not reprocess the
untouched files).

Idempotency: the metadata document's MongoDB _id is set to the CPG file_path,
so the connector's default upsert-by-_id behavior overwrites the same
document on replay instead of inserting a duplicate.

Usage:
    python spark_streaming_job.py                # process all currently available data, then exit
    python spark_streaming_job.py --continuous    # run as a long-lived streaming query
"""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = "localhost:29092"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "cpg_db"
MONGO_COLLECTION = "cpg_metadata"
CHECKPOINT_DIR = Path(__file__).resolve().parent.parent / "checkpoints" / "cpg-metadata"

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
])

METADATA_SCHEMA = StructType([
    StructField("schema_version", StringType()),
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("file_path", StringType()),
    StructField("file_hash", StringType()),
    StructField("size_bytes", LongType()),
    StructField("loc", LongType()),
    StructField("num_functions", LongType()),
    StructField("num_classes", LongType()),
    StructField("num_nodes", LongType()),
    StructField("num_edges", LongType()),
])


def build_spark():
    return (
        SparkSession.builder.appName("CPG-Metadata-Streaming")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run(continuous: bool = False):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "cpg.metadata")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), METADATA_SCHEMA).alias("data")
    ).select("data.*")

    # MongoDB Spark Connector upserts on _id by default; using file_path as the
    # id is what makes replaying an unchanged/modified file idempotent.
    out = parsed.withColumnRenamed("file_path", "_id")

    writer = (
        out.writeStream.format("mongodb")
        .option("spark.mongodb.connection.uri", MONGO_URI)
        .option("spark.mongodb.database", MONGO_DB)
        .option("spark.mongodb.collection", MONGO_COLLECTION)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .outputMode("append")
    )

    if continuous:
        query = writer.trigger(processingTime="10 seconds").start()
    else:
        query = writer.trigger(availableNow=True).start()

    query.awaitTermination()
    print("Streaming query finished. Final progress:", query.lastProgress)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--continuous", action="store_true",
                         help="Run as a long-lived streaming query instead of processing available data once")
    args = parser.parse_args()
    run(continuous=args.continuous)
