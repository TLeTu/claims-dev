import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, MapType

# --- Configuration ---
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TELEMATICS_TOPIC = "telematics_topic"

# Delta Lake Configuration
# These paths are inside the container. Consider using volumes for persistence.
BASE_S3_PATH = "s3a://car-smart-claims"
TELEMATICS_BRONZE_PATH = os.path.join(BASE_S3_PATH, "bronze/telematics")
TELEMATICS_CHECKPOINT = os.path.join(TELEMATICS_BRONZE_PATH, "_checkpoint")


def ingest_telematics(spark: SparkSession):
    """Reads telematics data from Kafka and writes to a Delta table."""
    print(f"Starting telematics stream from topic: {TELEMATICS_TOPIC}")

    # Define the schema of the JSON data in the Kafka message value
    payload_schema = MapType(StringType(), StringType())

    # Read from Kafka
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TELEMATICS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse the JSON and flatten the structure
    parsed_df = stream_df.select(
        from_json(col("value").cast("string"), payload_schema).alias("data")
    ).select(
        col("data.chassis_no").alias("chassis_no"),
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        col("data.event_timestamp").alias("event_timestamp"),
        col("data.speed").alias("speed")
    )

    # Write the stream to a Delta table
    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", TELEMATICS_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start(TELEMATICS_BRONZE_PATH)
    )
    print(f"Telematics stream writing to {TELEMATICS_BRONZE_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DeltaTelematicsIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    ingest_telematics(spark)