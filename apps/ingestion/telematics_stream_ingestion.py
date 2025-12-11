import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, MapType

# --- Configuration ---
APP_NAME = "Ingest_Telematics_Bronze"
KAFKA_SERVERS = "kafka:9092"
KAFKA_TOPIC = "telematics_topic"
BASE_PATH = "s3a://car-smart-claims"
BRONZE_PATH = os.path.join(BASE_PATH, "bronze/telematics")
CHECKPOINT_DIR = os.path.join(BRONZE_PATH, "_checkpoints")

def ingest_telematics(spark: SparkSession):
    # Schema for raw JSON payload
    payload_schema = MapType(StringType(), StringType())

    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = stream_df.select(
        from_json(col("value").cast("string"), payload_schema).alias("data")
    ).select(
        col("data.chassis_no").alias("chassis_no"),
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        col("data.event_timestamp").alias("event_timestamp"),
        col("data.speed").alias("speed")
    )

    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="60 seconds")
        .start(BRONZE_PATH)
    )
    
    query.awaitTermination()

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ingest_telematics(spark)