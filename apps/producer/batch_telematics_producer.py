import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct

# --- Configuration ---
APP_NAME = "Producer_Telematics_Batch"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "telematics_topic"
SOURCE_PATH = "/opt/spark/raw_data/telematics/"

def produce_telematics_batch(spark: SparkSession):
    print(f"Reading Parquet source: {SOURCE_PATH}")

    try:
        df = spark.read.parquet(SOURCE_PATH)
    except Exception as e:
        print(f"Error reading source: {e}")
        return

    # Prepare DataFrame for Kafka (Key/Value pairs)
    # We cast all fields to string to ensure JSON compatibility in the value payload
    value_columns = df.columns
    
    kafka_df = df.select(
        [col(c).cast("string") for c in df.columns]
    ).withColumn(
        "key", col("chassis_no")
    ).withColumn(
        "value", to_json(struct(*value_columns))
    )

    record_count = kafka_df.count()
    print(f"Producing {record_count} records to topic '{KAFKA_TOPIC}'...")

    (
        kafka_df.select("key", "value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", KAFKA_TOPIC)
        .save()
    )

    print("Batch production complete.")

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .getOrCreate()
    )

    produce_telematics_batch(spark)
    spark.stop()