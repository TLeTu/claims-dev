import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct

# --- CONFIGURATION ---
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "telematics_topic"
PARQUET_FILE_PATH = "/opt/spark/raw_data/telematics/"
# ---------------------

def produce_telematics_batch(spark: SparkSession):
    """
    Reads all telematics data from Parquet files in a directory,
    and writes it as a batch to a Kafka topic.
    """
    print(f"Reading all Parquet files from: {PARQUET_FILE_PATH}")

    try:
        # Read all parquet files in the directory into a DataFrame.
        # Spark handles the parallel reading automatically.
        df = spark.read.parquet(PARQUET_FILE_PATH)
    except Exception as e:
        # This can happen if the directory is empty or doesn't exist.
        print(f"Error reading Parquet files: {e}")
        print("Please ensure the directory exists and contains .parquet files.")
        return

    # Get a list of all columns to be included in the JSON value
    value_columns = [c for c in df.columns]

    # 1. Cast all columns to String to match the consumer's schema.
    # 2. Use 'chassis_no' as the Kafka message key for partitioning.
    # 3. Create a 'value' column by packing all columns into a JSON string.
    #    This is what the Kafka sink expects.
    kafka_df = df.select(
        [col(c).cast("string") for c in df.columns]
    ).withColumn(
        "key", col("chassis_no")
    ).withColumn(
        "value", to_json(struct(*value_columns))
    )

    print(f"Writing {kafka_df.count()} records to Kafka topic '{KAFKA_TOPIC}'...")

    # Write the DataFrame to Kafka
    (
        kafka_df.select("key", "value") # Select only the key and value for the sink
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", KAFKA_TOPIC)
        .save()
    )

    print("Successfully wrote batch to Kafka.")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("BatchTelematicsProducer") \
        .getOrCreate()

    produce_telematics_batch(spark)
    spark.stop()