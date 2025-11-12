import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
# ADDED imports for the binaryFile schema
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, BinaryType

# --- Configuration ---
# Using a local path to simulate a cloud S3/ADLS/GCS path, following the pattern in your scripts
BASE_S3_PATH = "/opt/spark/bucket"
RAW_DATA_PATH = "/opt/spark/raw_data"

# == Source Paths (where new files will be dropped) ==
CLAIM_IMAGES_SOURCE_PATH = os.path.join(RAW_DATA_PATH, "claims/images")
METADATA_SOURCE_PATH = os.path.join(RAW_DATA_PATH, "claims/metadata")
TRAINING_IMAGES_SOURCE_PATH = os.path.join(RAW_DATA_PATH, "training_imgs")

# == Sink & Checkpoint Paths (where Delta tables will be written) ==
CLAIM_IMAGES_BRONZE_PATH = os.path.join(BASE_S3_PATH, "bronze/objects/claim_images")
CLAIM_IMAGES_CHECKPOINT = os.path.join(BASE_S3_PATH, "bronze/objects/claim_images/_checkpoints")

METADATA_BRONZE_PATH = os.path.join(BASE_S3_PATH, "bronze/objects/image_metadata")
METADATA_CHECKPOINT = os.path.join(BASE_S3_PATH, "bronze/objects/image_metadata/_checkpoints")

TRAINING_IMAGES_BRONZE_PATH = os.path.join(BASE_S3_PATH, "bronze/objects/training_images")
TRAINING_IMAGES_CHECKPOINT = os.path.join(BASE_S3_PATH, "bronze/objects/training_images/_checkpoints")

# Schema for the image_metadata.csv file
METADATA_SCHEMA = StructType([
    StructField("image_name", StringType(), True),
    StructField("image_id", StringType(), True), 
    StructField("claim_no", StringType(), True),
    StructField("chassis_no", StringType(), True)
])

# --- ADDED: Schema for binaryFile format ---
# This is the standard schema that .format("binaryFile") produces.
BINARY_FILE_SCHEMA = StructType([
    StructField("path", StringType(), False),
    StructField("modificationTime", TimestampType(), False),
    StructField("length", LongType(), False),
    StructField("content", BinaryType(), True)
])

# --- Ingestion Functions ---

def ingest_claim_images(spark: SparkSession):
    """
    Uses standard Spark streaming (.format("binaryFile")) to read claim images.
    """
    print(f"Starting claim images stream from: {CLAIM_IMAGES_SOURCE_PATH}")
    
    stream_df = (
        spark.readStream
        .format("binaryFile")
        # --- ADDED: Explicitly provide the schema ---
        .schema(BINARY_FILE_SCHEMA)
        .load(CLAIM_IMAGES_SOURCE_PATH)
        .filter(
            lower(col("path")).endswith(".jpg") |
            lower(col("path")).endswith(".jpeg") |
            lower(col("path")).endswith(".png")
        )
    )
    
    query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CLAIM_IMAGES_CHECKPOINT)
        .option("mergeSchema", "true") 
        .trigger(processingTime="60 seconds")
        .start(CLAIM_IMAGES_BRONZE_PATH)
    )
    
    print(f"Claim images stream writing to {CLAIM_IMAGES_BRONZE_PATH}")
    return query

def ingest_image_metadata(spark: SparkSession):
    """
    Uses standard Spark streaming (.format("csv")) to read image metadata.
    """
    print(f"Starting image metadata stream from: {METADATA_SOURCE_PATH}")
    
    stream_df = (
        spark.readStream
        .format("csv")
        .option("header", "true") 
        .schema(METADATA_SCHEMA)  # This was already correct
        .load(METADATA_SOURCE_PATH)
    )
    
    query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", METADATA_CHECKPOINT)
        .option("mergeSchema", "true") 
        .trigger(processingTime="60 seconds")
        .start(METADATA_BRONZE_PATH)
    )
    
    print(f"Image metadata stream writing to {METADATA_BRONZE_PATH}")
    return query

def ingest_training_images(spark: SparkSession):
    """
    Uses standard Spark streaming (.format("binaryFile")) to read training images.
    """
    print(f"Starting training images stream from: {TRAINING_IMAGES_SOURCE_PATH}")
    
    stream_df = (
        spark.readStream
        .format("binaryFile")
        # --- ADDED: Explicitly provide the schema ---
        .schema(BINARY_FILE_SCHEMA)
        .load(TRAINING_IMAGES_SOURCE_PATH)
        .filter(
            lower(col("path")).endswith(".jpg") |
            lower(col("path")).endswith(".jpeg") |
            lower(col("path")).endswith(".png")
        )
    )
    
    query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", TRAINING_IMAGES_CHECKPOINT)
        .option("mergeSchema", "true") 
        .trigger(processingTime="60 seconds")
        .start(TRAINING_IMAGES_BRONZE_PATH)
    )
    
    print(f"Training images stream writing to {TRAINING_IMAGES_BRONZE_PATH}")
    return query

# --- Main Execution ---

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("DeltaObjectsIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    print("Spark session created. Starting all object streams...")
    
    # Start all three streams
    query_claims_img = ingest_claim_images(spark)
    query_metadata = ingest_image_metadata(spark)
    query_training_img = ingest_training_images(spark)
    
    print("All streams started. Awaiting any termination...")
    
    spark.streams.awaitAnyTermination()