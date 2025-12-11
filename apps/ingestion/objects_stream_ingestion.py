import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, BinaryType

# --- Configuration ---
APP_NAME = "Ingest_Objects_Bronze"
BASE_PATH = "s3a://car-smart-claims"
LANDING_PATH = os.path.join(BASE_PATH, "landing")
BRONZE_PATH = os.path.join(BASE_PATH, "bronze")

# Sources
SRC_IMG_CLAIMS = os.path.join(LANDING_PATH, "claims/images")
SRC_METADATA = os.path.join(LANDING_PATH, "claims/metadata")
SRC_IMG_TRAINING = os.path.join(LANDING_PATH, "training_imgs")

# Sinks
SINK_IMG_CLAIMS = os.path.join(BRONZE_PATH, "claim_images")
SINK_METADATA = os.path.join(BRONZE_PATH, "image_metadata")
SINK_IMG_TRAINING = os.path.join(BRONZE_PATH, "training_images")

# Schemas
SCHEMA_METADATA = StructType([
    StructField("image_name", StringType(), True),
    StructField("image_id", StringType(), True), 
    StructField("claim_no", StringType(), True),
    StructField("chassis_no", StringType(), True)
])

SCHEMA_BINARY = StructType([
    StructField("path", StringType(), False),
    StructField("modificationTime", TimestampType(), False),
    StructField("length", LongType(), False),
    StructField("content", BinaryType(), True)
])

def ingest_claim_images(spark: SparkSession):
    stream_df = (
        spark.readStream
        .format("binaryFile")
        .schema(SCHEMA_BINARY)
        .load(SRC_IMG_CLAIMS)
        .filter(lower(col("path")).rlike(".*\\.(jpg|jpeg|png)$"))
    )
    
    return (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(SINK_IMG_CLAIMS, "_checkpoints"))
        .option("mergeSchema", "true") 
        .trigger(processingTime="60 seconds")
        .start(SINK_IMG_CLAIMS)
    )

def ingest_image_metadata(spark: SparkSession):
    stream_df = (
        spark.readStream
        .format("csv")
        .option("header", "true") 
        .schema(SCHEMA_METADATA)
        .load(SRC_METADATA)
    )
    
    return (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(SINK_METADATA, "_checkpoints"))
        .option("mergeSchema", "true") 
        .trigger(processingTime="60 seconds")
        .start(SINK_METADATA)
    )

def ingest_training_images(spark: SparkSession):
    stream_df = (
        spark.readStream
        .format("binaryFile")
        .schema(SCHEMA_BINARY)
        .load(SRC_IMG_TRAINING)
        .filter(lower(col("path")).rlike(".*\\.(jpg|jpeg|png)$"))
    )
    
    return (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(SINK_IMG_TRAINING, "_checkpoints"))
        .option("mergeSchema", "true") 
        .trigger(processingTime="60 seconds")
        .start(SINK_IMG_TRAINING)
    )

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
    
    ingest_claim_images(spark)
    ingest_image_metadata(spark)
    ingest_training_images(spark)
    
    spark.streams.awaitAnyTermination()