import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, date_add, split, concat_ws
from pyspark.sql.types import IntegerType, DateType
from delta.tables import DeltaTable

# --- 1. Define Paths ---
BRONZE_BASE_PATH = "/opt/spark/bucket/bronze/database" # Path from cdc_stream_ingestion.py
SILVER_BASE_PATH = "/opt/spark/bucket/silver"

# --- Paths for the 'customer' table ---
CUSTOMER_BRONZE_TABLE_PATH = os.path.join(BRONZE_BASE_PATH, "customer")
CUSTOMER_SILVER_TABLE_PATH = os.path.join(SILVER_BASE_PATH, "customer")
CUSTOMER_SILVER_CHECKPOINT = os.path.join(SILVER_BASE_PATH, "customer", "_checkpoints")

# --- 2. Define the Transformation Logic ---
def transform_bronze_to_silver(bronze_df, spark):
    """
    Applies all cleaning, validation, and enrichment logic for ONE table.
    This is where you'll do 90% of your work.
    """
    print(f"Applying Silver transformations to {TABLE_NAME}...")
    
    # Transformations for 'customer' table:
    silver_df = (
        bronze_df
        .dropna(subset=["customer_id"])
        .dropDuplicates(["customer_id"])
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        # --- Cleaning: Convert dates from days-since-epoch (integer) to DateType ---
        # The date '1970-01-01' is the epoch start date.
        # We first cast the string from bronze to an integer, then add it to the epoch date.
        .withColumn("date_of_birth", date_add(lit("1970-01-01"), col("date_of_birth").cast(IntegerType())))
        # --- Enrichment & Cleaning: Create structured fields from raw ones ---
        .withColumn("firstname", split(col("name"), " ").getItem(0))
        .withColumn("lastname", split(col("name"), " ").getItem(1))
        .withColumn("address", concat_ws(", ", col("borough"), col("zip_code")))
        .drop("name", "borough", "zip_code")
    )

    return silver_df

# --- 3. Define the `foreachBatch` Merge Function ---
def upsert_to_silver(batch_df, batch_id):
    """
    Takes the transformed batch (silver data) and merges it into the 
    target Silver Delta table.
    """
    print(f"Upserting batch {batch_id} to {CUSTOMER_SILVER_TABLE_PATH}...")
    
    # Get the spark session from the batch_df
    spark = batch_df.sparkSession

    # Apply the transformations
    silver_df = transform_bronze_to_silver(batch_df, spark)

    # Define the Primary Key for merging
    PK_COL = "customer_id"
    MERGE_CONDITION = f"target.{PK_COL} = source.{PK_COL}"

    # Initialize DeltaTable if it doesn't exist
    if not DeltaTable.isDeltaTable(spark, CUSTOMER_SILVER_TABLE_PATH):
        print(f"Creating new Silver table at: {CUSTOMER_SILVER_TABLE_PATH}")
        silver_df.write.format("delta").mode("overwrite").save(CUSTOMER_SILVER_TABLE_PATH)
    
    # Merge the batch into the Silver table
    delta_table = DeltaTable.forPath(spark, CUSTOMER_SILVER_TABLE_PATH)
    
    (
        delta_table.alias("target")
        .merge(silver_df.alias("source"), MERGE_CONDITION)
        .whenMatchedUpdateAll() # Update all columns if PK matches
        .whenNotMatchedInsertAll() # Insert new row if PK doesn't match
        .execute()
    )
    print(f"Batch {batch_id} complete.")


# --- 4. Main Stream Execution ---
if __name__ == "__main__":
    TABLE_NAME = "customer"

    spark = SparkSession.builder \
        .appName(f"Bronze_to_Silver_{TABLE_NAME}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print(f"Starting stream from Bronze table: {CUSTOMER_BRONZE_TABLE_PATH}")

    # Read the Bronze table as a stream
    bronze_stream_df = (
        spark.readStream
        .format("delta")
        .load(CUSTOMER_BRONZE_TABLE_PATH)
    )

    # Write the stream to Silver using our upsert function
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(upsert_to_silver)
        .outputMode("update")
        .option("checkpointLocation", CUSTOMER_SILVER_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()