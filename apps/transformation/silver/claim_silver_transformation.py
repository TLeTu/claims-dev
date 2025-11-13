import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_add, when
from pyspark.sql.types import IntegerType, BooleanType, LongType
from delta.tables import DeltaTable

# --- 1. Define Paths ---
BRONZE_BASE_PATH = "/opt/spark/bucket/bronze/database" # Path from cdc_stream_ingestion.py
SILVER_BASE_PATH = "/opt/spark/bucket/silver"

# --- Paths for the 'claim' table ---
CLAIM_BRONZE_TABLE_PATH = os.path.join(BRONZE_BASE_PATH, "claim")
CLAIM_SILVER_TABLE_PATH = os.path.join(SILVER_BASE_PATH, "claim")
CLAIM_SILVER_CHECKPOINT = os.path.join(SILVER_BASE_PATH, "claim", "_checkpoints")

# --- 2. Define the Transformation Logic ---
def transform_bronze_to_silver(bronze_df, spark):
    """
    Applies all cleaning, validation, and enrichment logic for ONE table.
    This is where you'll do 90% of your work.
    """
    print(f"Applying Silver transformations to {TABLE_NAME}...")
    
    # Transformations for 'claim' table:
    silver_df = (
        bronze_df
        .dropna(subset=["claim_no"])
        .dropDuplicates(["claim_no"])
        # --- Cleaning: Convert dates from days-since-epoch (integer) to DateType ---
        # The date '1970-01-01' is the epoch start date.
        # We first cast the string from bronze to an integer, then add it to the epoch date.
        .withColumn("claim_date", date_add(lit("1970-01-01"), col("claim_date").cast(IntegerType())))
        .withColumn("license_issue_date", date_add(lit("1970-01-01"), col("license_issue_date").cast(IntegerType())))
        .withColumn("incident_date", date_add(lit("1970-01-01"), col("incident_date").cast(IntegerType())))
        # --- Cleaning: Cast to correct numeric and boolean types ---
        .withColumn("months_as_customer", col("months_as_customer").cast(IntegerType()))
        .withColumn("injury", col("injury").cast(LongType()))
        .withColumn("property", col("property").cast(LongType()))
        .withColumn("vehicle", col("vehicle").cast(LongType()))
        .withColumn("total", col("total").cast(LongType()))
        .withColumn("number_of_vehicles_involved", col("number_of_vehicles_involved").cast(IntegerType()))
        .withColumn("driver_age", col("driver_age").cast(IntegerType()))
        .withColumn("incident_hour_cast", col("incident_hour").cast(IntegerType()))
        .withColumn("number_of_witnesses", col("number_of_witnesses").cast(IntegerType()))
        .withColumn("suspicious_activity", col("suspicious_activity").cast(BooleanType()))
        .withColumn("incident_hour",
                    when(col("incident_hour_cast") < 0, lit(0))
                    .when(col("incident_hour_cast") > 23, lit(23))
                    .otherwise(col("incident_hour_cast"))
        )
        .drop("incident_hour_cast")
    )

    return silver_df

# --- 3. Define the `foreachBatch` Merge Function ---
def upsert_to_silver(batch_df, batch_id):
    """
    Takes the transformed batch (silver data) and merges it into the 
    target Silver Delta table.
    """
    print(f"Upserting batch {batch_id} to {CLAIM_SILVER_TABLE_PATH}...")
    
    # Get the spark session from the batch_df
    spark = batch_df.sparkSession

    # Apply the transformations
    silver_df = transform_bronze_to_silver(batch_df, spark)

    # Define the Primary Key for merging
    PK_COL = "claim_no"
    MERGE_CONDITION = f"target.{PK_COL} = source.{PK_COL}"

    # Initialize DeltaTable if it doesn't exist
    if not DeltaTable.isDeltaTable(spark, CLAIM_SILVER_TABLE_PATH):
        print(f"Creating new Silver table at: {CLAIM_SILVER_TABLE_PATH}")
        silver_df.write.format("delta").mode("overwrite").save(CLAIM_SILVER_TABLE_PATH)
    
    # Merge the batch into the Silver table
    delta_table = DeltaTable.forPath(spark, CLAIM_SILVER_TABLE_PATH)
    
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
    TABLE_NAME = "claim"

    spark = SparkSession.builder \
        .appName(f"Bronze_to_Silver_{TABLE_NAME}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print(f"Starting stream from Bronze table: {CLAIM_BRONZE_TABLE_PATH}")

    # Read the Bronze table as a stream
    bronze_stream_df = (
        spark.readStream
        .format("delta")
        .load(CLAIM_BRONZE_TABLE_PATH)
    )

    # Write the stream to Silver using our upsert function
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(upsert_to_silver)
        .outputMode("update")
        .option("checkpointLocation", CLAIM_SILVER_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()