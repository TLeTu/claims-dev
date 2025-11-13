import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, lit, avg
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable

# --- 1. Define Paths ---
BRONZE_BASE_PATH = "/opt/spark/bucket/bronze" # Path from ingestion scripts
SILVER_BASE_PATH = "/opt/spark/bucket/silver"

BRONZE_TABLE_PATH = os.path.join(BRONZE_BASE_PATH, "telematics")
SILVER_TABLE_PATH = os.path.join(SILVER_BASE_PATH, "telematics")
SILVER_CHECKPOINT = os.path.join(SILVER_BASE_PATH, "telematics", "_checkpoints")

# --- 2. Define the Transformation Logic ---
def transform_bronze_to_silver(bronze_df, spark):
    """
    Applies all cleaning, validation, and enrichment logic for ONE table.
    This is where you'll do 90% of your work.
    """
    print("Applying Silver transformations to telematics data...")

    # Base transformations that are always applied
    base_df = (
        bronze_df
        # --- Validation: Drop rows where any part of the composite PK is null ---
        .dropna(subset=["chassis_no", "event_timestamp"])
        # --- Deduplication: Ensure each PK is unique within the batch before merging ---
        .dropDuplicates(["chassis_no", "event_timestamp"])
        # --- Cleaning: Cast to correct data types ---
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("latitude_cast", col("latitude").cast(DoubleType()))
        .withColumn("longitude_cast", col("longitude").cast(DoubleType()))
        # --- Validation: Clamp coordinates to their valid ranges ---
        .withColumn("latitude", 
            when(col("latitude_cast") < -90.0, lit(-90.0))
            .when(col("latitude_cast") > 90.0, lit(90.0))
            .otherwise(col("latitude_cast"))
        )
        .withColumn("longitude", 
            when(col("longitude_cast") < -180.0, lit(-180.0))
            .when(col("longitude_cast") > 180.0, lit(180.0))
            .otherwise(col("longitude_cast"))
        )
    )

    # --- Imputation: Calculate historical average speed for imputation ---
    # We do this before the main transformation chain.
    # This step is conditional because the silver table might not exist on the first run.
    if DeltaTable.isDeltaTable(spark, SILVER_TABLE_PATH):
        print("Silver table exists. Calculating average speed for imputation.")
        silver_telematics_df = DeltaTable.forPath(spark, SILVER_TABLE_PATH).toDF()

        # Calculate the average speed for each chassis_no from historical data
        avg_speed_per_chassis = (
            silver_telematics_df
            .groupBy("chassis_no")
            .agg(avg("speed").alias("avg_chassis_speed"))
            .where(col("avg_chassis_speed").isNotNull()) # Only use chassis with valid historical speed
        )

        # Join the incoming batch with the calculated averages
        # Use a left join to ensure all new records are kept
        df_with_avg = base_df.join(
            avg_speed_per_chassis,
            "chassis_no",
            "left"
        )

        # Apply imputation logic
        silver_df = df_with_avg.withColumn("speed_cast", col("speed").cast(DoubleType())) \
            .withColumn("speed",
                when(col("speed_cast").isNotNull(), col("speed_cast"))
                .when(col("avg_chassis_speed").isNotNull(), col("avg_chassis_speed"))
                .otherwise(lit(None))
            ).drop("speed_cast", "avg_chassis_speed")

    else:
        print("Silver table does not exist yet. Skipping speed imputation for the first batch.")
        # For the first batch, just cast the speed column without imputation
        silver_df = base_df.withColumn("speed", col("speed").cast(DoubleType()))

    # Final cleanup of temporary columns
    final_df = silver_df.drop("latitude_cast", "longitude_cast")

    return final_df

# --- 3. Define the `foreachBatch` Merge Function ---
def upsert_to_silver(batch_df, batch_id):
    """
    Takes the transformed batch (silver data) and merges it into the 
    target Silver Delta table.
    """
    print(f"Upserting batch {batch_id} to {SILVER_TABLE_PATH}...")
    
    # Get the spark session from the batch_df
    spark = batch_df.sparkSession

    # Apply the transformations
    silver_df = transform_bronze_to_silver(batch_df, spark)

    # Define the Composite Primary Key for merging
    PK_COLS = ["chassis_no", "event_timestamp"]
    # Build the merge condition string dynamically from the PK columns
    MERGE_CONDITION = " AND ".join([f"target.{pk} = source.{pk}" for pk in PK_COLS])

    # Initialize DeltaTable if it doesn't exist
    if not DeltaTable.isDeltaTable(spark, SILVER_TABLE_PATH):
        print(f"Creating new Silver table at: {SILVER_TABLE_PATH}")
        silver_df.write.format("delta").mode("overwrite").save(SILVER_TABLE_PATH)
    
    # Merge the batch into the Silver table
    delta_table = DeltaTable.forPath(spark, SILVER_TABLE_PATH)
    
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
    TABLE_NAME = "telematics"

    spark = SparkSession.builder \
        .appName(f"Bronze_to_Silver_{TABLE_NAME}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print(f"Starting stream from Bronze table: {BRONZE_TABLE_PATH}")

    # Read the Bronze table as a stream
    bronze_stream_df = (
        spark.readStream
        .format("delta")
        .load(BRONZE_TABLE_PATH)
    )

    # Write the stream to Silver using our upsert function
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(upsert_to_silver)
        .outputMode("update")
        .option("checkpointLocation", SILVER_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()