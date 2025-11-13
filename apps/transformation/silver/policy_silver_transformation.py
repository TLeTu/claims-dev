import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, date_add, abs
from pyspark.sql.types import IntegerType, DecimalType
from delta.tables import DeltaTable

# --- 1. Define Paths ---
BRONZE_BASE_PATH = "/opt/spark/bucket/bronze/database" # Path from cdc_stream_ingestion.py
SILVER_BASE_PATH = "/opt/spark/bucket/silver"

# --- Example for the 'policies' table ---
POLICY_BRONZE_TABLE_PATH = os.path.join(BRONZE_BASE_PATH, "policy")
POLICY_SILVER_TABLE_PATH = os.path.join(SILVER_BASE_PATH, "policy")
POLICY_SILVER_CHECKPOINT = os.path.join(SILVER_BASE_PATH, "policy", "_checkpoints")

# --- 2. Define the Transformation Logic ---
def transform_bronze_to_silver(bronze_df, spark):
    """
    Applies all cleaning, validation, and enrichment logic for ONE table.
    This is where you'll do 90% of your work.
    """
    print(f"Applying Silver transformations to {TABLE_NAME}...")
    
    # Example for 'policies' table:
    silver_df = (
        bronze_df
        .dropna(subset=["policy_no", "cust_id"])
        .dropDuplicates(["policy_no"])
        .withColumn("cust_id", col("cust_id").cast(IntegerType()))
        # --- Cleaning: Convert dates from days-since-epoch (integer) to DateType ---
        # The date '1970-01-01' is the epoch start date.
        # We first cast the string from bronze to an integer, then add it to the epoch date.
        .withColumn("pol_issue_date", date_add(lit("1970-01-01"), col("pol_issue_date").cast(IntegerType())))
        .withColumn("pol_eff_date", date_add(lit("1970-01-01"), col("pol_eff_date").cast(IntegerType())))
        .withColumn("pol_expiry_date", date_add(lit("1970-01-01"), col("pol_expiry_date").cast(IntegerType())))
        .withColumn("model_year", col("model_year").cast(IntegerType()))
        .withColumn("sum_insured", col("sum_insured").cast(DecimalType(10, 2)))
        .withColumn("premium", abs(col("premium").cast(DecimalType(10, 2))))
        .withColumn("deductable", col("deductable").cast(IntegerType()))
        # --- Validation: Handle bad data ---
        .withColumn("model_year", when(col("model_year") < 1980, lit(None)).otherwise(col("model_year")))
    )
    
    # --- Enrichment: Join with other tables ---
    # Example: If you wanted to add customer details to the policy table
    # 1. Load the 'customers' table as a STATIC dataframe
    # try:
    #     customer_static_df = spark.read.format("delta").load(os.path.join(SILVER_BASE_PATH, "customer"))
    #     # 2. Join the stream (silver_df) with the static table
    #     silver_df = silver_df.join(
    #         customer_static_df.alias("cust"),
    #         silver_df.cust_id == col("cust.cust_id"),
    #         "left_outer"
    #     ).select(silver_df."*", col("cust.name").alias("customer_name"))
    # except Exception as e:
    #     print(f"Customer table not yet available for enrichment: {e}")

    return silver_df

# --- 3. Define the `foreachBatch` Merge Function ---
def upsert_to_silver(batch_df, batch_id):
    """
    Takes the transformed batch (silver data) and merges it into the 
    target Silver Delta table.
    """
    print(f"Upserting batch {batch_id} to {POLICY_SILVER_TABLE_PATH}...")
    
    # Get the spark session from the batch_df
    spark = batch_df.sparkSession

    # Apply the transformations
    silver_df = transform_bronze_to_silver(batch_df, spark)

    # Define the Primary Key for merging
    PK_COL = "policy_no" # Change this for each table!
    MERGE_CONDITION = f"target.{PK_COL} = source.{PK_COL}"

    # Initialize DeltaTable if it doesn't exist
    if not DeltaTable.isDeltaTable(spark, POLICY_SILVER_TABLE_PATH):
        print(f"Creating new Silver table at: {POLICY_SILVER_TABLE_PATH}")
        silver_df.write.format("delta").mode("overwrite").save(POLICY_SILVER_TABLE_PATH)
    
    # Merge the batch into the Silver table
    delta_table = DeltaTable.forPath(spark, POLICY_SILVER_TABLE_PATH)
    
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
    TABLE_NAME = "policy"

    spark = SparkSession.builder \
        .appName(f"Bronze_to_Silver_{TABLE_NAME}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print(f"Starting stream from Bronze table: {POLICY_BRONZE_TABLE_PATH}")

    # Read the Bronze table as a stream
    bronze_stream_df = (
        spark.readStream
        .format("delta")
        .load(POLICY_BRONZE_TABLE_PATH)
    )

    # Write the stream to Silver using our upsert function
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(upsert_to_silver)
        .outputMode("update")
        .option("checkpointLocation", POLICY_SILVER_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()