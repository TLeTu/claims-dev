import os
import random
from typing import Iterator

import pandas as pd
import geopy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, avg

# =============================================================================
# --- 1. Configuration & Path Definitions ---
# =============================================================================

# --- Base Paths ---
SILVER_BASE_PATH = "/opt/spark/bucket/silver"
GOLD_BASE_PATH = "/opt/spark/bucket/gold"

# --- Silver Table Paths ---
POLICY_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "policy")
CLAIM_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "claim")
CUSTOMER_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "customer")
TELEMATICS_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "telematics")

# --- Gold Table Paths ---
AGG_TELEMATICS_GOLD_PATH = os.path.join(GOLD_BASE_PATH, "aggregated_telematics")
CUSTOMER_CLAIM_POLICY_GOLD_PATH = os.path.join(GOLD_BASE_PATH, "customer_claim_policy")
CUSTOMER_CLAIM_POLICY_TELEMATICS_GOLD_PATH = os.path.join(GOLD_BASE_PATH, "customer_claim_policy_telematics")


# =============================================================================
# --- 2. Geocoding UDF Definitions ---
# =============================================================================

def geocode(geolocator, address):
    """
    Geocodes a single address string to latitude and longitude.
    Includes a mocked response for demonstration purposes.
    """
    try:
        # For demonstration, we use a mocked response to avoid hitting the real API with too many requests.
        return pd.Series({'latitude':  random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)})
        location = geolocator.geocode(address)
        if location:
            return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
        print(f"error getting lat/long: {e}")
    return pd.Series({'latitude': None, 'longitude': None})


# =============================================================================
# --- 3. Gold Layer Transformation Functions ---
# =============================================================================

def create_aggregated_telematics_gold(spark: SparkSession):
    """
    Reads the Silver telematics table, calculates average speed and location
    per vehicle, and writes the result to a Gold Delta table.
    """
    print("\nStarting Gold transformation: Aggregating telematics data...")
    
    # Read the source Silver table
    telematics_silver_df = spark.read.format("delta").load(TELEMATICS_SILVER_PATH)

    # Apply the aggregation logic
    aggregated_df = (
        telematics_silver_df
        .groupBy("chassis_no")
        .agg(
            avg("speed").alias("avg_speed"),
            avg("latitude").alias("avg_latitude"),
            avg("longitude").alias("avg_longitude"),
        )
    )

    # Write the aggregated data to the Gold layer, overwriting previous versions
    # For a full summary table like this, 'overwrite' is the standard and safest mode.
    # It ensures the table is always a complete and accurate reflection of the source data
    # and makes the job idempotent (re-runnable).
    print(f"Writing aggregated telematics data to Gold table: {AGG_TELEMATICS_GOLD_PATH}")
    aggregated_df.write.format("delta").mode("overwrite").save(AGG_TELEMATICS_GOLD_PATH)
    print("Finished writing aggregated telematics Gold table.")

def create_customer_claim_policy_gold(spark: SparkSession):
    """
    Creates a denormalized Gold table by joining claim, policy, and customer data.
    This provides a unified view for analytics.
    """
    print("\nStarting Gold transformation: Creating customer_claim_policy table...")

    # 1. Read the source Silver tables
    print("Reading Silver tables: claim, policy, customer")
    policy_df = spark.read.format("delta").load(POLICY_SILVER_PATH)
    claim_df = spark.read.format("delta").load(CLAIM_SILVER_PATH)
    customer_df = spark.read.format("delta").load(CUSTOMER_SILVER_PATH)

    # 2. Join claim and policy tables
    # The claim table is the primary fact table here, so we start with it.
    claim_policy_df = claim_df.join(policy_df, "policy_no", "inner")

    # 3. Join the result with the customer table
    # The join condition explicitly compares the customer ID from the two sources.
    # We use an inner join to ensure we only have claims with valid, linked customers.
    final_df = claim_policy_df.join(
        customer_df,
        claim_policy_df.cust_id == customer_df.customer_id,
        "inner"
    ).drop(customer_df.customer_id) # Drop the redundant customer_id column

    # 4. Write the final denormalized table to the Gold layer
    # 'overwrite' is used to completely rebuild this analytical table in each batch run.
    print(f"Writing customer_claim_policy data to Gold table: {CUSTOMER_CLAIM_POLICY_GOLD_PATH}")
    final_df.write.format("delta").mode("overwrite").save(CUSTOMER_CLAIM_POLICY_GOLD_PATH)
    print("Finished writing customer_claim_policy Gold table.")

def create_customer_claim_policy_telematics_gold(spark: SparkSession, geocoding_udf):
    """
    Creates a final enriched Gold table by joining the customer_claim_policy
    table with aggregated telematics data and geocoded locations.
    """
    print("\nStarting Gold transformation: Creating customer_claim_policy_telematics table...")

    # 1. Read the prerequisite Gold tables
    print(f"Reading from: {AGG_TELEMATICS_GOLD_PATH}")
    telematics_df = spark.read.format("delta").load(AGG_TELEMATICS_GOLD_PATH)

    print(f"Reading from: {CUSTOMER_CLAIM_POLICY_GOLD_PATH}")
    customer_claim_policy_df = spark.read.format("delta").load(CUSTOMER_CLAIM_POLICY_GOLD_PATH)

    # 2. Enrich the customer_claim_policy data with geocoded locations
    # The UDF 'get_lat_long' is applied to the address column.
    print("Applying geocoding UDF to customer addresses...")
    enriched_df = customer_claim_policy_df.withColumn(
        "lat_long", geocoding_udf(col("address"))
    )

    # 3. Join the enriched data with the aggregated telematics data
    # An inner join ensures we only include records that have matching telematics.
    print("Joining with aggregated telematics data...")
    final_df = enriched_df.join(telematics_df, on="chassis_no", how="inner")

    # 4. Write the final table to the Gold layer
    print(f"Writing final enriched data to Gold table: {CUSTOMER_CLAIM_POLICY_TELEMATICS_GOLD_PATH}")
    final_df.write.format("delta").mode("overwrite").save(CUSTOMER_CLAIM_POLICY_TELEMATICS_GOLD_PATH)
    print("Finished writing customer_claim_policy_telematics Gold table.")


# =============================================================================
# --- 4. Main Execution ---
# =============================================================================

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Silver_to_Gold_Transformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("Spark session created. Starting Silver-to-Gold transformations...")

    # =========================================================================
    # --- Execute Gold Transformations ---
    # =========================================================================
    
    # Define the Pandas UDF *after* the SparkSession has been created.
    @pandas_udf("latitude float, longitude float")
    def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
        """
        Pandas UDF to apply the geocoding function to a batch of addresses.
        """
        # This function runs on the Spark executors.
        geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https' )
        for address_batch in batch_iter:
            yield address_batch.apply(lambda x: geocode(geolocator, x))

    # Call the function to create the aggregated telematics Gold table
    create_aggregated_telematics_gold(spark)

    # Call the function to create the denormalized claims and policy table
    create_customer_claim_policy_gold(spark)

    # Call the function to create the final enriched table, passing the UDF in.
    create_customer_claim_policy_telematics_gold(spark, get_lat_long)

    print("\nAll Silver-to-Gold transformations are complete.")
    spark.stop()