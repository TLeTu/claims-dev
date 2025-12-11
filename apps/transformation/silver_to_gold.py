import os
import random
from typing import Iterator

import pandas as pd
import geopy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, avg

# --- Configuration ---
APP_NAME = "Transformation_Silver_to_Gold"
BASE_PATH = "s3a://car-smart-claims"
SILVER_BASE = os.path.join(BASE_PATH, "silver")
GOLD_BASE = os.path.join(BASE_PATH, "gold")

# Sources
PATH_SILVER_POLICY = os.path.join(SILVER_BASE, "policy")
PATH_SILVER_CLAIM = os.path.join(SILVER_BASE, "claim")
PATH_SILVER_CUSTOMER = os.path.join(SILVER_BASE, "customer")
PATH_SILVER_TELEMATICS = os.path.join(SILVER_BASE, "telematics")

# Sinks
PATH_GOLD_AGG_TELEMATICS = os.path.join(GOLD_BASE, "aggregated_telematics")
PATH_GOLD_CUST_CLAIM_POL = os.path.join(GOLD_BASE, "customer_claim_policy")
PATH_GOLD_ENRICHED = os.path.join(GOLD_BASE, "customer_claim_policy_telematics")

# --- Geocoding Logic ---

def geocode(geolocator, address):
    try:
        # Mocked response for demo environment to prevent API rate limits
        return pd.Series({'latitude':  random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)})
        
        # Production Logic:
        # location = geolocator.geocode(address)
        # if location:
        #     return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
        pass
    return pd.Series({'latitude': None, 'longitude': None})

# --- Transformations ---

def create_aggregated_telematics(spark: SparkSession):
    print(f"Aggregating telematics to {PATH_GOLD_AGG_TELEMATICS}...")
    
    (
        spark.read.format("delta").load(PATH_SILVER_TELEMATICS)
        .groupBy("chassis_no")
        .agg(
            avg("speed").alias("avg_speed"),
            avg("latitude").alias("avg_latitude"),
            avg("longitude").alias("avg_longitude"),
        )
        .write.format("delta").mode("overwrite").save(PATH_GOLD_AGG_TELEMATICS)
    )

def create_customer_claim_policy(spark: SparkSession):
    print(f"Joining core business tables to {PATH_GOLD_CUST_CLAIM_POL}...")

    df_policy = spark.read.format("delta").load(PATH_SILVER_POLICY)
    df_claim = spark.read.format("delta").load(PATH_SILVER_CLAIM)
    df_customer = spark.read.format("delta").load(PATH_SILVER_CUSTOMER)

    (
        df_claim
        .join(df_policy, "policy_no", "inner")
        .join(df_customer, df_policy.cust_id == df_customer.customer_id, "inner")
        .drop(df_customer.customer_id)
        .write.format("delta").mode("overwrite").save(PATH_GOLD_CUST_CLAIM_POL)
    )

def create_enriched_gold(spark: SparkSession, geocoding_udf):
    print(f"Creating final enriched view at {PATH_GOLD_ENRICHED}...")

    df_telematics = spark.read.format("delta").load(PATH_GOLD_AGG_TELEMATICS)
    df_core = spark.read.format("delta").load(PATH_GOLD_CUST_CLAIM_POL)

    (
        df_core
        .withColumn("lat_long", geocoding_udf(col("address")))
        .join(df_telematics, on="chassis_no", how="inner")
        .write.format("delta").mode("overwrite").save(PATH_GOLD_ENRICHED)
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

    # Register UDF
    @pandas_udf("latitude float, longitude float")
    def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
        geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https')
        for address_batch in batch_iter:
            yield address_batch.apply(lambda x: geocode(geolocator, x))

    # Execute Batch Jobs
    create_aggregated_telematics(spark)
    create_customer_claim_policy(spark)
    create_enriched_gold(spark, get_lat_long)

    print("Silver-to-Gold transformations complete.")