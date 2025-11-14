import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, date_add, abs, regexp_extract, split, concat_ws, to_timestamp, avg
from pyspark.sql.types import IntegerType, DecimalType, BooleanType, LongType, DoubleType
from delta.tables import DeltaTable

# =============================================================================
# --- 1. Configuration & Path Definitions ---
# =============================================================================

# --- Base Paths ---
BRONZE_BASE_PATH = "s3a://car-smart-claims/bronze"
SILVER_BASE_PATH = "s3a://car-smart-claims/silver"

# --- Policy Table Paths ---
POLICY_BRONZE_PATH = os.path.join(BRONZE_BASE_PATH, "database/policy")
POLICY_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "policy")
POLICY_CHECKPOINT = os.path.join(POLICY_SILVER_PATH, "_checkpoints")

# --- Claim Table Paths ---
CLAIM_BRONZE_PATH = os.path.join(BRONZE_BASE_PATH, "database/claim")
CLAIM_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "claim")
CLAIM_CHECKPOINT = os.path.join(CLAIM_SILVER_PATH, "_checkpoints")

# --- Customer Table Paths ---
CUSTOMER_BRONZE_PATH = os.path.join(BRONZE_BASE_PATH, "database/customer")
CUSTOMER_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "customer")
CUSTOMER_CHECKPOINT = os.path.join(CUSTOMER_SILVER_PATH, "_checkpoints")

# --- Claim Images Table Paths ---
CLAIM_IMAGES_BRONZE_PATH = os.path.join(BRONZE_BASE_PATH, "claim_images")
CLAIM_IMAGES_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "claim_images")
CLAIM_IMAGES_CHECKPOINT = os.path.join(CLAIM_IMAGES_SILVER_PATH, "_checkpoints")

# --- Image Metadata Table Paths ---
IMAGE_METADATA_BRONZE_PATH = os.path.join(BRONZE_BASE_PATH, "image_metadata")
IMAGE_METADATA_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "image_metadata")
IMAGE_METADATA_CHECKPOINT = os.path.join(IMAGE_METADATA_SILVER_PATH, "_checkpoints")

# --- Training Images Table Paths ---
TRAINING_IMAGES_BRONZE_PATH = os.path.join(BRONZE_BASE_PATH, "training_images")
TRAINING_IMAGES_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "training_images")
TRAINING_IMAGES_CHECKPOINT = os.path.join(TRAINING_IMAGES_SILVER_PATH, "_checkpoints")

# --- Telematics Table Paths ---
TELEMATICS_BRONZE_PATH = os.path.join(BRONZE_BASE_PATH, "telematics")
TELEMATICS_SILVER_PATH = os.path.join(SILVER_BASE_PATH, "telematics")
TELEMATICS_CHECKPOINT = os.path.join(TELEMATICS_SILVER_PATH, "_checkpoints")


# =============================================================================
# --- 2. Transformation Logic ---
# Each function contains the specific logic for one table.
# =============================================================================

def transform_policy(bronze_df, spark):
    """Applies Silver transformations to the 'policy' table."""
    print("Applying Silver transformations to policy...")
    return (
        bronze_df
        .dropna(subset=["policy_no", "cust_id"])
        .dropDuplicates(["policy_no"])
        .withColumn("cust_id", col("cust_id").cast(IntegerType()))
        .withColumn("pol_issue_date", date_add(lit("1970-01-01"), col("pol_issue_date").cast(IntegerType())))
        .withColumn("pol_eff_date", date_add(lit("1970-01-01"), col("pol_eff_date").cast(IntegerType())))
        .withColumn("pol_expiry_date", date_add(lit("1970-01-01"), col("pol_expiry_date").cast(IntegerType())))
        .withColumn("model_year", col("model_year").cast(IntegerType()))
        .withColumn("sum_insured", col("sum_insured").cast(DecimalType(10, 2)))
        .withColumn("premium", abs(col("premium").cast(DecimalType(10, 2))))
        .withColumn("deductable", col("deductable").cast(IntegerType()))
        .withColumn("model_year", when(col("model_year") < 1980, lit(None)).otherwise(col("model_year")))
    )

def transform_claim(bronze_df, spark):
    """Applies Silver transformations to the 'claim' table."""
    print("Applying Silver transformations to claim...")
    return (
        bronze_df
        .dropna(subset=["claim_no"])
        .dropDuplicates(["claim_no"])
        .withColumn("claim_date", date_add(lit("1970-01-01"), col("claim_date").cast(IntegerType())))
        .withColumn("license_issue_date", date_add(lit("1970-01-01"), col("license_issue_date").cast(IntegerType())))
        .withColumn("incident_date", date_add(lit("1970-01-01"), col("incident_date").cast(IntegerType())))
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

def transform_customer(bronze_df, spark):
    """Applies Silver transformations to the 'customer' table."""
    print("Applying Silver transformations to customer...")
    return (
        bronze_df
        .dropna(subset=["customer_id"])
        .dropDuplicates(["customer_id"])
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .withColumn("date_of_birth", date_add(lit("1970-01-01"), col("date_of_birth").cast(IntegerType())))
        .withColumn("firstname", split(col("name"), " ").getItem(0))
        .withColumn("lastname", split(col("name"), " ").getItem(1))
        .withColumn("address", concat_ws(", ", col("borough"), col("zip_code")))
        .drop("name", "borough", "zip_code")
    )

def transform_claim_images(bronze_df, spark):
    """Applies Silver transformations to the 'claim_images' data."""
    print("Applying Silver transformations to claim_images...")
    return (
        bronze_df
        .withColumn("image_name", regexp_extract(col("path"), r".*/(.*?.jpg)", 1))
    )

def transform_image_metadata(bronze_df, spark):
    """Applies Silver transformations to the 'image_metadata' data."""
    print("Applying Silver transformations to image_metadata...")
    return (
        bronze_df
        .dropna(subset=["chassis_no", "claim_no"])
        .dropDuplicates(["image_name"])
    )

def transform_training_images(bronze_df, spark):
    """Applies Silver transformations to the 'training_images' data."""
    print("Applying Silver transformations to training_images...")
    return (
        bronze_df
        .withColumn("label", regexp_extract("path", r"/(\d+)-([a-zA-Z]+)(?: \(\d+\))?\.png$", 2))
    )

def transform_telematics(bronze_df, spark):
    """Applies Silver transformations to the 'telematics' data."""
    print("Applying Silver transformations to telematics...")
    base_df = (
        bronze_df
        .dropna(subset=["chassis_no", "event_timestamp"])
        .dropDuplicates(["chassis_no", "event_timestamp"])
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("latitude_cast", col("latitude").cast(DoubleType()))
        .withColumn("longitude_cast", col("longitude").cast(DoubleType()))
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

    if DeltaTable.isDeltaTable(spark, TELEMATICS_SILVER_PATH):
        print("Telematics silver table exists. Calculating average speed for imputation.")
        silver_telematics_df = DeltaTable.forPath(spark, TELEMATICS_SILVER_PATH).toDF()

        avg_speed_per_chassis = (
            silver_telematics_df
            .groupBy("chassis_no")
            .agg(avg("speed").alias("avg_chassis_speed"))
            .where(col("avg_chassis_speed").isNotNull())
        )

        df_with_avg = base_df.join(avg_speed_per_chassis, "chassis_no", "left")

        silver_df = df_with_avg.withColumn("speed_cast", col("speed").cast(DoubleType())) \
            .withColumn("speed",
                when(col("speed_cast").isNotNull(), col("speed_cast"))
                .when(col("avg_chassis_speed").isNotNull(), col("avg_chassis_speed"))
                .otherwise(lit(None))
            ).drop("speed_cast", "avg_chassis_speed")
    else:
        print("Telematics silver table does not exist yet. Skipping speed imputation.")
        silver_df = base_df.withColumn("speed", col("speed").cast(DoubleType()))

    final_df = silver_df.drop("latitude_cast", "longitude_cast")
    return final_df


# =============================================================================
# --- 3. Generic `foreachBatch` Upsert Function ---
# =============================================================================

def upsert_to_silver(batch_df, batch_id, silver_path, pk_cols, transform_func, table_name):
    """
    A generic function to transform and merge a micro-batch into a Silver Delta table.
    
    :param batch_df: The micro-batch DataFrame from the stream.
    :param batch_id: The ID of the micro-batch.
    :param silver_path: The path to the target Silver Delta table.
    :param pk_cols: A list of primary key column names for the merge.
    :param transform_func: The specific transformation function to apply to the batch.
    :param table_name: The name of the table being processed (for logging).
    """
    print(f"Upserting batch {batch_id} to {table_name} at {silver_path}...")
    
    spark = batch_df.sparkSession

    # Apply the specific transformations for this table
    silver_df = transform_func(batch_df, spark)

    # Build the merge condition dynamically from the PK columns
    merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

    # Initialize DeltaTable if it doesn't exist, otherwise merge
    if not DeltaTable.isDeltaTable(spark, silver_path):
        print(f"Creating new Silver table for {table_name} at: {silver_path}")
        silver_df.write.format("delta").mode("overwrite").save(silver_path)
    else:
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(silver_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    print(f"Batch {batch_id} for {table_name} complete.")


# =============================================================================
# --- 4. Stream Starting Functions ---
# =============================================================================

def start_policy_stream(spark):
    """Starts the streaming query for the 'policy' table."""
    print(f"Starting stream from Bronze policy table: {POLICY_BRONZE_PATH}")
    bronze_stream_df = spark.readStream.format("delta").load(POLICY_BRONZE_PATH)
    
    # Using a lambda to pass extra arguments to the foreachBatch function
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(lambda df, id: upsert_to_silver(df, id, POLICY_SILVER_PATH, ["policy_no"], transform_policy, "policy"))
        .outputMode("update")
        .option("checkpointLocation", POLICY_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )
    return query

def start_claim_stream(spark):
    """Starts the streaming query for the 'claim' table."""
    print(f"Starting stream from Bronze claim table: {CLAIM_BRONZE_PATH}")
    bronze_stream_df = spark.readStream.format("delta").load(CLAIM_BRONZE_PATH)
    
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(lambda df, id: upsert_to_silver(df, id, CLAIM_SILVER_PATH, ["claim_no"], transform_claim, "claim"))
        .outputMode("update")
        .option("checkpointLocation", CLAIM_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )
    return query

def start_customer_stream(spark):
    """Starts the streaming query for the 'customer' table."""
    print(f"Starting stream from Bronze customer table: {CUSTOMER_BRONZE_PATH}")
    bronze_stream_df = spark.readStream.format("delta").load(CUSTOMER_BRONZE_PATH)
    
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(lambda df, id: upsert_to_silver(df, id, CUSTOMER_SILVER_PATH, ["customer_id"], transform_customer, "customer"))
        .outputMode("update")
        .option("checkpointLocation", CUSTOMER_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )
    return query

def start_claim_images_stream(spark):
    """Starts the streaming query for the 'claim_images' table."""
    print(f"Starting stream from Bronze claim_images table: {CLAIM_IMAGES_BRONZE_PATH}")
    bronze_stream_df = spark.readStream.format("delta").load(CLAIM_IMAGES_BRONZE_PATH)
    
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(lambda df, id: upsert_to_silver(df, id, CLAIM_IMAGES_SILVER_PATH, ["path"], transform_claim_images, "claim_images"))
        .outputMode("update")
        .option("checkpointLocation", CLAIM_IMAGES_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )
    return query

def start_image_metadata_stream(spark):
    """Starts the streaming query for the 'image_metadata' table."""
    print(f"Starting stream from Bronze image_metadata table: {IMAGE_METADATA_BRONZE_PATH}")
    bronze_stream_df = spark.readStream.format("delta").load(IMAGE_METADATA_BRONZE_PATH)
    
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(lambda df, id: upsert_to_silver(df, id, IMAGE_METADATA_SILVER_PATH, ["image_name"], transform_image_metadata, "image_metadata"))
        .outputMode("update")
        .option("checkpointLocation", IMAGE_METADATA_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )
    return query

def start_training_images_stream(spark):
    """Starts the streaming query for the 'training_images' table."""
    print(f"Starting stream from Bronze training_images table: {TRAINING_IMAGES_BRONZE_PATH}")
    bronze_stream_df = spark.readStream.format("delta").load(TRAINING_IMAGES_BRONZE_PATH)
    
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(lambda df, id: upsert_to_silver(df, id, TRAINING_IMAGES_SILVER_PATH, ["path"], transform_training_images, "training_images"))
        .outputMode("update")
        .option("checkpointLocation", TRAINING_IMAGES_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )
    return query

def start_telematics_stream(spark):
    """Starts the streaming query for the 'telematics' table."""
    print(f"Starting stream from Bronze telematics table: {TELEMATICS_BRONZE_PATH}")
    bronze_stream_df = spark.readStream.format("delta").load(TELEMATICS_BRONZE_PATH)
    
    query = (
        bronze_stream_df.writeStream
        .foreachBatch(lambda df, id: upsert_to_silver(df, id, TELEMATICS_SILVER_PATH, ["chassis_no", "event_timestamp"], transform_telematics, "telematics"))
        .outputMode("update")
        .option("checkpointLocation", TELEMATICS_CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )
    return query


# =============================================================================
# --- 5. Main Execution ---
# =============================================================================

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Bronze_to_Silver_Unified_Transformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("Spark session created. Starting all Bronze-to-Silver streams...")

    # Start all the streams
    policy_query = start_policy_stream(spark)
    claim_query = start_claim_stream(spark)
    customer_query = start_customer_stream(spark)
    claim_images_query = start_claim_images_stream(spark)
    image_metadata_query = start_image_metadata_stream(spark)
    training_images_query = start_training_images_stream(spark)
    telematics_query = start_telematics_stream(spark)

    print("All streams started. Awaiting any termination...")
    
    # This will keep the application running until any of the streams fail or are stopped.
    spark.streams.awaitAnyTermination()