import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, LongType
from delta.tables import DeltaTable

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
CDC_TOPIC_PATTERN = "pg-claims-server.demo.*"
BASE_DELTA_PATH = "s3a://car-smart-claims/bronze/database"

# =============================================================================
# --- 1. DEFINE SCHEMAS FOR EACH TABLE ---
# MODIFIED: All fields are set to StringType() as requested.
# =============================================================================

def get_policy_schema():
    """Defines the schema for the 'policies' table. All fields as String."""
    return StructType([
        StructField("policy_no", StringType(), True),
        StructField("cust_id", StringType(), True),
        StructField("policytype", StringType(), True),
        StructField("pol_issue_date", StringType(), True),
        StructField("pol_eff_date", StringType(), True),
        StructField("pol_expiry_date", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("model_year", StringType(), True),
        StructField("chassis_no", StringType(), True),
        StructField("use_of_vehicle", StringType(), True),
        StructField("product", StringType(), True),
        StructField("sum_insured", StringType(), True),
        StructField("premium", StringType(), True),
        StructField("deductable", StringType(), True)
    ])

def get_claim_schema():
    """Defines the schema for the 'claims' table. All fields as String."""
    return StructType([
        StructField("claim_no", StringType(), True),
        StructField("policy_no", StringType(), True),
        StructField("claim_date", StringType(), True),
        StructField("months_as_customer", StringType(), True), 
        StructField("injury", StringType(), True),  
        StructField("property", StringType(), True),
        StructField("vehicle", StringType(), True),     
        StructField("total", StringType(), True),        
        StructField("collision_type", StringType(), True),
        StructField("number_of_vehicles_involved", StringType(), True), 
        StructField("driver_age", StringType(), True),     
        StructField("insured_relationship", StringType(), True),
        StructField("license_issue_date", StringType(), True),
        StructField("incident_date", StringType(), True),
        StructField("incident_hour", StringType(), True),  
        StructField("incident_type", StringType(), True),
        StructField("incident_severity", StringType(), True),
        StructField("number_of_witnesses", StringType(), True),
        StructField("suspicious_activity", StringType(), True)
    ])

def get_customer_schema():
    """Defines the schema for the 'customers' table. All fields as String."""
    return StructType([
        # This field was missing from the previous check.
        StructField("customer_id", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("neighborhood", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("name", StringType(), True),
    ])

def get_schema_and_pk(table_name):
    """Helper function to return the schema and primary key for a given table."""
    if table_name == "policy":
        return get_policy_schema(), "policy_no"
    elif table_name == "claim":
        return get_claim_schema(), "claim_no"
    elif table_name == "customer":
        return get_customer_schema(), "customer_id" 
    else:
        print(f"Warning: No schema defined for table '{table_name}'. Skipping.")
        return None, None

# =============================================================================
# --- 2. DEFINE THE BATCH PROCESSING (MERGE) FUNCTION ---
# This function is the core of the CDC logic.
# We MUST use .foreachBatch() because the standard .writeStream()
# sink does not support MERGE (upsert/delete) operations.
# =============================================================================

def upsert_to_delta(batch_df, batch_id):
    """
    Parses a micro-batch of CDC data, applies the correct schema per table,
    and merges it into the corresponding Delta table.
    """
    # Get all distinct tables present in this micro-batch
    # We do this first so we can process one table at a time.
    tables = [row.table_name for row in batch_df.select("table_name").distinct().collect()]
    
    for table_name in tables:
        print(f"Processing batch {batch_id} for table: {table_name}")
        
        # Get the correct schema and Primary Key for this table
        table_schema, pk_col = get_schema_and_pk(table_name)
        
        if table_schema is None:
            continue # Skip if we don't have a schema for this table

        # Filter the batch to only include rows for the current table
        table_cdc_df = batch_df.filter(col("table_name") == table_name)

        # Parse the 'before' and 'after' JSON strings using the *correct* schema (all strings)
        parsed_df = table_cdc_df.withColumn(
            "before_data", 
            from_json(col("before_str"), table_schema)
        ).withColumn(
            "after_data", 
            from_json(col("after_str"), table_schema)
        )

        # Select the final dataset.
        # - For 'delete' (d), we use the 'before' data (to get the PK).
        # - For 'create' (c), 'update' (u), or 'read' (r), we use the 'after' data.
        # We then flatten the struct ('data.*') to get all columns.
        final_data_df = parsed_df.select(
            "op",
            when(col("op") == "d", col("before_data")).otherwise(col("after_data")).alias("data")
        ).select("op", "data.*")
        
        # Define the target Delta table path (e.g., /opt/spark/data/claims/policies)
        delta_path = os.path.join(BASE_DELTA_PATH, table_name)

        # Ensure the target Delta table exists before trying to merge
        if not DeltaTable.isDeltaTable(spark, delta_path):
            print(f"Creating new Delta table at: {delta_path}")
            # Create an empty DF with the correct schema to initialize the table
            # We exclude 'op' as it's not part of the final table
            columns_to_create = [c for c in final_data_df.columns if c != 'op']
            empty_df = spark.createDataFrame([], final_data_df.select(columns_to_create).schema)
            empty_df.write.format("delta").mode("overwrite").save(delta_path)

        # Get the DeltaTable object
        delta_table = DeltaTable.forPath(spark, delta_path)

        # --- CORE CDC LOGIC ---
        # Merge the new batch (source) into the existing Delta table (target).
        # The merge condition uses the primary key column identified for the table.
        merge_condition = f"target.{pk_col} = source.{pk_col}"
        
        # Define the column mapping for updates and inserts
        # This dynamically builds: {'col1': 'source.col1', 'col2': 'source.col2', ...}
        update_set = {col: f"source.{col}" for col in final_data_df.columns if col != 'op'}
        insert_values = {col: f"source.{col}" for col in final_data_df.columns if col != 'op'}

        (
            delta_table.alias("target")
            .merge(final_data_df.alias("source"), merge_condition)
            .whenMatchedUpdate(  # Handle Updates (op = 'u')
                condition = "source.op = 'u'",
                set = update_set
            )
            .whenMatchedDelete(  # Handle Deletes (op = 'd')
                condition = "source.op = 'd'"
            )
            .whenNotMatchedInsert( # Handle Creates (op = 'c') and Reads (op = 'r')
                condition = "source.op IN ('c', 'r')",
                values = insert_values
            )
            .execute()
        )
    print(f"Finished processing batch {batch_id}")


def process_cdc_stream(spark: SparkSession):
    """Reads CDC data from Kafka, processes it, and writes to corresponding Delta tables."""
    print(f"Starting CDC stream from topic pattern: {CDC_TOPIC_PATTERN}")

    # Define the schema for the Debezium message *wrapper*.
    # We read 'before' and 'after' as raw strings to parse them later
    # inside foreachBatch, because their schema depends on the table.
    debezium_wrapper_schema = StructType([
        StructField("before", StringType(), True), # Keep as JSON string
        StructField("after", StringType(), True),  # Keep as JSON string
        StructField("source", StructType([StructField("table", StringType(), True)]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True)
    ])
    
    # Read from Kafka
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribePattern", CDC_TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse the JSON wrapper
    parsed_df = stream_df.select(
        from_json(col("value").cast("string"), debezium_wrapper_schema).alias("data"),
    ).select(
        col("data.before").alias("before_str"),
        col("data.after").alias("after_str"),
        col("data.source.table").alias("table_name"),
        col("data.op").alias("op")
    ).filter(col("table_name").isNotNull()) # Filter out any messages without a table name

    # Write the stream using our custom foreachBatch function
    query = (
        parsed_df.writeStream
        .outputMode("update") # Use 'update' mode for foreachBatch
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", os.path.join(BASE_DELTA_PATH, "_checkpoint"))
        .trigger(processingTime="60 seconds")
        .start()
    )

    print(f"CDC stream writing to Delta tables at {BASE_DELTA_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DeltaClaimsIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    # Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    process_cdc_stream(spark)