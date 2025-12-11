import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, LongType
from delta.tables import DeltaTable

# --- Configuration ---
APP_NAME = "Ingest_CDC_Bronze"
KAFKA_SERVERS = "kafka:9092"
KAFKA_TOPIC_PATTERN = "pg-claims-server.demo.*"
BASE_PATH = "s3a://car-smart-claims"
BRONZE_DB_PATH = os.path.join(BASE_PATH, "bronze/database")
CHECKPOINT_DIR = os.path.join(BRONZE_DB_PATH, "_checkpoints")

# --- Schemas ---
SCHEMA_POLICY = StructType([
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

SCHEMA_CLAIM = StructType([
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

SCHEMA_CUSTOMER = StructType([
    StructField("customer_id", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("neighborhood", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("name", StringType(), True),
])

SCHEMA_DEBEZIUM = StructType([
    StructField("before", StringType(), True),
    StructField("after", StringType(), True),
    StructField("source", StructType([StructField("table", StringType(), True)]), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

def get_table_config(table_name):
    if table_name == "policy":
        return SCHEMA_POLICY, "policy_no"
    elif table_name == "claim":
        return SCHEMA_CLAIM, "claim_no"
    elif table_name == "customer":
        return SCHEMA_CUSTOMER, "customer_id" 
    return None, None

def upsert_to_delta(batch_df, batch_id):
    tables = [row.table_name for row in batch_df.select("table_name").distinct().collect()]
    
    for table_name in tables:
        table_schema, pk_col = get_table_config(table_name)
        if not table_schema:
            continue

        table_cdc_df = batch_df.filter(col("table_name") == table_name)

        parsed_df = table_cdc_df.withColumn(
            "before_data", from_json(col("before_str"), table_schema)
        ).withColumn(
            "after_data", from_json(col("after_str"), table_schema)
        )

        final_data_df = parsed_df.select(
            "op",
            when(col("op") == "d", col("before_data")).otherwise(col("after_data")).alias("data")
        ).select("op", "data.*")
        
        delta_path = os.path.join(BRONZE_DB_PATH, table_name)

        if not DeltaTable.isDeltaTable(spark, delta_path):
            columns_to_create = [c for c in final_data_df.columns if c != 'op']
            empty_df = spark.createDataFrame([], final_data_df.select(columns_to_create).schema)
            empty_df.write.format("delta").mode("overwrite").save(delta_path)

        delta_table = DeltaTable.forPath(spark, delta_path)
        merge_condition = f"target.{pk_col} = source.{pk_col}"
        
        update_set = {col: f"source.{col}" for col in final_data_df.columns if col != 'op'}
        insert_values = {col: f"source.{col}" for col in final_data_df.columns if col != 'op'}

        (
            delta_table.alias("target")
            .merge(final_data_df.alias("source"), merge_condition)
            .whenMatchedUpdate(condition="source.op = 'u'", set=update_set)
            .whenMatchedDelete(condition="source.op = 'd'")
            .whenNotMatchedInsert(condition="source.op IN ('c', 'r')", values=insert_values)
            .execute()
        )

def process_cdc_stream(spark: SparkSession):
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribePattern", KAFKA_TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = stream_df.select(
        from_json(col("value").cast("string"), SCHEMA_DEBEZIUM).alias("data"),
    ).select(
        col("data.before").alias("before_str"),
        col("data.after").alias("after_str"),
        col("data.source.table").alias("table_name"),
        col("data.op").alias("op")
    ).filter(col("table_name").isNotNull())

    query = (
        parsed_df.writeStream
        .outputMode("update")
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="60 seconds")
        .start()
    )
    
    query.awaitTermination()

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

    process_cdc_stream(spark)