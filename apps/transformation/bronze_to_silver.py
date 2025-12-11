import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, date_add, abs, regexp_extract, split, concat_ws, to_timestamp, avg
from pyspark.sql.types import IntegerType, DecimalType, BooleanType, LongType, DoubleType
from delta.tables import DeltaTable

# --- Configuration ---
APP_NAME = "Transformation_Bronze_to_Silver"
BASE_PATH = "s3a://car-smart-claims"
BRONZE_BASE = os.path.join(BASE_PATH, "bronze")
SILVER_BASE = os.path.join(BASE_PATH, "silver")

# --- Transformation Logic ---

def transform_policy(df, spark):
    return (
        df.dropna(subset=["policy_no", "cust_id"])
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

def transform_claim(df, spark):
    return (
        df.dropna(subset=["claim_no"])
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

def transform_customer(df, spark):
    return (
        df.dropna(subset=["customer_id"])
        .dropDuplicates(["customer_id"])
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .withColumn("date_of_birth", date_add(lit("1970-01-01"), col("date_of_birth").cast(IntegerType())))
        .withColumn("firstname", split(col("name"), " ").getItem(0))
        .withColumn("lastname", split(col("name"), " ").getItem(1))
        .withColumn("address", concat_ws(", ", col("borough"), col("zip_code")))
        .drop("name", "borough", "zip_code")
    )

def transform_claim_images(df, spark):
    return df.withColumn("image_name", regexp_extract(col("path"), r".*/(.*?.jpg)", 1))

def transform_image_metadata(df, spark):
    return df.dropna(subset=["chassis_no", "claim_no"]).dropDuplicates(["image_name"])

def transform_training_images(df, spark):
    return df.withColumn("label", regexp_extract("path", r"/(\d+)-([a-zA-Z]+)(?: \(\d+\))?\.png$", 2))

def transform_telematics(df, spark):
    silver_path = os.path.join(SILVER_BASE, "telematics")
    
    base_df = (
        df.dropna(subset=["chassis_no", "event_timestamp"])
        .dropDuplicates(["chassis_no", "event_timestamp"])
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("latitude", 
            when(col("latitude").cast(DoubleType()) < -90.0, lit(-90.0))
            .when(col("latitude").cast(DoubleType()) > 90.0, lit(90.0))
            .otherwise(col("latitude").cast(DoubleType()))
        )
        .withColumn("longitude", 
            when(col("longitude").cast(DoubleType()) < -180.0, lit(-180.0))
            .when(col("longitude").cast(DoubleType()) > 180.0, lit(180.0))
            .otherwise(col("longitude").cast(DoubleType()))
        )
    )

    # Speed Imputation using existing Silver data
    if DeltaTable.isDeltaTable(spark, silver_path):
        avg_speed_df = (
            DeltaTable.forPath(spark, silver_path).toDF()
            .groupBy("chassis_no")
            .agg(avg("speed").alias("avg_chassis_speed"))
        )
        
        return (
            base_df.join(avg_speed_df, "chassis_no", "left")
            .withColumn("speed",
                when(col("speed").cast(DoubleType()).isNotNull(), col("speed").cast(DoubleType()))
                .otherwise(col("avg_chassis_speed"))
            )
            .drop("avg_chassis_speed")
        )
    
    return base_df.withColumn("speed", col("speed").cast(DoubleType()))

# --- Generic Upsert Logic ---

def upsert_to_silver(batch_df, batch_id, target_path, pk_cols, transform_func):
    spark = batch_df.sparkSession
    silver_df = transform_func(batch_df, spark)
    
    merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

    if not DeltaTable.isDeltaTable(spark, target_path):
        silver_df.write.format("delta").mode("overwrite").save(target_path)
    else:
        (
            DeltaTable.forPath(spark, target_path).alias("target")
            .merge(silver_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

# --- Stream Orchestration ---

TABLE_CONFIGS = [
    {
        "name": "policy",
        "source": os.path.join(BRONZE_BASE, "database/policy"),
        "target": os.path.join(SILVER_BASE, "policy"),
        "pk": ["policy_no"],
        "func": transform_policy
    },
    {
        "name": "claim",
        "source": os.path.join(BRONZE_BASE, "database/claim"),
        "target": os.path.join(SILVER_BASE, "claim"),
        "pk": ["claim_no"],
        "func": transform_claim
    },
    {
        "name": "customer",
        "source": os.path.join(BRONZE_BASE, "database/customer"),
        "target": os.path.join(SILVER_BASE, "customer"),
        "pk": ["customer_id"],
        "func": transform_customer
    },
    {
        "name": "claim_images",
        "source": os.path.join(BRONZE_BASE, "claim_images"),
        "target": os.path.join(SILVER_BASE, "claim_images"),
        "pk": ["path"],
        "func": transform_claim_images
    },
    {
        "name": "image_metadata",
        "source": os.path.join(BRONZE_BASE, "image_metadata"),
        "target": os.path.join(SILVER_BASE, "image_metadata"),
        "pk": ["image_name"],
        "func": transform_image_metadata
    },
    {
        "name": "training_images",
        "source": os.path.join(BRONZE_BASE, "training_images"),
        "target": os.path.join(SILVER_BASE, "training_images"),
        "pk": ["path"],
        "func": transform_training_images
    },
    {
        "name": "telematics",
        "source": os.path.join(BRONZE_BASE, "telematics"),
        "target": os.path.join(SILVER_BASE, "telematics"),
        "pk": ["chassis_no", "event_timestamp"],
        "func": transform_telematics
    }
]

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

    print(f"Starting Bronze-to-Silver streams for {len(TABLE_CONFIGS)} tables...")

    for config in TABLE_CONFIGS:
        print(f"Initializing stream: {config['name']}")
        
        # We capture the specific config variables in the lambda defaults to avoid late binding issues in loops
        writer_callback = (
            lambda df, id, p=config['target'], k=config['pk'], f=config['func']: 
            upsert_to_silver(df, id, p, k, f)
        )

        (
            spark.readStream.format("delta").load(config['source'])
            .writeStream
            .foreachBatch(writer_callback)
            .outputMode("update")
            .option("checkpointLocation", os.path.join(config['target'], "_checkpoints"))
            .trigger(processingTime="60 seconds")
            .start()
        )

    spark.streams.awaitAnyTermination()