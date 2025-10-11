from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FireCallsETL")
    .enableHiveSupport()
    .getOrCreate()
)

print("✅ Spark Session started")

# Load from Hive
df = spark.sql("SELECT * FROM fire_calls")

# Filter + Optimize
df_filtered = df.filter(df.city == "SAN FRANCISCO").cache()

# Write to Parquet
df_filtered.write.mode("overwrite").parquet("hdfs:///data/fire_calls/processed")

print("✅ Data written successfully to HDFS")

spark.stop()
