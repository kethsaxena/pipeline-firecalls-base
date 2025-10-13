from pyspark.sql import SparkSession
# --- USER CONFIG: put your Spark SQL code here ---
spark_sql_code = """
SELECT Name, Age, Age + 10 AS AgeIn10Years
FROM people
WHERE Age > 25
"""

# --- Spark session setup ---
spark = SparkSession.builder \
    .appName("SparkSQLRunner") \
    .master("local[*]") \
    .getOrCreate()

# Create a sample DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Register DataFrame as a temporary view to run SQL
df.createOrReplaceTempView("people")

# Run the Spark SQL code
result_df = spark.sql(spark_sql_code)

# Show the result
result_df.show()

# Simple action to trigger a job (so you can see it in Spark UI)
print("Total rows:", result_df.count())

# Keep the session alive for Spark UI inspection
input("Spark is running. Open http://localhost:4040 to view the UI and press Enter to stop...")

# Stop the Spark session
spark.stop()
