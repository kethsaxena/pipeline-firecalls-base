from pyspark.sql import SparkSession
import time
from app.utils.footnotes import timed_job


def run_spark_job():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("HelloSpark") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    # Create a simple DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    # Show DataFrame
    df.show()
    # Simple Spark action to trigger a job (for UI)
    print("Total count:", df.count())
    input("Enter")
    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    run_spark_job()
