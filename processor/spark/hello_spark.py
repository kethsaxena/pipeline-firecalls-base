import time
from utils.timers import timed_job

@timed_job
def run_spark_job(spark):
    # Create a simple DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    # Show DataFrame
    df.show()
    # Simple Spark action to trigger a job (for UI)
    print("Total count:", df.count())

if __name__ == "__main__":
    run_spark_job()
