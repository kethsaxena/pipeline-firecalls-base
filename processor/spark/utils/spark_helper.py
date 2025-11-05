import time
from pyspark.sql import SparkSession

def get_spark_session(job_name: str):
    """
    Initialize and return a SparkSession, timing its startup.
    """
    t0 = time.time()

    spark = (
        SparkSession.builder
        .appName(job_name)
        .master("local[1]")
        # .config("spark.driver.memory", "512m")
        # .config("spark.sql.shuffle.partitions", "1")
        # .config("spark.executor.instances", "1")
        # .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.driver.memory", "512m")  # small driver for local testing
        .config("spark.sql.shuffle.partitions", "1")  # minimal shuffle
        .config("spark.sql.catalogImplementation", "in-memory")  # no Hive
        .config("spark.sql.codegen.wholeStage", "false")  # reduce micro-job overhead
        .config("spark.sql.adaptive.enabled", "false")  # disable adaptive optimization
        .config("spark.sql.cbo.enabled", "false")  # disable cost-based optimizer
        .config("spark.ui.enabled", "false")  # disable UI if not needed
        .config("spark.ui.showConsoleProgress", "false")  # console progress can be slow
        .config("spark.sql.legacy.createHiveTableByDefault", "false")  # minimal catalog work
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # optional for Arrow optimizations
        .getOrCreate()
    )

    t1 = time.time()
    print(f"Spark session started in {t1 - t0:.2f} seconds")
    return spark
