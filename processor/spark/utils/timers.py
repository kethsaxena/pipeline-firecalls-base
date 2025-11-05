import time
import os,inspect
from functools import wraps
from utils.spark_helper import get_spark_session

def timed_job(func):
    """Decorator to measure and print job duration with script name."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        frame = inspect.stack()[1]
        caller_file = os.path.basename(frame.filename)
        print(f"Starting Job: {caller_file}\n")
        total_start = time.time()
        
        # Initialize Spark via helper
        spark = get_spark_session(func.__name__)
        try:
            result = func(spark,*args, **kwargs)
        finally:
            elapsed = time.time() - total_start
            print(f"\n{caller_file} job completed in {elapsed:.2f} seconds.")

            # Stop the Spark session
            spark.stop()
            print("\nSpark stopped cleanly")

        total_end = time.time()
        print(f"\nTotal runtime: {total_end - total_start:.2f} seconds\n")
        return result
    return wrapper
