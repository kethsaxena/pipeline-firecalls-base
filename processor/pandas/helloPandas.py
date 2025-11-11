import pandas as pd
from processor.utils.timers import timed_Pandajob

@timed_Pandajob
def run_pandas_job():
    # --- USER CONFIG: put your pandas code here ---
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = pd.DataFrame(data, columns=["Name", "Age"])
    # Show DataFrame (equivalent to df.show() in Spark)
    print(df)
    # Count total rows (equivalent to df.count() in Spark)
    print("Total count:", len(df))

if __name__ == "__main__":
    run_pandas_job()
