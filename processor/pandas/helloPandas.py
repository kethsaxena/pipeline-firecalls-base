import pandas as pd
import time

@timed_job
def run_pandas_job():
    # --- USER CONFIG: put your pandas code here ---
    # Create a sample DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = pd.DataFrame(data, columns=["Name", "Age"])

    # Filter rows (equivalent to WHERE Age > 25)
    filtered_df = df[df["Age"] > 25].copy()

    # Add computed column (equivalent to Age + 10 AS AgeIn10Years)
    filtered_df["AgeIn10Years"] = filtered_df["Age"] + 10

    # Show the result
    print(filtered_df)

    # Count total rows
    print("Total rows:", len(filtered_df))

if __name__ == "__main__":
    run_pandas_job()
