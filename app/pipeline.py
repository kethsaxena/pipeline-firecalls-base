import os
# from pyspark.sql import SparkSession
from drive_download import download_from_drive,get_output_data_dir,get_file_id_by_name

# === Step 1: Download from Google Drive
FILE = "FDEMS_DispatchedCallsService_20240831.csv"
FILE_ID = get_file_id_by_name(FILE)
OUTPUT_PATH = fr"{get_output_data_dir()}\{FILE}"

if not os.path.exists(OUTPUT_PATH):
    print("⬇️ Downloading from Google Drive...")
    download_from_drive(FILE_ID, OUTPUT_PATH)
else:
    print("✅ File already exists, skipping download.")
# # === Step 2: Start Spark Session ===
# spark = SparkSession.builder \
#     .appName("DriveToMySQL") \
#     .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
#     .getOrCreate()

# # === Step 3: Load CSV ===
# df = spark.read.option("header", True).csv(OUTPUT_PATH)
# print(f"Loaded {df.count()} rows from Google Drive CSV")

# # === Step 4: Simple Transform ===
# df_clean = df.dropna()

# # === Step 5: Write to MySQL ===
# df_clean.write \
#     .format("jdbc") \
#     .option("url", "jdbc:mysql://mysql:3306/pipelineFirecallsdb") \
#     .option("driver", "com.mysql.cj.jdbc.Driver") \
#     .option("dbtable", "firecalls_clean") \
#     .option("user", "sparkuser") \
#     .option("password", "sparkpass") \
#     .mode("overwrite") \
#     .save()

# print("✅ ETL complete! Data successfully written to MySQL.")
