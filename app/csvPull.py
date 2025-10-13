import os
from app.gdriveUtils import download_from_drive,get_output_data_dir,get_file_id_by_name

# === Step 1: Download from Google Drive
FILE = "FDEMS_DispatchedCallsService_20240831.csv"
FILE_ID = get_file_id_by_name(FILE)
OUTPUT_PATH = fr"{get_output_data_dir()}\{FILE}"

def main():
    if not os.path.exists(OUTPUT_PATH):
        print("⬇ Downloading from Google Drive...")
        download_from_drive(FILE_ID, OUTPUT_PATH)
    else:
        print("! File already exists, skipping download.")

if __name__ == "__main__":
    main()
