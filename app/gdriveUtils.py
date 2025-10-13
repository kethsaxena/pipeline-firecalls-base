from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import io
import os


def find_service_account_key(filename="service_account_key.json"):
    # Get absolute path of the project root (two levels up from app/)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, ".."))

    # Walk through the project directory tree
    for root, _, files in os.walk(project_root):
        if filename in files:
            return os.path.join(root, filename)

    return None

def get_obj_GDService():
    """
    Creates and returns an authorized Google Drive API service object.

    Returns:
        service: Google Drive API v3 service object
    """
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
    SERVICE_ACCOUNT_FILE = find_service_account_key()

    if not SERVICE_ACCOUNT_FILE:
        raise FileNotFoundError("❌ service_account_key.json not found in project directory.")

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )

    service = build('drive', 'v3', credentials=creds)
    return service

def get_output_data_dir(folder_name="data"):
    """
    Locate or create the 'data' directory inside the project root.
    Returns the absolute path to the data directory.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
    data_dir = os.path.join(project_root, folder_name)
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def get_file_id_by_name(filename):
    """
    Given an authorized Google Drive `service` object and a file name,
    returns the file ID if found (or None if not found).

    Args:
        service: Google Drive API service object
        filename (str): The exact name of the file to search for

    Returns:
        str or None: The file ID if found, otherwise None
    """
    service = get_obj_GDService()
    results = service.files().list(
        q=f"name = '{filename}'",
        spaces='drive',
        fields="files(id, name)",
        pageSize=10
    ).execute()

    files = results.get('files', [])
    if not files:
        print(f"❌ No file found with name: {filename}")
        return None

    if len(files) > 1:
        print(f"⚠️ Multiple files found with name '{filename}':")
        for f in files:
            print(f"   {f['name']} ({f['id']})")
        print("Returning the first one...")

    return files[0]['id']

def download_from_drive(file_id, destination):
    service = get_obj_GDService()
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(destination, 'wb')
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Downloading: {int(status.progress() * 100)}%")

    print(f"✅ Download complete: {destination}")
