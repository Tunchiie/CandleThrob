import logging
import pandas as pd
import os
from google.cloud import storage
from io import BytesIO
import google.auth

logging.basicConfig(
    filename="utils/debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
credentials, project_id = google.auth.default()
logger.info("Credentials and project_id loaded: %s", project_id)
print(f"Credentials and project_id loaded: {project_id}")

def upload_to_gcs(bucket_name:str, data:pd.DataFrame, destination_blob_name:str):
    """
    Uploads a DataFrame to the Google Cloud Storage bucket.
    Args:
        bucket_name (str): The name of the GCS bucket.
        data (pd.DataFrame): The DataFrame to upload.
        destination_blob_name (str): The destination path in the GCS bucket.
    """

    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Save the DataFrame to a temporary file
    buffer = BytesIO()
    data.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type='application/octet-stream')

    logger.info(f"File %s uploaded to %s.", buffer, destination_blob_name)
    
def load_from_gcs(bucket_name:str, source_blob_name:str) -> pd.DataFrame:
    """
    Loads a DataFrame from the Google Cloud Storage bucket.
    Args:
        bucket_name (str): The name of the GCS bucket.
        source_blob_name (str): The source path in the GCS bucket.
    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    buffer = BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)

    df = pd.read_parquet(buffer)
    
    logger.info(f"File %s loaded from %s.", source_blob_name, bucket_name)
    
    return df

def blob_exists(bucket_name:str, blob_name:str) -> bool:
    """
    Checks if a file or directory exists in the Google Cloud Storage bucket.
    Args:
        bucket_name (str): The name of the GCS bucket.
        blob_name (str): The name of the blob to check.
    Returns:
        bool: True if the file exists, False otherwise.
    """
    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    exists = blob.exists()
    
    logger.info(f"File %s exists: %s", blob_name, exists)
    
    return exists

def load_all_gcs_data(bucket_name:str, prefix:str) -> pd.DataFrame:
    """
    Loads all DataFrames from a specific prefix in the Google Cloud Storage bucket.
    Args:
        bucket_name (str): The name of the GCS bucket.
        prefix (str): The prefix to filter blobs.
    Returns:
        pd.DataFrame: Concatenated DataFrame of all loaded data.
    """
    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.bucket(bucket_name)
    
    blobs = bucket.list_blobs(prefix=prefix)
    
    dataframes = []
    
    for blob in blobs:
        if blob.name.endswith('.parquet'):
            df = load_from_gcs(bucket_name, blob.name)
            dataframes.append(df)
    logger.info(f"Loaded %d DataFrames from prefix %s in bucket %s.", len(dataframes), prefix, bucket_name)
    return dataframes