import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import BytesIO

adl_acct_key = os.getenv('ADL_ACCT_KEY')

def blob_authorization():
    blob_service_client = BlobServiceClient.from_connection_string(f'DefaultEndpointsProtocol=https;AccountName=americanvetgroup;AccountKey={adl_acct_key};EndpointSuffix=core.windows.net')
    return blob_service_client



def upload_blob(blob_auth, df, fp):
    blob_client = blob_auth.get_blob_client(container='solver', blob=fp)

    parquet_file = BytesIO()
    df.to_parquet(parquet_file, engine='pyarrow')
    parquet_file.seek(0)  # change the stream position back to the beginning after writing

    blob_client.upload_blob(parquet_file, overwrite=True)