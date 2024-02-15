from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import HttpResponseError, ResourceExistsError
import io
import pandas as pd
import json
import logging
import numpy as np
from core import config

class AzureStorage:
    def __init__(self, container_name: str):
        self.connect_str = config.azure_config.conn_str
        logging.info(
            f"Connecting to Azure Storage with connection string {self.connect_str}"
        )
        # Create the BlobServiceClient object which will be used to create a container client
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.connect_str
        )
        self.container_name = container_name
        logging.info(
            f"Connected to Azure Storage to container name {container_name}")

    def upload_blob_df(self, df: pd.DataFrame, path: str):
        logging.info(f"Uploading blob to {path}", extra={
                     "dataframe.info": df.info()})
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=path
            )
            output = io.StringIO()
            output = df.to_csv(encoding="utf-8")
            blob_client.upload_blob(
                output, blob_type="BlockBlob", overwrite=True, tags={"overwrite": "True"}
            )
            logging.info(f"Successfully uploaded blob to {path}")
        except ResourceExistsError:
            logging.info(
                f"Blob already exists in {path}", extra={"dataframe.info": df.info()}
            )
            pass

    def upload_blob_dfs(self, df: pd.DataFrame, path: str, date: str):
        logging.info(f"Uploading blob to {path}", extra={
                     "dataframe.info": df.info()})
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=f"{date}/{path}"
            )
            output = io.StringIO()
            output = df.to_csv(encoding="utf-8")
            blob_client.upload_blob(
                output, blob_type="BlockBlob", tags={"overwrite": "True"}
            )
            logging.info(f"Successfully uploaded blob to {path}")
        except ResourceExistsError:
            logging.info(
                f"Blob already exists in {path}", extra={"dataframe.info": df.info()}
            )
            pass
        
    def upload_blob_logs(self, log_file, path: str):
        try:
            logging.info(f"Uploading blob to {path}")
            blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, blob=f"{path}"
        )

            blob_client.upload_blob(
                log_file, blob_type="BlockBlob", overwrite=True, tags={"overwrite": "True"}
            )
            logging.info(f"Successfully uploaded blob to {path}")
        except ResourceExistsError:
            logging.info(
                f"Blob already exists in {path}", extra={"dataframe.info": df.info()}
            )
            pass


    def download_blob_df(self, path):
        logging.info(f"Downloading blob from {path}")
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, blob=path
        )
        downloaded_blob = blob_client.download_blob()
        df = pd.read_csv(io.StringIO(downloaded_blob.content_as_text()))
        """logging.info(
            "Successfully downloaded blob",
            extra={"path": path, "dataframe.info": df.info()},
        )"""
        return df

    def download_blob_dfs_date(self, container_name, date):
        container_client = self.blob_service_client.get_container_client(
            container=self.container_name
        )
        blobs = container_client.list_blobs(
            name_starts_with=f"{date}/dataforseo/")
        folder_names = set()
        lis1 = []
        for blob in blobs:
            relative_path = blob.name.replace(f"{date}/similarweb/", "")
            lis1.append(relative_path)

        return lis1

    def download_blob_dfs(self, path, date):
        logging.info(f"Downloading blob from {path}")
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, blob=f"{date}/{path}"
        )
        downloaded_blob = blob_client.download_blob()
        df = pd.read_csv(io.StringIO(downloaded_blob.content_as_text()))
        logging.info(
            "Successfully downloaded blob",
            extra={"path": path, "dataframe.info": df.info()},
        )
        return df

    def download_blob_folder(self, folder_path):
        logging.info(f"Downloading blob from {folder_path}")
        container_client = self.blob_service_client.get_container_client(
            container=self.container_name
        )
        files_path = []
        for file in container_client.walk_blobs(folder_path):
            files_path.append(file.name)
        logging.info("Successfully downloaded folder",
                     extra={"path": folder_path})
        return files_path

    def download_blob_dict(self, path):
        logging.info(f"Downloading blob from {path}")
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, blob=path
        )
        downloaded_blob = blob_client.download_blob()
        dict_blob = json.loads(downloaded_blob.readall())
        logging.info(
            "Successfully downloaded blob", extra={"path": path, "output": dict_blob}
        )
        return dict_blob

    def blob_exists(self, blob_path) -> bool:
        blob = BlobClient.from_connection_string(
            conn_str=self.connect_str,
            container_name=self.container_name,
            blob_name=blob_path,
        )
        return blob.exists()

    def delete_blob(self, blob_path):
        blob_client = BlobClient.from_connection_string(
            conn_str=self.connect_str,
            container_name=self.container_name,
            blob_name=blob_path,
        )

        blob_client.delete_blob()
        logging.info("Successfully deleted blob", extra={"path": blob_path})

    def container_exists(self):
        container_client = self.blob_service_client.get_container_client(
            container=self.container_name
        )
        return container_client.exists()

    def create_container(self):
        logging.info(f"Trying to create container named {self.container_name}")
        container_client = self.blob_service_client.get_container_client(
            container=self.container_name
        )
        container_client.create_container()
        logging.info(f"Successfully created container {self.container_name}")

def save_logs(log_stream, filename):
    azs = AzureStorage(config.azure_config.container_name)
    azs.upload_blob_logs(log_stream, filename)