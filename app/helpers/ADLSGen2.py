from azure.storage.filedatalake import DataLakeServiceClient
import os
from app.constants.ADLS import *



class ADLSGen2:
    def __init__(self):
        self.account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        self.credentials = AZURE_STORAGE_ACCOUNT_KEY

    def connect(self):
        service_client = DataLakeServiceClient(
            account_url=self.account_url, credential=self.credentials
        )
        return service_client