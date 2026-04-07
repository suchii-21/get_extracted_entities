import os, json, logging, uuid
from azure.identity import DefaultAzureCredential
from azure.cosmos import CosmosClient
from datetime import datetime
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
load_dotenv() 

class CosmosLogs:
    def __init__(self) :

        self.keyvault_name = os.getenv('keyvault_url')
        self.kv_uri = f"https://{self.keyvault_name}.vault.azure.net"

        self.credential = DefaultAzureCredential()
        self.kv_client = SecretClient(vault_url=self.kv_uri, credential=self.credential)

        self.get_db_name = self.get_kv_secrets('cosmos-db')
        self.get_container_name = self.get_kv_secrets('cosmos-container')
        self.get_cosmos_endpoint = self.get_kv_secrets('cosmos-url')

        # cosmos client initalization
        self.client = CosmosClient(url=self.get_cosmos_endpoint, credential=self.credential) # type: ignore
        # retreive the existing db  
        self.database = self.client.get_database_client(self.get_db_name)  # type: ignore
        # retrieve the existing container
        self.container = self.database.get_container_client(self.get_container_name) # type: ignore

    def get_kv_secrets(self, secret_name):
        """
        get keyvault secrets 
        """

        try:
            return self.kv_client.get_secret(secret_name).value
        except Exception as e:
            print(f"Error fetching secret {secret_name}: {str(e)}")
            return None


    def upsert_log_entries(self,log_msg, status, session_id = None):
        if not isinstance(log_msg, (str, dict, list)):
            log_msg = str(log_msg)
        try : 
            log_item = {
                "id": str(uuid.uuid4()),
                "log_msg" : log_msg,
                "timestamp": datetime.utcnow().isoformat(),
                "status" : status,
                "email_session_id" : session_id

            }


            self.container.upsert_item(log_item)
        except Exception as e:
            logging.error(f'Failure while uploading the log entries, due to : {e}')

