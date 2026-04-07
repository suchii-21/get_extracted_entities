import os, time
import json
import logging
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv
load_dotenv()
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.identity import ClientSecretCredential

# try:
#     from cosmos_logging import CosmosLogs

# except Exception as e:
#     logging.error(f'Module error : {e}')


class ExtractingContent:

    def __init__(self):
        self.keyvault_name = os.getenv('keyvault_url')
        self.kv_uri = f"https://{self.keyvault_name}.vault.azure.net"

        self.credential = ClientSecretCredential(
            tenant_id= os.getenv('AZURE_TENANT_ID'), # type: ignore
            client_id= os.getenv('AZURE_CLIENT_ID'), # type: ignore
            client_secret=os.getenv('AZURE_CLIENT_SECRET') # type: ignore
        )
        self.kv_client = SecretClient(vault_url=self.kv_uri, credential=self.credential)

        self.doc_endpoint = self.get_kv_secrets('doc-int-endpoint')
        # self.doc_int_key = os.getenv("DOC_INT_KEY")

        if not all([self.doc_endpoint, self.keyvault_name]):
             logging.error("Missing required environment variables: ")

        self.doc_int_client = DocumentIntelligenceClient(
            endpoint=self.doc_endpoint, # type: ignore
            credential=self.credential  # type: ignore
        )
        # try:
        #     self.cosmos_class = CosmosLogs() if CosmosLogs else None
        # except Exception as e:
        #     logging.error(f'CosmosLogs initialization error: {e}')
        #     self.cosmos_class = None


    def get_kv_secrets(self, secret_name, max_retries=3, delay=2):
        for attempt in range(max_retries):
            try:
                value = self.kv_client.get_secret(secret_name).value
                if value:
                    return value
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} failed for secret '{secret_name}': {e}")
            time.sleep(delay * (attempt + 1))  
        logging.error(f"Failed to retrieve secret '{secret_name}' after {max_retries} attempts")
        return None


    def write_to_json(self, extracted_content, file_name: str, json_file: str = "/tmp/email_sessions/content_json.json") -> None:
        try:
            os.makedirs("/tmp/email_sessions", exist_ok=True)

            if os.path.exists(json_file):
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = {}

            data[file_name] = extracted_content

            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)

            logging.info(f"[JSON] Written '{file_name}' to '{json_file}'")

        except (OSError, json.JSONDecodeError) as e:
            logging.error(f"Failed to write to JSON file '{json_file}': {e}")
            



    def extract_content(self, email_session_id , file_bytes: bytes, file_name: str, blob_handler   ) -> str | None:
        """
        Extract Documnet Intelligence content from txt, jpeg, pdf , docx jpg, heic and png 
        
        """
        try:
            if file_name.endswith('.txt'):
                final_result = file_bytes.decode("utf-8", errors="replace")
            
            else:

                poller = self.doc_int_client.begin_analyze_document(
                    model_id="prebuilt-layout",
                    body=file_bytes,  # type: ignore
                    content_type="application/octet-stream",
                ) # type: ignore

                result = poller.result()
                if file_name.endswith('pdf'):
                    final_result = result
                    lines = [
                        line.content
                        for page in final_result.pages
                        for line in page.lines  
                    ]
                    self.write_to_json(lines, file_name)
                    # self.cosmos_class.upsert_log_entries(log_msg=f'pdf : {file_name}, content : {lines[5]}',
                    #                                 status="sucess",
                    #                                 session_id=email_session_id)
                    blob_handler.upload_extracted_content(email_session_id)
                    return "\n".join(lines)
                
                else:
                    final_result = result.get('content')
                    logging.info(f'docx : {file_name} , content : {final_result[:50]}')
                    # self.cosmos_class.upsert_log_entries(log_msg=f'docx : {file_name} , content : {final_result[:50]}',
                    #                                 status="sucess",
                    #                                 session_id= email_session_id)
                    self.write_to_json(final_result, file_name)
                    blob_handler.upload_extracted_content(email_session_id)

            return final_result 


        except Exception as e:
            logging.error(f"Error analyzing document '{file_name}': {e}")
            return None  