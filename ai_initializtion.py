import os, logging, json
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv
load_dotenv()
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.appconfiguration.provider import (
    load,
    SettingSelector
)

# try:
#     from cosmos_logging import CosmosLogs
#     cosmos_class = CosmosLogs()
# except Exception as e:
#     logging.error(f'Module error : {e}')

class AIInitializtion:

    def __init__(self):
        self.keyvault_name = os.getenv('keyvault_url')
        self.kv_uri = f"https://{self.keyvault_name}.vault.azure.net"
        self.credential = ClientSecretCredential(
            tenant_id= os.getenv('AZURE_TENANT_ID'), # type: ignore
            client_id= os.getenv('AZURE_CLIENT_ID'), # type: ignore
            client_secret=os.getenv('AZURE_CLIENT_SECRET') # type: ignore
        )

        self.kv_client = SecretClient(vault_url=self.kv_uri, credential=self.credential)

        self.azure_openai_endpoint = self.get_kv_secrets('azure-endpoint')
        self.azure_openai_version = self.get_kv_secrets('api-version')
        self.deployment_name = self.get_kv_secrets('deploymentname')
        self.app_config_endpoint = self.get_kv_secrets('app-config-endpoint')
        self.config = load(endpoint = self.app_config_endpoint,  # type: ignore
                           credential = self.credential)
        
        # self.entities_extraction_prompt =  os.getenv('entities_extraction_prompt')
        self.nature_of_fraud_detection = self.config['nature_of_fraud_detection']
        self.entities_extraction_prompt = self.config['entities_extraction_prompt']
        # self.get_summarised_content_prompt = os.getenv('get_summarised_content_prompt')
        self.token_provider = get_bearer_token_provider(
                        self.credential,
                        "https://cognitiveservices.azure.com/.default"
                        )
        # self.api_key = os.getenv('azure_api_key')
        if not all([self.keyvault_name, self.azure_openai_endpoint,self.azure_openai_version,self.deployment_name,self.entities_extraction_prompt]):
            logging.error("azure  openai environment variables." )

        
        try :
            self.azure_model_client = AzureOpenAI(
                azure_endpoint= self.azure_openai_endpoint, # type: ignore
                # azure_deployment=self.deployment_name,
                api_version=self.azure_openai_version,
                # api_key= self.api_key
                azure_ad_token_provider= self.token_provider
            )
        except Exception as e:
            logging.error(f'Error Initializing due to : {e}')
            raise 


    def normalize_json(self, data, required_fields, default=''):
        for key, item in data.items():
            if isinstance(item, dict):          
                for field in required_fields:
                    if field not in item:
                        item[field] = default

        return data

    def get_kv_secrets(self, secret_name):
        """
        get keyvault secrets 
        """

        try:
            return self.kv_client.get_secret(secret_name).value
        except Exception as e:
            print(f"Error fetching secret {secret_name}: {str(e)}")
            return None

    def get_extraction(self, session_id, extracted_content):
        try:
            response = self.azure_model_client.chat.completions.create(
                model=self.deployment_name, # type: ignore
                messages=[
                    {"role": "system", "content": self.entities_extraction_prompt + "\nAlways respond with a valid JSON object."}, # type: ignore
                    {"role": "user", "content": f'##extracted_content##  : {extracted_content}'}
                    
                     #extractedcontent# is : {extracted_content}, and ##emailbody## is : {email_body}'}
                ],
                temperature=0, 
                response_format={"type": "json_object"}
            )

            raw_output = response.choices[0].message.content
            json_output = json.loads(raw_output) # type: ignore
            logging.warning(f'Extracted entities for the session id : {session_id} are : {json_output}')

            required_fields = ['description', 'adib_issaffinvolved', 'adib_staffid', 'adib_amount', 'customer_name'] 


            if json_output and isinstance(next(iter(json_output.values())), dict):
                # Nested structure: {"file1": {"customer_name": ...}, ...}
                for file_name, data in json_output.items():
                    logging.info(f'Checking filename: {file_name}')
                    missing_fields = [f for f in required_fields if f not in data]
                    if missing_fields:
                        logging.info(f"Missing fields: {missing_fields}")
                        for field in missing_fields:
                            data[field] = ''         
                    else:
                        logging.info('All fields are present')
            else:

                logging.info('Flat JSON response received')
                missing_fields = [f for f in required_fields if f not in json_output]
                if missing_fields:
                    logging.info(f"Missing fields: {missing_fields}")
                    for field in missing_fields:
                        json_output[field] = ''
                else:
                    logging.info('All fields are present')

            return json_output   # return dict, not the raw string

        except Exception as e:
            logging.error(f'Failed to fetch response due to: {e}')
            return {"description" : '',
                    'adib_issaffinvolved' : '',
                    'adib_staffid' : '',
                    'adib_amount': '',
                    'customer_name': '' }
        
    # def get_summarised_query(self, email_session_id, extracted_content):
    #     try:
    #         response = self.azure_model_client.chat.completions.create(
    #         model=self.deployment_name, # type: ignore
    #         messages=[
    #             {"role": "system", "content": self.get_summarised_content_prompt + "\nAlways respond with a valid JSON object."}, # type: ignore
    #             {"role": "user", "content": f'#extractedcontent# is : {extracted_content}'}
    #         ],
    #         temperature=0,
    #         response_format={"type": "json_object"}
    #     )

    #         raw_output = response.choices[0].message.content
    #         return raw_output

    #     except Exception as e:

    #         logging.error(f'Failed to get the summary of the extracted content   due to : {e}')
    #         return None

    def get_fraud_type(self,description, session_id,extracted_content):
                      
        try:
            response = self.azure_model_client.chat.completions.create(
            model=self.deployment_name, # type: ignore
            messages=[
                {"role": "system", "content": self.nature_of_fraud_detection}, # type: ignore
                {"role": "user", "content": f'#extractedcontent# is : {extracted_content}, description is : {description}'}
                 
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )

            raw_output = response.choices[0].message.content
            json_output = json.loads(raw_output) # type: ignore
            logging.warning(f'email session id : {session_id} nature of fraud is : {json_output}')
            return json_output
        except Exception as e:
            logging.error(f'Failed to get the nature of fraud due to : {e}')
            return {'nature_of_fraud': 'no_nature_of_fraud'}
