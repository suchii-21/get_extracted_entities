import os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.search.documents.models import (
    VectorizableTextQuery,
    QueryType,
    QueryAnswerType,
    QueryCaptionType
)
from azure.search.documents import (SearchClient,SearchItemPaged)
from azure.search.documents.indexes import SearchIndexClient 
from dotenv import load_dotenv
load_dotenv() 

class get_top_chunk:
    def __init__(self) :
       
        self.keyvault_name = os.getenv('keyvault_url')
        self.kv_uri = f"https://{self.keyvault_name}.vault.azure.net"
        self.credential = ClientSecretCredential(
            tenant_id= os.getenv('AZURE_TENANT_ID'), # type: ignore
            client_id= os.getenv('AZURE_CLIENT_ID'), # type: ignore
            client_secret=os.getenv('AZURE_CLIENT_SECRET') # type: ignore
        )

        self.kv_client = SecretClient(vault_url=self.kv_uri, credential=self.credential)
        self.index_name =  self.get_kv_secrets('get-index-name')
        self.search_endpoint = self.get_kv_secrets('get-search-endpoint')
        self.search_client=SearchClient(endpoint=self.search_endpoint,credential=self.credential,index_name=self.index_name) # type: ignore


    def get_kv_secrets(self, secret_name):
        """
        get keyvault secrets 
        """

        try:
            return self.kv_client.get_secret(secret_name).value
        except Exception as e:
            print(f"Error fetching secret {secret_name}: {str(e)}")
            return None
        
    def retriveal_of_top_chunk(self, query):
        vector_query=VectorizableTextQuery(
        text=query,
        k=5,
        fields='text_vector',
        exhaustive=True
    ) 
        results=self.search_client.search(
        search_text=query,
        vector_queries=[vector_query],
        select=['title','chunk','parent_id','confidential'],
        query_type=QueryType.SEMANTIC,
        semantic_configuration_name='legacy-semantic-config', 
        query_caption=QueryCaptionType.EXTRACTIVE,
        query_answer=QueryAnswerType.EXTRACTIVE,
        top=3
    ) 
        print(results)
        # return results.get_answers()
        context_chunks = []
        for result in results:
            chunk = result.get('chunk')
            if chunk:
                context_chunks.append(chunk)

        semantic_answers = results.get_answers()
        if semantic_answers:
            for ans in semantic_answers:
                if ans.text not in context_chunks:
                    context_chunks.append(ans.text)
        return "\n\n".join(context_chunks)
    


