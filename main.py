import os 
import json
import hvac
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from importlib import import_module
from dotenv import load_dotenv

load_dotenv()


class DataPipeline:
    def __init__(self,vault_address, vault_token, secret_path,config):
        self.vault_address= vault_address
        self.vault_token= vault_token
        self.secret_path= secret_path
        self.config = config
        self.location= self.get_vault_credentials()
        self.source_engine= self.create_source_engine()
        self.sink_engine= self.create_sink_engine()
        pass


    def get_vault_credentials(self):
        client = hvac.Client(url=self.vault_address, token=self.vault_token)
        read_secret_result = client.read(self.secret_path)
        return read_secret_result['data']['data']
    
    def create_source_engine(self):
        return create_engine(
            f"postgresql+psycopg2://{self.location['POST_USER']}:{self.location['POST_PASS']}@"
            f"{self.location['HOST']}:{self.location['PORT']}/{self.location['DB']}"
        )
    
    def create_sink_engine(self):
        return create_engine(URL(
            user=self.location['SNOW_USER'],
            password=self.location['SNOW_PASSWORD'],
            account=self.location['SNOW_ACCOUNT'],
            warehouse=self.location['SNOW_WAREHOUSE'],
            database=self.location['SNOW_DATABASE'],
            schema=self.location['SNOW_SCHEMA'],
            role=self.location['SNOW_ROLE']
        ))
    
    def load_provider(self, provider):
        module = import_module(f'providers.{provider}.{provider.lower()}_fetch')
        provider_class = getattr(module, f'{provider.capitalize()}Fetch')
        return provider_class(self, self.config)
    
    

if __name__ == "__main__":

    with open("config\\co2_emission_data.json","r") as file:
        config = json.load(file)

    vault_address = os.getenv("VALUT_ADDRESS")
    vault_token = os.getenv("VALUT_TOKEN")
    secret_path = os.getenv("SECRET_PATH")

    pipeline = DataPipeline(vault_address, vault_token, secret_path,config)
    provider = pipeline.load_provider(config['provider'])
    data = provider.fetch_data()