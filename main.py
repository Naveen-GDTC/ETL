import os 
import json
import hvac
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
        pass

    def get_vault_credentials(self):
        client = hvac.Client(url=self.vault_address, token=self.vault_token)
        read_secret_result = client.read(self.secret_path)
        return read_secret_result['data']['data']
    
    def load_provider(self, provider):
        module = import_module(f'providers.{provider}.{provider.lower()}_fetch')
        provider_class = getattr(module, f'{provider.capitalize()}Fetch')
        return provider_class(self, self.config)
    
    def database_engine(self, database):
        module = import_module(f'databases.{database}.{database.lower()}_engine')
        database_class = getattr(module, f'{database.capitalize()}Engine')
        return database_class(self)

if __name__ == "__main__":

    with open("config\\co2_emission_data.json","r") as file:
        config = json.load(file)

    vault_address = os.getenv("VAULT_ADDRESS")
    vault_token = os.getenv("VAULT_TOKEN")
    secret_path = os.getenv("SECRET_PATH")

    pipeline = DataPipeline(vault_address, vault_token, secret_path,config)
    conn = pipeline.database_engine(config['sourceDatabase'])
    provider = pipeline.load_provider(config['provider'])
    provider.fetch_data(conn)