from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

class SnowflakeEngine:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def create_engine(self):
        vault_secrets = self.pipeline.get_vault_credentials()
        return create_engine(URL(
            user=vault_secrets['SNOW_USER'],
            password=vault_secrets['SNOW_PASSWORD'],
            account=vault_secrets['SNOW_ACCOUNT'],
            warehouse=vault_secrets['SNOW_WAREHOUSE'],
            database=vault_secrets['SNOW_DATABASE'],
            schema=vault_secrets['SNOW_SCHEMA'],
            role=vault_secrets['SNOW_ROLE']
        ))