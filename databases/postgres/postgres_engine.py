from sqlalchemy import create_engine


class PostgresEngine():
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.engine = self.create_engine()
        
    def create_engine(self):
        vault_secrets = self.pipeline.get_vault_credentials()
        return create_engine(
            f"postgresql+psycopg2://{vault_secrets['POST_USER']}:{vault_secrets['POST_PASS']}@"
            f"{vault_secrets['HOST']}:{vault_secrets['PORT']}/{vault_secrets['DB']}"
        )