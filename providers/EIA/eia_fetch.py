import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
from main import DataPipeline


class EiaFetch(DataPipeline): 
    def __init__(self, vault_address, vault_token, secret_path, config):
        super().__init__(vault_address, vault_token, secret_path, config)

    def fetch_data(self):
        vault_secrets = self.get_vault_credentials()
        base_api_url = self.config["base_url"]
        
        for table in self.config["tables"]:
            engine = self.database_engine(table['sourceDatabase'])
            table_name = table["tableName"]
            columns_required = table["columns"]
            rename_value_col = table["renameValueCol"]
            api_url = base_api_url+table["url"]
            offset = self.existing_data_count(table_name,engine)

            params=table['params']
            params['api_key'] = vault_secrets['API_KEY']
            params['offset'] = offset

            response = requests.get(api_url,params )
            data = response.json()
            total_record = int(data['response']['total'])
            print(total_record)

            list_praser=self.create_chunks(total_record,offset)
            if len(list_praser) > 1:
                for i in range(len(list_praser)-1):
                    offsets = range(list_praser[i], list_praser[i+1], 5000)
                    self.thread_executor(engine,offsets,api_url,params,table_name,columns_required,rename_value_col)
            else:
                print("No new records were discovered.")

        


    def existing_data_count(self,tableName,engine):
        """
        Checks if a specified table exists in the database and retrieve no. of records for the table.

        Parameters:
        - tableName (str): The name of the table to check for existence.
        - engine (SQLAlchemy Engine): The database engine to connect to.

        Returns:
        - int: The count of records in the table if it exists, or 0 if the table does not exist or an error occurs.
        """
        try:
            query = f'SELECT count(*) FROM {tableName};'  
            df = pd.read_sql(query, engine)
            print(f"present records count {df.iloc[0,0]}")
            return df.iloc[0,0]
        except:
            print(f"No records available for table {tableName}")
            return 0

    def create_chunks(self,total,offset): # E
        """
        Creates a list of chunk boundaries for splitting a total into smaller 
        segments.

        Parameters:
        - total (int): The total number that needs to be divided into chunks.
        - offset (int): existing number of records.

        Returns:
        - list: A list of integers representing the boundaries of each chunk, 
        starting from 0 and ending with the total. The default chunk size is 
        set to 10,00,000.
        """
        chunks = []
        chunk_size = 1000000 
        for i in range(offset, total + 1, chunk_size):
            chunks.append(i)
        if total not in chunks:
            chunks.append(total)
        
        return chunks
    
    def thread_executor(self,en,offsets,apiUrl,params_template,table_name,requiredCol,repColNameWith,db_lock=threading.Lock()): # E
        """
        Executes API requests in parallel using a thread pool and inserts the 
        retrieved data into a PostgreSQL database.

        Parameters:
        - en: The SQLAlchemy engine object used to connect to the PostgreSQL database.
        - offsets (range): A range of offsets for pagination in API requests.
        - apiUrl (str): The URL of the API to fetch data from.
        - params_template (dict): A dictionary template of parameters for the API request.
        - table_name (str): The name of the table in the PostgreSQL database where 
        the data will be inserted.
        - requiredCol (list): A list of columns to extract from the API response.
        - repColNameWith (str): The new name for the 'value' column in the DataFrame.
        - db_lock (threading.Lock): A lock to ensure thread-safe database operations.

        Note:
        - The function employs a sleep mechanism to manage the request rate to 
        avoid overloading the API.
        """
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_offset = {}
            
            for offset in offsets:
                params = params_template.copy()
        
                params['offset'] = offset
                future = executor.submit(requests.get, apiUrl, params)
                future_to_offset[future] = offset
                time.sleep(0.2)

            for future in as_completed(future_to_offset):
                offset = future_to_offset[future]
                response = future.result()
                try:
                    response.raise_for_status()  
                    data = response.json()
                    df = data['response']['data']
                    df = pd.DataFrame(df)
                    df['value'] = pd.to_numeric(df['value'], errors='coerce')  

                    df = df[requiredCol].rename(columns={'value': repColNameWith})
                    df["timestamp"] = pd.Timestamp.now()

                    self.validate_column_types(df)

                    with db_lock:
                        df.to_sql(table_name, en, if_exists='append', index=False)
                        time.sleep(0.2 )
                        print(f"Data loaded successfully for {table_name} with offset {offset}")

                except Exception as e:
                    print(e)
                    df = pd.DataFrame({'table': [table_name], 'offset': [offset], 'error': [response.status_code]})
                    df.to_sql('Failed_import_api', en, if_exists='append', index=False)
                    print(f"An error occurred for {table_name} at Offset {offset}: {response.status_code}")


    def validate_column_types(self, df):
        """
        Validates the column types of the DataFrame.

        Parameters:
        - df (pd.DataFrame): The DataFrame to validate.

        Raises:
        - ValueError: If any column type is incorrect.
        """
        if not pd.api.types.is_string_dtype(df['period']):
            raise ValueError("Column 'period' should be of string type.")
        if not pd.api.types.is_string_dtype(df['stateId']):
            raise ValueError("Column 'stateId' should be of string type.")
        if not pd.api.types.is_string_dtype(df['fuelId']):
            raise ValueError("Column 'fuelId' should be of string type.")
        if not pd.api.types.is_numeric_dtype(df['co2_emission_MMT']):
            raise ValueError("Column 'co2_emission_MMT' should be of numeric type.")