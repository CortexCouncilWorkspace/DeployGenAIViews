from google.cloud import bigquery
from google.oauth2 import service_account 
import json
import os


class DeployViews:
    def __init__(self):        
        # Get variables from the config file
        config_data = self.read_config_file('config/config.json')
        self.project_id_tgt = config_data['project_id_tgt']
        self.project_id_src = config_data['project_id_src']
        self.dataset_reporting_tgt = config_data['dataset_reporting_tgt']
        self.dataset_cdc_processed = config_data['dataset_cdc_processed']
        self.k9_datasets_processing = config_data['k9_datasets_processing']
        self.k9_datasets_reporting = config_data['k9_datasets_reporting']  

    def read_config_file(self, variables_path):
        with open(os.path.join(os.path.dirname(__file__), variables_path)) as f:
            return json.load(f)

    def execute_sql_scripts(self, folder_path):
        """Execute all SQL scripts in a given folder.

        :param folder_path: Path to the folder containing the SQL scripts.
        """
        # Start cliente BigQuery
        credentials = service_account.Credentials.from_service_account_file('account/gcp_service_account.json')
        bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        
        sql_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".sql")])
        
        for filename in sql_files:
            file_path = os.path.join(folder_path, filename)
            
            try:
                with open(file_path, 'r') as file:
                    sql_script = file.read()
                    
                sql_script = sql_script.replace("{{ project_id_tgt }}", self.project_id_tgt)
                sql_script = sql_script.replace("{{ project_id_src }}", self.project_id_src)                
                sql_script = sql_script.replace("{{ dataset_reporting_tgt }}", self.dataset_reporting_tgt)
                sql_script = sql_script.replace("{{ dataset_cdc_processed }}", self.dataset_cdc_processed)
                sql_script = sql_script.replace("{{ k9_datasets_processing }}", self.k9_datasets_processing)
                sql_script = sql_script.replace("{{ k9_datasets_reporting }}", self.k9_datasets_reporting)

                query_job = bigquery_client.query(sql_script)
                
                query_job.result()

                print(f"Executed script {filename} successfully.")
            except FileNotFoundError:
                print(f"File {filename} not found.")
            except Exception as e:
                print(f"{filename} - An error occurred: {e}")
                raise e
                

cls = DeployViews()  
cls.execute_sql_scripts('scripts')
