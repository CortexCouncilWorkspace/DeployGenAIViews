from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json


class CreateFebraban:
    def __init__(self):        
        # Get variables from the config file
        variables = self.read_config_file('config/config.json')
        self.project_id_tgt = variables['project_id_tgt']
        self.project_id_src = variables['project_id_src']
        self.dataset_reporting_tgt = variables['dataset_reporting_tgt']
        self.dataset_cdc_processed = variables['dataset_cdc_processed']

    def read_config_file(self, variables_path):
        with open(os.path.join(os.path.dirname(__file__), variables_path)) as f:
            config_data = json.load(f)
            return config_data


# Instantiate the class
cls = CreateFebraban()

# Set up Google Cloud credentials and clients
credentials = service_account.Credentials.from_service_account_file('account/gcp_service_account.json')
bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

file_name = "files/FebrabanHoliday.csv"

# Load CSV file into BigQuery
table_id = f"{cls.project_id_tgt}.{cls.dataset_reporting_tgt}.FebrabanHoliday"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Skip header row
    autodetect=True,  # Automatically infer the schema
    field_delimiter=";",  # Specify the field delimiter as a semicolon
)

with open(file_name, "rb") as source_file:
    load_job = bigquery_client.load_table_from_file(source_file, table_id, job_config=job_config)

# Wait for the job to complete
load_job.result()

# Verify the loaded data
table = bigquery_client.get_table(table_id)
print(f"Loaded {table.num_rows} rows into {table_id}.")
