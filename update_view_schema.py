from google.cloud import bigquery
import pandas as pd
import json

# Carregar o JSON
with open('AccountsPayableGenAI.json') as f:
    field_descriptions = json.load(f)

# Criar um DataFrame a partir do JSON
df = pd.DataFrame(field_descriptions, columns=['name', 'description'])

client = bigquery.Client()

project_id = 'ce-sap-latam-genai'
dataset_id = 'cortex_sap_reporting'
view_id = 'AccountsPayableGenAI'
table_id = f"{project_id}.{dataset_id}.{view_id}"

table = client.get_table(table_id)

schema_updates = []

for field in table.schema:

    filtered_df = df.loc[df['name'] == field.name]   
    if not filtered_df.empty:    
        field_description = df.loc[df['name'] == field.name, 'description'].values[0]    
    else:
        field_description = field.name
        print(field.name)
    # field_description = f"This is the description for {field.name}"
    updated_field = bigquery.SchemaField(
        field.name, field.field_type, mode=field.mode, description=field_description
    )
    schema_updates.append(updated_field)

table.schema = schema_updates
client.update_table(table, ["schema"])

print(f"Updated descriptions in views  {table_id}.")

