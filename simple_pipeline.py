from datetime import datetime
from google.cloud import bigquery, storage
from google.oauth2 import service_account

import json
import os

def main(config_path: str) -> None:

    # Load Config File
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        raise FileNotFoundError('Config File Not Found')
    
    # Authentication via Oauth, avoiding repated authentication between bq and gcs
    # export SERIVCE_ACCOUNT_PATH="YOUR_PATH_TO_GCP_SERVICE_ACCOUNT"
    try:
        service_account_file = os.environ.get('SERVICE_ACCOUNT_PATH')
    except Exception as _e:
        raise FileNotFoundError('Cannot Find Service Account') from _e

    token = service_account.Credentials.from_service_account_file(
        service_account_file)
    
    # Create GCS Path Structure
    date_str = datetime.now().strftime('%Y%m%d%H%M%S')
    gcs_path = f'''odm/{config.get('table_name')}/{date_str}/{os.path.basename(config.get('source_path'))}'''

    # Upload File to GCS
    # We'll use this for ingest to BQ
    try:
        storage_client = storage.client.Client(credentials=token)
        bucket = storage_client.bucket(config.get('bucket_id'))
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(config.get('source_path'))
        print(f'Upload File to GCS Completed')
    except Exception:
        raise('Cannot Upload File To GCS')


    # Ingest Data From GCS to GBQ
    client = bigquery.client.Client(credentials=token)

    # TODO In Development This Coonfiguration Should Be Saperated From Source Code
    # Suggestion in JSON or YAML Format: 
    # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition='WRITE_TRUNCATE',
    )
    uri = f'''gs://{config.get('bucket_id')}/{gcs_path}'''
    table_id = f'''{config.get('project_id')}.{config.get('dataset_id')}.{config.get('table_name')}'''

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location = config.get('location'),  # Must match the destination dataset location.
        job_config = job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print('Loaded {} rows.'.format(destination_table.num_rows))

    
    # TODO: In Our Project please unnest json array and saperate blob upload mechanism :
    ## (1.) Upload the whole json payload
    ## Source
    ##############################
    ### {
    ### attr1_str: "value",
    ### attr2_blob: "blob_path", -> (2.) make upload again -> ret = gcs_path of this blob
    ### attr3_nested: ["subattr1": "value1", "subattr2" ] -> please unnest this
    ### }
    ##############################
    ## Result Table
    ### | attr1_str | attr_2   | subattr1 | subattr2 |
    ### |-----------|----------|----------|----------|
    ### | value     | blob_path| value1   | value2   |
    ##############################
    ## After Finish Ingestion Process YOU MUST HAVE:
    ### 1) JSON Payload of Fetch Data From Source in GCS
    ### 2) Blob (if any) in GCS
    ### 3) Data in GBQ

if __name__ == "__main__":
    main(config_path='./config.json')
