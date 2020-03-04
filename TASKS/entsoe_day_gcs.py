from google.cloud import storage
from google.oauth2 import service_account
import os
from entsoe import EntsoePandasClient
import pandas as pd
from datetime import datetime, timedelta


def query_entsoe_data(date):
    client = EntsoePandasClient(api_key=os.environ['ENTSOE_TOKEN'])
    data = client.query_load("ES", 
                             start=pd.Timestamp(f"{date}T0000", tz='UTC'), 
                             end=pd.Timestamp(f"{date}T2300", tz='UTC'))
    return data

def gcs_file_name(date):
    return f'es-energy-demand-raw-{date}'

def generate_date():
    return (datetime.today()+timedelta(-1)).strftime('%Y%m%d')


if __name__ == "__main__":

	date = generate_date()

	data = query_entsoe_data(date)

	creds = service_account.Credentials.from_service_account_file(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
	project = 'ml-energy-dashboard'
	client_s = storage.Client(credentials=creds, project=project)

	print(f'Getting data for {date}')

	bucket_name = 'ml-energy-dashboard-raw-data'
	bucket = client_s.get_bucket(bucket_name)
	blob = bucket.blob(f'raw-days/{gcs_file_name(date)}')
	blob.upload_from_string(data.to_json())
	print('Upload complete')