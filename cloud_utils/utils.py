from entsoe import EntsoePandasClient
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from google.cloud import storage
import os
from google.cloud import storage
from google.oauth2 import service_account

BUCKET = 'ml-energy-dashboard-raw-data'
FOLDER_DOWN = 'raw-days'
FOLDER_PERSIST = 'persistance_forecasts'


def generate_dates(date=None):
    if date == None:
        start = (datetime.today() + timedelta(-2)).strftime('%Y%m%d')
        start = f'{start}T2300'

        end = (datetime.today() + timedelta(-1)).strftime('%Y%m%d')
        end = f'{end}T2300'

        date = (datetime.today() + timedelta(-1)).strftime('%Y%m%d')

    return start, end, date


def gcs_file_name(start, end):
    return f'es-energy-demand-{start}-{end}'


def query_entsoe_load(start, end):
    client = EntsoePandasClient(api_key=os.environ['ENTSOE_TOKEN'])
    data = client.query_load("ES",
                             start=pd.Timestamp(start, tz='UTC'),
                             end=pd.Timestamp(end, tz='UTC'))

    return data


def check_fill_nans(data, date):
    """
    Inserts any missing times in the index and interpolates the missing value
    """
    idx = pd.date_range(start=f'{date}T0000', end=f'{date}T2300', freq='H').tz_localize('Europe/Madrid')
    data = data.append(pd.Series(np.NaN, index=idx.difference(data.index))).sort_index().interpolate()

    return data

def get_client():
    assert os.environ['GOOGLE_APPLICATION_CREDENTIALS'] is not None, 'set GCP application credential'

    creds = service_account.Credentials.from_service_account_file(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    return storage.Client(credentials=creds, project='ml-energy-dashboard')

def upload_data_to_gcs(data, bucket_name, folder_name, file_name):
    storage_client = get_client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{file_name}')
    blob.upload_from_string(data.to_json())

def get_time_dates(period, pairs=False):
    end = datetime.today()
    start = datetime.today() + timedelta(-period)
    delta = end - start

    if pairs:
        time_pairs = list()

        for i in range(delta.days + 1):
            begin_time = (start + timedelta(i - 1)).strftime('%Y%m%d')
            begin_time = f'{begin_time}T2300'
            end_time = (start + timedelta(i)).strftime('%Y%m%d')
            end_time = f'{end_time}T2300'

            time_pairs.append((begin_time, end_time))
        return time_pairs
    else:
        dates = list()
        for i in range(delta.days + 1):
            date = (start + timedelta(i + 1)).strftime('%Y%m%d')
            dates.append(date)

        return dates

def gcs_load_name(start, end):
    return f'es-energy-demand-{start}-{end}'

def reset_data_index(data_list):

    data = pd.concat(data_list, axis=0)
    data.index = data.index.tz_localize('UTC').tz_convert('Europe/Madrid')

    return data

def get_gcs_data(client, bucket_name, folder_name, file_name):

    bucket = client.get_bucket(bucket_name)
    path = f'{folder_name}/{file_name}'
    print(path)
    blob = bucket.blob(f'{folder_name}/{file_name}')
    data_json = blob.download_as_string()
    return data_json

def get_data(client, time_pairs):
    data_list = list()
    for time_pair in time_pairs:
        file_name = f'es-energy-demand-{time_pair[0]}-{time_pair[1]}'
        data = get_gcs_data(client, BUCKET, FOLDER_DOWN, file_name)
        data = pd.read_json(data, typ='series', orient='records', keep_default_dates=False)
        data_list.append(data)

    data = reset_data_index(data_list)
    return data

def get_persistence(client, dates):
    data_list = list()
    for date in dates:
        file_name = f'es-persistance-forecasts-{date}'
        data_json = get_gcs_data(client, BUCKET, FOLDER_PERSIST, file_name)
        data = pd.read_json(data_json)
        data_list.append(data)

    data = pd.concat(data_list, axis=0)
    return data