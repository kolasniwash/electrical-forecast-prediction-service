def comb_all_data(request):
    from flask import jsonify
    from datetime import datetime, timedelta
    from google.cloud import storage
    import pandas as pd

    BUCKET = 'ml-energy-dashboard-raw-data'
    FOLDER_DOWN = 'raw-days'
    FOLDER_PERSIST = 'persistance_forecasts'
    FOLDER_KERAS = 'keras_forecasts_v1'

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

        data = reset_data_index(data_list)
        return data

    def get_keras_forecast(client, dates):
        data_list = list()
        for date in dates:
            file_name = f'keras-forecast-v1--{date}'
            data = get_gcs_data(client, BUCKET, FOLDER_KERAS, file_name)
            data = pd.read_json(data)
            data_list.append(data)

        #localizing keras data to UTC results in offset of +1 hour compared to other datasets.
        data = reset_data_index(data_list)
        return data

    payload = {"success": False}

    params = request #.get_json()

    if "download" in params and params['download']:

        if params['load_period'] is None:
            load_period = 7
        else:
            load_period = params['load_period']

        if params['persist_period'] is None:
            persist_period = 8
        else:
            persist_period = params['persist_period']

        if params['keras_forecast'] is None:
            keras_period = 3
        else:
            keras_period = params['keras_forecast']

        # from google.oauth2 import service_account
        # import os
        #
        # creds = service_account.Credentials.from_service_account_file(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        # client = storage.Client(credentials=creds, project='ml-energy-dashboard')
        client = storage.Client()

        payload['df_loads'] = str(get_data(client, get_time_dates(load_period, pairs=True)).to_json())
        persistance = get_persistence(client, get_time_dates(persist_period, pairs=False))
        payload['df_naive'] = str(persistance['naive'].to_json())
        payload['df_MA3'] = str(persistance['MA3-day'].to_json())
        payload['df_MA3_hbh'] = str(persistance['MA30day-hbh'].to_json())
        payload['keras_forecast'] = str(get_keras_forecast(client, get_time_dates(keras_period, pairs=False)).to_json())
        payload['success'] = True

    return jsonify(payload)