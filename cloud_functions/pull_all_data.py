def pull_all_data(request):
    from flask import jsonify
    from datetime import datetime, timedelta
    import pandas
    from google.cloud import storage
    from google.oauth2 import service_account
    
    BUCKET='ml-energy-dashboard-raw-data'
    FOLDER_DOWN='raw-days'
    FOLDER_PERSIST = 'persistance_forecasts'

    def get_time_dates(period, pairs=False):
        end = datetime.today()
        start = datetime.today() + timedelta(-period)
        delta = end-start

        if pairs:
            time_pairs = list()

            for i in range(delta.days+1):
                begin_time = (start + timedelta(i-1)).strftime('%Y%m%d')
                begin_time = f'{begin_time}T2300'
                end_time = (start + timedelta(i)).strftime('%Y%m%d')
                end_time = f'{end_time}T2300'

                time_pairs.append((begin_time, end_time))
            return time_pairs
        else:
            dates = list()
            for i in range(delta.days+1):
                date = (start + timedelta(i+1)).strftime('%Y%m%d')
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
    
    payload = {"success": False}
    if "download" in request and request['download']:
        
        payload['df_loads'] = get_data(client, get_time_dates(7, pairs=True))
        persistance = get_persistence(client, get_time_dates(2, pairs=False))
        payload['df_naive'] = persistance['naive']
        payload['df_MA3'] = persistance['MA3-day']
        payload['df_MA3_hbh'] = persistance['MA30day-hbh']
        payload['success']=True
        
    return jsonify(payload)