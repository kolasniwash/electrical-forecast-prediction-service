"""
Google Cloud Function

Triggered by a google cloud cron daily at 0110

Accesses the raw predictions in GCS and calcualtes the persistance forecast for the next day's benchmark predictions.

Types of persistance forecasts available:

1. Persistance: Previous day's demand used as today's forecast.
2. Persistance 3 Day Moving Average: Applies a moving average to the last 3 days.
3. Persistance Hourly-by-hour 3 day moving average: Applies a moving average for each hour of the day for the last 3 days.

"""


def gen_persistance_forecasts(request):
    from datetime import datetime, timedelta
    import pandas as pd
    import numpy as np
    from google.cloud import storage
    
    FOLDER_DOWN = 'raw-days'
    FOLDER_UP = 'persistance_forecasts'
    BUCKET = 'ml-energy-dashboard-raw-data'
    
    def get_time_dates(period):
        end = datetime.today()
        start = datetime.today() + timedelta(-period)
        delta = end-start

        time_pairs = list()

        for i in range(delta.days+1):
            begin_time = (start + timedelta(i-1)).strftime('%Y%m%d')
            begin_time = f'{begin_time}T2300'
            end_time = (start + timedelta(i)).strftime('%Y%m%d')
            end_time = f'{end_time}T2300'

            time_pairs.append((begin_time, end_time))
        
        return time_pairs 
    
    def raw_data_date():
        return (datetime.today()+timedelta(-1)).strftime('%Y%m%d')
    
    def gcs_save_name(date):
        return f'es-persistance-forecasts-{date}'
    
    def gcs_load_name(start, end):
        return f'es-energy-demand-{start}-{end}'

    def get_gcs_data(client, bucket_name, folder_name, file_name):
        
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(f'{folder_name}/{file_name}')
        data_json = blob.download_as_string()
        
        return pd.read_json(data_json, typ='series', orient='records', keep_default_dates=False)
    
    def upload_data_to_gcs(client, data, bucket_name, folder_name, file_name):
        
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(f'{folder_name}/{file_name}')
        blob.upload_from_string(data.to_json())
    
    def reset_data_index(data_list):

        data = pd.concat(data_list, axis=0)
        data.index = data.index.tz_localize('UTC').tz_convert('Europe/Madrid')

        return data


    def persistance(series, date):
        return series[date]

    def persistance_day_ma(series, num_days, date):
    
        window=24*num_days
        rolling_mean = series.rolling(window=window, min_periods=24, closed='right').mean()
    
        return rolling_mean[date]

    def persistance_MA_hourly(series, days):
        
        df = series.groupby(series.index.hour).mean()
    
        return df

    def calc_persistance_forecasts(data):
        date = (datetime.today()+timedelta(-1)).strftime('%Y%m%d')
        
        p1 = persistance(data, date)
        p2 = persistance_day_ma(data, 3, date)
        p3 = persistance_MA_hourly(data, 3)
        
        data = np.vstack([p1.values, p2.values, p3.values]).T
        persist_df = pd.DataFrame(data, columns=['naive', 'MA3-day', 'MA30day-hbh'])

        today = datetime.today().strftime('%Y%m%d')
        persist_df.index = pd.DatetimeIndex(pd.date_range(start=f'{today}T0000', end=f'{today}T2300', freq='H'))
        
        return persist_df

    params = request.get_json()
    
    if 'gen_persist' in params and params['gen_persist']:
        storage_client = storage.Client()

        # creds = service_account.Credentials.from_service_account_file(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        # storage_client = storage.Client(credentials=creds, project=project_id)
        #download the data to make the persistance forecast
        time_pairs = get_time_dates(3)
        print(time_pairs)
        data_list = list()

        for time_pair in time_pairs:

            file_name = f'es-energy-demand-{time_pair[0]}-{time_pair[1]}'
            data = get_gcs_data(storage_client, BUCKET, FOLDER_DOWN, file_name)
            data_list.append(data)
            
        data = reset_data_index(data_list)

        #calcuate the persistance forecasts
        persistance_forecasts = calc_persistance_forecasts(data)

        #upload to gcs
        persistance_file_name = gcs_save_name(datetime.today().strftime('%Y%m%d'))
        #create persistance - simple persistance today's values >> tomorrow forecast
        upload_data_to_gcs(storage_client, persistance_forecasts, BUCKET, FOLDER_UP, persistance_file_name)
