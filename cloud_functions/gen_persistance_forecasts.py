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
    
    def upload_data_to_gcs(client, data, date, bucket_name, folder_name, file_name):
        
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(f'{folder_name}/{file_name}')
        blob.upload_from_string(data.to_json())
    
    def reset_data_index(data_list):

        data = pd.concat(data_list, axis=0)
        data.index = data.index.tz_localize('UTC').tz_convert('Europe/Madrid')

        return data


    def persistance(series):
        date = (datetime.today()+timedelta(-1)).strftime('%Y%m%d')
        return series[date]


    def persistance_day_ma(series, num_days):
        window=24*num_days
        rolling_mean = series.rolling(window=window, min_periods=24, closed='right').mean()
    
        return rolling_mean[-24:]

    def persistance_MA_hourly(series, days):
        
        df = pd.DataFrame(series.values.reshape((days,24)))
        mean = df.mean()
        mean.index = series.index[-24:]
    
        return mean

    def calc_persistance_forecasts(data):
        p1 = persistance(data)
        p2 = persistance_day_ma(data, 3)
        p3 = persistance_MA_hourly(data, 3)

        persist_forecast = pd.concat([p1, p2, p3], axis=1)
        persist_forecast.columns = ['naive', 'MA3-day', 'MA30day-hbh']

        date = datetime.today().strftime('%Y%m%d')
        persist_forecast.index = pd.DatetimeIndex(pd.date_range(start=f'{date}T0000', end=f'{date}T2300', freq='H'))
        
        return persist_forecast

    params = request.get_json()
    
    if 'gen_persist' in params and params['gen_persist']:
        #storage_client = storage.Client()

        storage_client = service_account.Credentials.from_service_account_file(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

        #download the data to make the persistance forecast
        time_pairs = get_time_dates(3)

        data_list = list()

        for time_pair in time_pairs:

            file_name = f'es-energy-demand-{time_pair[0]}-{time_pair[1]}'
            data = get_gcs_data(storage_client, BUCKET, FOLDER_DOWN, file_name)

            data_list.append(data)


        data = reset_data_index(data_list)

        #calcuate the persistance forecasts
        persistance_forecasts = calc_persistance_forecats(data)

        return persistance_forecasts
        #upload to gcs
        #persistance_file_name = gcs_save_name(datetime.today())
        #create persistance - simple persistance today's values >> tomorrow forecast
        #upload_data_to_gcs(storage_client, data, date_to_forecast, BUCKET, FOLDER_UP, file_to_forecast)
