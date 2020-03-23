"""
Google Cloud Function

Triggered by a google cloud cron daily at 0130

Accesses the raw data in GCS and loads a keras model to make predictions.

"""

def keras_predict(request):
    """
    1. Function to load the data from the last 30 days
    2. Funciton to preprocess and prepare the data for infrence
    3. Function to make predictions

    """
    import pandas as pd
    from tensorflow.keras.models import load_model
    from google.cloud import storage
    from sklearn.preprocessing import MinMaxScaler
    import os
    from cloud_utils.utils import get_client
    from datetime import datetime, timedelta

    FOLDER_DOWN = 'raw-days'
    FOLDER_UP = 'keras_forecasts_v1'
    BUCKET = 'ml-energy-dashboard-raw-data'

    def get_time_dates(period):
        end = datetime.today()
        start = datetime.today() + timedelta(-period)
        delta = end - start

        time_pairs = list()

        for i in range(delta.days + 1):
            begin_time = (start + timedelta(i - 1)).strftime('%Y%m%d')
            begin_time = f'{begin_time}T2300'
            end_time = (start + timedelta(i)).strftime('%Y%m%d')
            end_time = f'{end_time}T2300'

            time_pairs.append((begin_time, end_time))

        return time_pairs

    def gcs_save_name(name, date):
        return f'{name}-{date}'

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

    def load_keras_model(client):

        file_name = 'v1_univar_20200321.h5'

        bucket = client.get_bucket(BUCKET)
        blob = bucket.blob(f'models/{file_name}')
        blob.download_to_filename(file_name)
        model = load_model(f'/tmp/{file_name}')

        return model

    def transform_hours(series):
        df = pd.DataFrame()

        df['year'] = series.index.year
        df['month'] = series.index.month
        df['day'] = series.index.day
        df['hours'] = series.index.hour
        df['loads'] = series.values

        date_list = pd.to_datetime(df.loc[:, ['year', 'month', 'day']], format='%Y-%m-%d', errors='ignore')
        df = df.set_index(pd.DatetimeIndex(date_list, name='date'))
        df = df.drop(['year', 'month', 'day'], axis=1)
        df = df.drop_duplicates()
        df = df.pivot(columns='hours', values='loads')

        return df

    def fill_nans(df):
        return df.interpolate(method='linear', axis=0)

    def normalize_df(df):

        scaler = MinMaxScaler().fit(df.values)
        data_normd = scaler.transform(df.values)

        # return as dataframe
        df = pd.DataFrame(data_normd, index=df.index, columns=df.columns)

        return df, scaler


    params = request.get_json()

    if 'gen_keras' in params and params['gen_keras']:

        storage_client = storage.Client()

        time_pairs = get_time_dates(30)
        data_list = list()

        for time_pair in time_pairs:
            file_name = f'es-energy-demand-{time_pair[0]}-{time_pair[1]}'
            data = get_gcs_data(storage_client, BUCKET, FOLDER_DOWN, file_name)
            data_list.append(data)

        data = reset_data_index(data_list)

        #preprocessing pipeline
        data = transform_hours(data)
        data = fill_nans(data)
        data, scaler = normalize_df(data)

        #load model and make predictions
        model = load_keras_model(storage_client)

        lags = [x for x in range(7)] + [x for x in range(14, 29, 7)]

        data = data.iloc[lags].values.reshape((1, 10, 24))
        print(data.shape)

        predictions = model.predict(data)
        predictions = scaler.inverse_transform(predictions)

        date = datetime.today().strftime("%Y-%m-%d")
        idx = pd.DatetimeIndex(pd.date_range(start=date + "T0000", end=date + "T2300", freq='H'))

        preds = pd.DataFrame(predictions.reshape(-1), index=idx, columns=['keras_loads'])

        # upload to gcs
        keras_file_name = gcs_save_name('keras-forecast-v1-', datetime.today().strftime('%Y%m%d'))
        # create persistance - simple persistance today's values >> tomorrow forecast
        upload_data_to_gcs(storage_client, preds, BUCKET, FOLDER_UP, keras_file_name)

