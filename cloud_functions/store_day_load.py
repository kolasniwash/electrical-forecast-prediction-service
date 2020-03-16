#copy of cloud function to download and store data
def store_day_load(request):
    from entsoe import EntsoePandasClient
    from datetime import datetime, timedelta
    import pandas as pd
    import numpy as np
    from google.cloud import storage
    
    FOLDER = 'raw-days'
    BUCKET = 'ml-energy-dashboard-raw-data'
    
    def generate_dates(date=None):

        if date==None:
            start = (datetime.today()+timedelta(-2)).strftime('%Y%m%d')
            start = f'{start}T2300'

            end = (datetime.today()+timedelta(-1)).strftime('%Y%m%d')
            end =f'{end}T2300'

            date = (datetime.today()+timedelta(-1)).strftime('%Y%m%d')
        
        return start, end, date
    
    def gcs_file_name(start, end):
        return f'es-energy-demand-{start}-{end}'
    
    def query_entsoe_load(start, end):
        
        client = EntsoePandasClient(api_key='909addb7-e4ae-4702-acc7-6b4f4fd9667b')
        data = client.query_load("ES", 
            start=pd.Timestamp(start, tz='UTC'), 
            end=pd.Timestamp(end, tz='UTC'))
        
        return data
    
    def check_fill_nans(data, date):
        """
        Inserts any missing times in the index and interpolates the missing value
        """
        idx = pd.date_range(start=f'{date}T0000', end=f'{date}T2300', freq='H').tz_localize('Europe/Madrid')
        data = data.append(pd.Series(np.NaN, index = idx.difference(data.index))).sort_index().interpolate()
        
        return data
        
    def upload_data_to_gcs(data, bucket_name, folder_name, file_name):
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(f'{folder_name}/{file_name}')
        blob.upload_from_string(data.to_json())
        
    
    params = request.get_json()
    
    if "get_data" in params and params['get_data']:
        
        start_time, end_time, date = generate_dates()
        
        file_name = gcs_file_name(start_time, end_time)
        
        data = query_entsoe_load(start_time, end_time)
        
        data = check_fill_nans(data, date)
        
        upload_data_to_gcs(data, BUCKET, FOLDER, file_name)