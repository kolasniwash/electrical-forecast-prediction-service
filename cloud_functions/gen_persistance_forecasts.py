def gen_persistance_forecasts(request):
    from datetime import datetime, timedelta
    import pandas as pd
    from google.cloud import storage
    
    FOLDER_DOWN = 'raw-days'
    FOLDER_UP = 'persistance_forecasts'
    BUCKET = 'ml-energy-dashboard-raw-data'
    
    def persistence_date():
        return datetime.today().strftime('%Y%m%d')
    
    def raw_data_date():
        return (datetime.today()+timedelta(-1)).strftime('%Y%m%d')
    
    def gcs_save_name(date):
        return f'es-persistance-forecasts-{date}'
    
    def gcs_load_name(date):
        return f'es-energy-demand-raw-{date}'
    
    def get_gcs_data(client, date, bucket_name, folder_name, file_name):
        
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(f'{folder_name}/{file_name}')
        data_json = blob.download_as_string()
 
        return pd.read_json(data_json, typ='series', orient='records')
    
    def upload_data_to_gcs(client, data, date, bucket_name, folder_name, file_name):
        
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(f'{folder_name}/{file_name}')
        blob.upload_from_string(data.to_json())
        
    params = request.get_json()
    
    if 'gen_persist' in params and params['gen_persist']:
        storage_client = storage.Client()

        #download the data to make the persistance forecast
        date_to_download = raw_data_date()
        file_to_download = gcs_load_name(date_to_download)
        data = get_gcs_data(storage_client, date_to_download, BUCKET, FOLDER_DOWN, file_to_download)


        #calculate the persistance forecasts and upload to gcs
        date_to_forecast = persistence_date()
        file_to_forecast = gcs_save_name(date_to_forecast)
        #create persistance - simple persistance today's values >> tomorrow forecast
        upload_data_to_gcs(storage_client, data, date_to_forecast, BUCKET, FOLDER_UP, file_to_forecast)
