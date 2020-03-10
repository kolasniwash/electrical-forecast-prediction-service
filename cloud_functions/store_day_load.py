def store_day_load(request):
    from entsoe import EntsoePandasClient
    from datetime import datetime, timedelta
    import pandas as pd
    from google.cloud import storage
    
    FOLDER = 'raw-days'
    BUCKET = 'ml-energy-dashboard-raw-data'
    
    def generate_date():
        return (datetime.today()+timedelta(-1)).strftime('%Y%m%d')
    
    def gcs_file_name(date):
        return f'es-energy-demand-raw-{date}'
    
    def query_entsoe_load(date):
        
        client = EntsoePandasClient(api_key='909addb7-e4ae-4702-acc7-6b4f4fd9667b')
        data = client.query_load("ES", start=pd.Timestamp(f"{date}T0000", tz='UTC'), end=pd.Timestamp(f"{date}T2300", tz='UTC'))
        
        return data
    
    def upload_data_to_gcs(data, date, bucket_name, folder_name, file_name):
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(f'{folder_name}/{file_name}')
        blob.upload_from_string(data.to_json())
        
    
    params = request.get_json()
    
    if "get_data" in params and params['get_data']:
        
        date = generate_date()
        
        file_name = gcs_file_name(date)
        
        data = query_entsoe_load(date)
        
        upload_data_to_gcs(data, date, BUCKET, FOLDER, file_name)
        
        
        