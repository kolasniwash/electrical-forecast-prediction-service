def load_and_persist(request):
    from flask import jsonify
    from entsoe import EntsoePandasClient
    from datetime import datetime, timedelta
    import pandas
    
    def get_date_range(date=None, period=None):

        if date == None:
            date = datetime.today()+timedelta(1)

        if period==None:
            delta = timedelta(14)
        else:
            delta = timedelta(period)

        end = pandas.Timestamp(date.strftime('%Y%m%d'), tz='UTC')
        start = pandas.Timestamp((date - delta).strftime('%Y%m%d'), tz='UTC')

        return start, end


    def get_entsoe_data(key, country='ES', date=None, period=None):


        start, end = get_date_range()

        client = EntsoePandasClient(api_key=key)

        data = client.query_load(country, start=start, end=end)

        return data

    def make_predictions(df, date=None, period=None):

        #move forward 24 hours. persistance model.
        df = df.shift(24)

        return df

    def load_data(key):

        assert type(key)==str, 'please pass your entsoe access code as string'
        df_load = get_entsoe_data(key)

        df_preds = make_predictions(df_load)

        return df_load, df_preds

    
    data = {"success": False}
    params = request.get_json()
    
    if "key" in params:
        
        df_load, df_preds = load_data(params["key"])
        
        data['df_load'] = str(df_load.to_json())
        data['df_preds'] = str(df_preds.to_json())
        data["success"] = True
                            
    return jsonify(data)