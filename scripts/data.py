import pandas as pd
import numpy as np
import plotly.graph_objs as go
import plotly.colors
from collections import OrderedDict
import requests
from entsoe import EntsoePandasClient
import os
from datetime import datetime, timedelta


def get_access_token():

    assert 'ENTSOE_TOKEN' in os.environ,'ENTSOE Access token not set in environment varibale.'
    
    return os.environ.get('ENTSOE_TOKEN')


# def get_date_range(date=None, period=None):

#     if date == None:
#         date = datetime.today()+timedelta(1)

#     if period==None:
#         delta = timedelta(14)
#     else:
#         delta = timedelta(period)

#     end = pd.Timestamp(date.strftime('%Y%m%d'), tz='UTC')
#     start = pd.Timestamp((date - delta).strftime('%Y%m%d'), tz='UTC')

#     return start, end


# def get_entsoe_data(country='ES', date=None, period=None):


#     start, end = get_date_range()

#     client = EntsoePandasClient(api_key=get_access_token())

#     data = client.query_load(country, start=start, end=end)

#     return data


# def load_data():

#     df_load = get_entsoe_data()

#     df_preds = make_predictions(df_load)

#     return df_load, df_preds

# def make_predictions(df, date=None, period=None):

#     #move forward 24 hours. persistance model.
#     df = df.shift(24)

#     return df


def request_graph_data():
    
    url="https://us-central1-ml-energy-dashboard.cloudfunctions.net/return-load"
    result = requests.post(url, json={"key": str(get_access_token())})

    df_load = pd.read_json(result.json()['df_load'], typ='series', orient='index')
    df_preds = pd.read_json(result.json()['df_preds'], typ='series', orient='index')

    return df_load, df_preds

def return_figures():
    """Creates four plotly visualizations using the World Bank API

    # Example of the World Bank API endpoint:
    # arable land for the United States and Brazil from 1990 to 2015
    # http://api.worldbank.org/v2/countries/usa;bra/indicators/AG.LND.ARBL.HA?date=1990:2015&per_page=1000&format=json

    Args:
        country_default (dict): list of countries for filtering the data

    Returns:
        list (dict): list containing the four plotly visualizations

    """

    # first chart plots arable land from 1990 to 2015 in top 10 economies 
    # as a line chart
    graph_one = list()
    graph_two = list()

    df_one, df_two = request_graph_data()

    # filter and sort values for the visualization
    # filtering plots the countries in decreasing order by their values
    # df_one = df_one[(df_one['date'] == '2015') | (df_one['date'] == '1990')]
    # df_one.sort_values('value', ascending=False, inplace=True)

    # this  country list is re-used by all the charts to ensure legends have the same
    # order and color

 
    graph_one.append( 
    go.Scatter(
      x = df_one.index,
      y = df_one.values,
      mode = 'lines',
      name = 'Actual Load'
      )
    )

    layout_one = dict(title = 'ES Energy Demand', 
        xaxis = dict(title = 'Time', autotick=True), 
        yaxis = dict(title = 'Load MWh', autotick=True))

    graph_one.append( 
    go.Scatter(
      x = df_two.index,
      y = df_two.values,
      mode = 'lines',
      name='Persistance: Forecast'
      )
    )


    # append all charts
    figures = []
    figures.append(dict(data=graph_one, layout=layout_one))

    return figures
