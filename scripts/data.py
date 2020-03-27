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

def request_graph_data():
    
    url="https://us-central1-ml-energy-dashboard.cloudfunctions.net/comb-all-data"
    result = requests.post(url, json={"download": 'true', "load_period": 6, "persist_period": 7, "keras_forecast": 7})

    df_load = pd.read_json(result.json()['df_loads'], typ='series', orient='index')
    df_load = df_load.tz_localize('UTC').tz_convert('Europe/Madrid')
    df_naive = pd.read_json(result.json()['df_naive'], typ='series', orient='index')
    df_MA3_hbh = pd.read_json(result.json()['df_MA3_hbh'], typ='series', orient='index')
    df_keras_forecast = pd.read_json(result.json()['keras_forecast'])['keras_loads']

    return df_load, df_naive, df_MA3_hbh, df_keras_forecast

def return_figures():
    """Creates plotly visualizations using GCP endpoint

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

    df_one, df_two, df_four, df_five = request_graph_data()

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

    layout_one = dict(title = 'Ground Truth & Forecast Energy Demand: Last 7 Days',
        xaxis = dict(title = 'Time', autotick=True), 
        yaxis = dict(title = 'Load MWh', autotick=True))

    graph_one.append( 
    go.Scatter(
        x = df_two.index,
        y = df_two.values,
        mode = 'lines',
        opacity = 0.3,
        name='Persistance: Naive'
      )
    )

    graph_one.append(
        go.Scatter(
            x=df_four.index,
            y=df_four.values,
            mode='lines',
            opacity=0.3,
            name='Moving Average: Last 3 Days'))

    graph_one.append(
        go.Scatter(
            x=df_five.index,
            y=df_five.values,
            mode='lines',
            opacity=0.3,
            name='Neural Network: LSTM Univariate'))


    # append all charts
    figures = []
    figures.append(dict(data=graph_one, layout=layout_one))

    return figures
