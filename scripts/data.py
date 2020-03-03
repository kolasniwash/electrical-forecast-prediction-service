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


def load_data(country='ES', date=None, period=None):

    if date == None:
        date = datetime.today()+timedelta(1)

    if period==None:
        delta = timedelta(14)
    else:
        delta = timedelta(period)


    end = pd.Timestamp(date.strftime('%Y%m%d'), tz='UTC')
    start = pd.Timestamp((date - delta).strftime('%Y%m%d'), tz='UTC')

    client = EntsoePandasClient(api_key=get_access_token())

    data = client.query_load(country, start=start, end=end)

    return data






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
    graph_one = []

    df_one = load_data()

    # filter and sort values for the visualization
    # filtering plots the countries in decreasing order by their values
    # df_one = df_one[(df_one['date'] == '2015') | (df_one['date'] == '1990')]
    # df_one.sort_values('value', ascending=False, inplace=True)

    # this  country list is re-used by all the charts to ensure legends have the same
    # order and color

    x_val = df_one.index.tolist()
    y_val =  df_one.values.tolist()
    graph_one.append( 
    go.Scatter(
      x = x_val,
      y = y_val,
      mode = 'lines'
      )
    )

    layout_one = dict(title = 'ES Energy Demand',
                xaxis = dict(title = 'Time',
                  autotick=True),
                yaxis = dict(title = 'Load MWh'),
                )

    # append all charts
    figures = []
    figures.append(dict(data=graph_one, layout=layout_one))

    return figures
