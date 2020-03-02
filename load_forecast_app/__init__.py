from flask import Flask

app = Flask(__name__)

from load_forecast_app import routes
