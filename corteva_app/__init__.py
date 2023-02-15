import os
import json

from flask import Flask, request, Response
from . import db
from . import handle_api
import logging


# Run once at startup:
logging.basicConfig(filename='app.log',
                level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'corteva_app.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    db.init_app(app)
    @app.route('/')
    def hello():
        return 'This is Corteva Code Assessment by Satish Mandal.'

    @app.route('/api/weather', methods=['GET', 'POST'])
    def api_weather():
        try:
            request_args = request.args
            date, station, page_num, page_size = '','',1, 10
            date = request_args.get("date")
            station = request_args.get("station")
            page_num = request_args.get("page_num")
            page_size = request_args.get("page_size")
            resp = handle_api.get_data(date, station, page_num, page_size, table = "WX_DATA")
            return Response(json.dumps(resp), 200, content_type='application/json')
        except Exception as e:
            return Response(json.dumps({}), 500, content_type='application/json')

    @app.route('/api/weather/stats', methods=['GET', 'POST'])
    def api_weather_stats():
        try:
            request_args = request.args
            date, station, page_num, page_size = '','',1, 10
            date = request_args.get("date")
            station = request_args.get("station")
            page_num = request_args.get("page_num")
            page_size = request_args.get("page_size")

            resp = handle_api.get_data(date, station, page_num, page_size, table = "WX_STATS")
            return Response(json.dumps(resp), 200, content_type='application/json')
        except Exception as e:
            return Response(json.dumps({}), 500, content_type='application/json')
    return app