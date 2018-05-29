#!/usr/bin/env python3

import os

from flask import Flask, request
from sklearn.externals import joblib
from solution.Predictor import Predictor
import sys
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

current_dir = os.path.dirname(os.path.realpath(__file__))
port_predictor_file = os.path.join(current_dir, "old_models/port_predictor.pkl")
time_predictor_file = os.path.join(current_dir, "old_models/minutes_predictor.pkl")
port_predictor = joblib.load(port_predictor_file)
time_predictor = joblib.load(time_predictor_file)
# create predictor and system adapter
predictor = Predictor(port_predictor, time_predictor)


@app.route("/", methods=['GET'])
def index():
    return "HEALTHY"


@app.route("/predict_port", methods=['POST'])
def predict_port():
    input = str(request.data)
    port, time = predictor.predict_port_and_time(input)
    return port


@app.route("/predict_port_and_time", methods=['POST'])
def predict_port_and_time():
    input = str(request.data)
    port, time = predictor.predict_port_and_time(input)
    return "%s, %s" % (port, time)

port = int(sys.argv[1])
app.run(host='0.0.0.0',port=port)
