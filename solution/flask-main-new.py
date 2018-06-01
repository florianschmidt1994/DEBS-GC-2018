#!/usr/bin/env python3

import os
import numpy as np
import pandas as pd
import h5py
from flask import Flask, request
from sklearn.externals import joblib
from tensorflow.python.keras.models import load_model
from solution2.Predictor import Predictor
import sys
import logging
import warnings

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

current_dir = os.path.dirname(os.path.realpath(__file__))
port_path = os.path.join(current_dir, "datasets/ports_extended.csv")
filename1 = os.path.join(current_dir, "new_models/random_forest300.pkl")
filename2 = os.path.join(current_dir, "new_models/clusters.pkl")
filename3 = os.path.join(current_dir, "new_models/scaler.pkl")
filename4 = os.path.join(current_dir, "new_models/eta_mlp.h5")

port_predictor = joblib.load(filename1)
clusters = joblib.load(filename2)
scaler = joblib.load(filename3)
time_predictor = tf.keras.models.load_model(
    filename4,
    custom_objects=None,
    compile=True
)

# create predictor and system adapter
predictor = Predictor(port_predictor, time_predictor, scaler, clusters)

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
warnings.filterwarnings(action='ignore', category=DeprecationWarning)
app.run(host='0.0.0.0',port=port)
