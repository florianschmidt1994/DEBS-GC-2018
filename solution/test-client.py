#!/usr/bin/env python3

import requests
import time

filepath = '../data/debs2018_second_dataset_training_labeled_v7.csv' 
with open(filepath) as fp: 
    line = fp.readline()
    cnt = 1
    while line:
        line = fp.readline()
        lineWoLabel = (",").join(line.split(",")[:-2])
        
        r = requests.post("http://localhost:5000/predict_port_and_time", data=lineWoLabel)
        print(r.text)
        print(r.status_code)
        #print(line)
        #print(lineWoLabel)
        time.sleep(.300)
        cnt += 1
