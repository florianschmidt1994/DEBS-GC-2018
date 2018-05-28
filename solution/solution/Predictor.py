import datetime
import numpy as np
import time

ports = {"BARCELONA": 1,
         "PALMA DE MALLORCA": 2,
         "GIBRALTAR": 3,
         "VALENCIA": 4,
         "LIVORNO": 5,
         "NEMRUT": 6,
         "TARRAGONA": 7,
         "GENOVA": 8,
         "VALLETTA": 9,
         "ISKENDERUN": 10,
         "FOS SUR MER": 11,
         "MARSAXLOKK": 12,
         "CEUTA": 13,
         "AUGUSTA": 14,
         "ALEXANDRIA": 15,
         "CARTAGENA": 16,
         "PIRAEUS": 17,
         "HAIFA": 18,
         "DAMIETTA": 19,
         "PORT SAID": 20,
         "GEMLIK": 21,
         "DILISKELESI": 22,
         "TUZLA": 23,
         "YALOVA": 24,
         "MONACO": 25
         }

# indices
SHIP_ID = 0  # 0xc35c9ebbf48cbb5857a868ce441824d0b2ff783a
SHIPTYPE = 1  # 99
SPEED = 2  # 8.2
LON = 3  # 14.56034
LAT = 4  # 35.8109
COURSE = 5  # 109
HEADING = 6  # 511
TIMESTAMP = 7  # 10-03-15 12:15
DEPARTURE_PORT_NAME = 8  # MARSAXLOKK
REPORTED_DRAUGHT = 9  # ''
TRIP_ID = 10  # 0xc35c9_10-03-15 12:xx - 10-03-15 13:26
ARRIVAL_CALC = 11  # ??
ARRIVAL_PORT_CALC = 12  # ??


class Predictor:
    def __init__(self, port_model, time_model):
        self.port_model = port_model
        self.time_model = time_model
        self.ports = ports

    def predict_port_and_time(self, data):
        # Parse string to array and convert all fields to numbers
        feed_list = parse(data)
        initial_time = feed_list[6]  # we need this below. TODO: Fix this ugly entanglement
        feed_list = np.asarray(feed_list)

        # Predict destination port
        port = self.port_model.predict(feed_list.reshape(-1, 8))
        port = port[0]
        port = int_to_port(port)

        # Select our input features for time prediction and append the predicted destination port to them
        feed_tuple = np.append(feed_list[[0, -1, -2]], port_to_int(port))

        # Predict the ETA
        time_left = self.time_model.predict(feed_tuple.reshape(-1, 4))[0]
        eta = initial_time + time_left

        return port, from_unixtime(eta)


def parse(input_str):
    data = input_str.split(",")
    parsed_data = [
        data[SHIPTYPE],
        data[SPEED],
        data[LON],
        data[LAT],
        data[COURSE],
        data[HEADING],
        to_unixtime(data[TIMESTAMP]),
        port_to_int(data[DEPARTURE_PORT_NAME]),
        # data[REPORTED_DRAUGHT]
    ]
    return [0 if elem is '' else elem for elem in parsed_data]


def port_to_int(port_name):
    return ports.get(port_name)


def int_to_port(data):
    for key, value in ports.items():
        if data == value:
            return key


def to_unixtime(value):
    try:
        return time.mktime(time.strptime(value, "%d-%m-%y %H:%M"))
    except:
        try:
            return time.mktime(time.strptime(value, "%d-%m-%Y %H:%M"))
        except:
            raise Exception("Unable to parse date '%s'" % value)


def from_unixtime(time):
    return datetime.datetime.fromtimestamp(time).strftime('%d-%m-%Y %H:%M')
