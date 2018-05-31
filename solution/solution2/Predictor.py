import numpy as np
import pandas as pd
import datetime
import time
import tensorflow as tf
from math import radians, cos, sin, asin, sqrt
from haversine import haversine

ports = {'ALEXANDRIA': 0,
 'AUGUSTA': 1,
 'BARCELONA': 2,
 'CAGLIARI': 3,
 'CARTAGENA': 4,
 'CASTELLON': 5,
 'CEUTA': 6,
 'DAMIETTA': 7,
 'DILISKELESI': 8,
 'FOS SUR MER': 9,
 'FORMIA': 10,
 'GEMLIK': 11,
 'GENOVA': 12,
 'GIBRALTAR': 13,
 'JIJEL': 14,
 'HAIFA': 15,
 'IBIZA': 16,
 'ISKENDERUN': 17,
 'LAPSEKI': 18,
 'LIVORNO': '19',
 'LIMASSOL': 20,
 'MARSAXLOKK': 21,
 'MONACO': 22,
 'NEMRUT': 23,
 'PALERMO': 24,
 'PALMA DE MALLORCA': 25,
 'PIRAEUS': 26,
 'PIOMBINO': 27,
 'PLOCE': 28,
 'PORT SAID': 29,
 'SANT ANTONI': 30,
 'GIOIA TAURO': 31,
 'SKIKDA': 32,
 'ORAN ANCH': 33,
 'TARRAGONA': 34,
 'TEL AVIV-YAFO': 35,
 'TOBRUK': 36,
 'TRAPANI': 37,
 'TRIPOLIS': 38,
 'TUZLA': 39,
 'VALENCIA': 40,
 'VALLETTA': 41,
 'YALOVA': 42}


coordinates = {
 0: np.array([29.88,31.18]),
 1: np.array([15.21,37.2]),
 2: np.array([2.16,41.35]),
 3: np.array([9.11,39.21]),
 4: np.array([-0.97,37.58]),
 5: np.array([0.02,39.96]),
 6: np.array([-5.31,35.89]),
 7: np.array([31.76,31.47]),
 8: np.array([29.53,40.77]),
 9: np.array([4.87,43.42]),
 10: np.array([13.61,41.26]),
 11: np.array([29.12,40.43]),
 12: np.array([8.91,44.4]),
 13: np.array([-5.36,36.14]),
 14: np.array([5.78,36.82]),
 15: np.array([35.00,32.83]),
 16: np.array([1.44,38.91]),
 17: np.array([36.18,36.68]),
 18: np.array([26.69,40.35]),
 19: np.array([10.31,43.56]),
 20: np.array([33.03,34.66]),
 21: np.array([14.54,35.83]),
 22: np.array([7.43,43.74]),
 23: np.array([26.91,38.77]),
 24: np.array([13.37,38.13]),
 25: np.array([2.63,39.56]),
 26: np.array([23.61,37.95]),
 27: np.array([10.55,42.93]),
 28: np.array([17.44,43.04]),
 29: np.array([32.32,31.24]),
 30: np.array([1.3,38.98]),
 31: np.array([15.9,38.45]),
 32: np.array([6.92,36.89]),
 33: np.array([-0.63,35.75]),
 34: np.array([1.22,41.11]),
 35: np.array([34.77,32.09]),
 36: np.array([23.98,32.07]),
 37: np.array([12.51,38.01]),
 38: np.array([13.18,32.91]),
 39: np.array([29.29,40.83]),
 40: np.array([-0.32,39.44]),
 41: np.array([14.52,35.89]),
 42: np.array([29.48,40.72])
}



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
#TRIP_ID = 10  # 0xc35c9_10-03-15 12:xx - 10-03-15 13:26
ARRIVAL_CALC = 10  # ??
ARRIVAL_PORT_CALC = 11  # ??


class Predictor:
    def __init__(self, port_model, time_model, scaler):
        self.port_model = port_model
        self.time_model = time_model
        self.graph = tf.get_default_graph()
        self.ports = ports
        self.scaler = scaler

    def predict_port_and_time(self, data):
        # Parse string to array and convert all fields to numbers

        #coordinates = get_coordinates() #Preloading coordinates for each port

        #print("IN: %s" % data)
        feed_list = parse(data)
        initial_time = feed_list[6]  # we need this below. TODO: Fix this ugly entanglement

        port_features = feed_list[:6]+feed_list[7:]
        port_features = np.array(port_features)

        # Predict destination port
        port = self.port_model.predict(port_features.reshape(-1, 8))
        port = port[0]
        #output for Q1
        destination_port = int_to_port(port)

        ''' Order of features should be
        [ 'SHIPTYPE', 'SPEED', 'LON_x', 'LAT_x', 'COURSE', 'HEADING',
        'DEPARTURE_PORT_NAME', 'REPORTED_DRAUGHT',
        'ARRIVAL_PORT_CALC',  'LON_y', 'LAT_y', 'DISTANCE']
        '''

        # Select our input features for time prediction and append the predicted destination port to them
        feed_tuple = np.append(feed_list, port)
        feed_tuple = np.append(feed_tuple, coordinates.get(port))
        feed_tuple = feed_tuple.astype(np.float)
        distance = haversine(feed_tuple[1], feed_tuple[2], feed_tuple[-2], feed_tuple[-1])
        feed_tuple = np.append(feed_tuple, distance)
        # Predict the ETA
        feed_tuple = [0 if v == 'nan' else v  for v in feed_tuple]
        with self.graph.as_default():
            time_left = self.time_model.predict(self.scaler.transform(np.asarray(feed_tuple).reshape(1, -1)))

        eta = initial_time + time_left
        #print("OUT: %s %s" % (destination_port,from_unixtime(eta)))
        return destination_port, from_unixtime(eta)
        

def parse(input_str):
    data = input_str.replace("\'","").split(",")
    data = [0 if v == 'nan' else v  for v in data]
    parsed_data = [
        data[SHIPTYPE],
        data[SPEED],
        data[LON],
        data[LAT],
        data[COURSE],
        data[HEADING],
        to_unixtime(data[TIMESTAMP]),
        port_to_int(data[DEPARTURE_PORT_NAME]),
        data[REPORTED_DRAUGHT]
    ]
    return [0 if elem is '' else elem for elem in parsed_data]

def predictQ2(parsed_tuple, port):
    '''
    [ 'SHIPTYPE', 'SPEED', 'LON_x', 'LAT_x', 'COURSE', 'HEADING',
        'DEPARTURE_PORT_NAME', 'REPORTED_DRAUGHT',
        'ARRIVAL_PORT_CALC',  'LON_y', 'LAT_y', 'DISTANCE'
        ]
    '''
    parsed_tuple = np.append(parsed_tuple,port)
    parsed_tuple = np.append(parsed_tuple,coordinates.get(port))
    #np.append(feed_tuple,coordinates.get(port[0]))
    #check assert

    time_left = time_predictor.predict(parsed_tuple.reshape(-1, 12))
    #pd.to_datetime(data[-3]) + datetime.timedelta(minutes=time_left[0])

    timestamp = to_unixtime(data[-3])
    eta = timestamp + time_left
    port = unparse(port)
    return port, from_unixtime(eta)


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

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

