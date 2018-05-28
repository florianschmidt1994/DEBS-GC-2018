import io
import json
import os
import sys

import bitstring
import pika
from sdk import RabbitMQUtils
import logging

SYSTEM_READY_SIGNAL = 1
BENCHMARK_READY_SIGNAL = 2
DATA_GENERATOR_READY_SIGNAL = 3
TASK_GENERATOR_READY_SIGNAL = 4
EVAL_STORAGE_READY_SIGNAL = 5
EVAL_MODULE_READY_SIGNAL = 6
DATA_GENERATOR_START_SIGNAL = 7
TASK_GENERATOR_START_SIGNAL = 8
EVAL_MODULE_FINISHED_SIGNAL = 9
EVAL_STORAGE_TERMINATE = 10
BENCHMARK_FINISHED_SIGNAL = 11
DOCKER_CONTAINER_START = 12
DOCKER_CONTAINER_STOP = 13
DATA_GENERATION_FINISHED = 14
TASK_GENERATION_FINISHED = 15
DOCKER_CONTAINER_TERMINATED = 16
START_BENCHMARK_SIGNAL = 17
REQUEST_SYSTEM_RESOURCES_USAGE = 18

# an array of commands so we can do a lookup of the command name by index
commands = [
    "UNKNOWN",
    "SYSTEM_READY_SIGNAL",
    "BENCHMARK_READY_SIGNAL",
    "DATA_GENERATOR_READY_SIGNAL",
    "TASK_GENERATOR_READY_SIGNAL",
    "EVAL_STORAGE_READY_SIGNAL",
    "EVAL_MODULE_READY_SIGNAL",
    "DATA_GENERATOR_START_SIGNAL",
    "TASK_GENERATOR_START_SIGNAL",
    "EVAL_MODULE_FINISHED_SIGNAL",
    "EVAL_STORAGE_TERMINATE",
    "BENCHMARK_FINISHED_SIGNAL",
    "DOCKER_CONTAINER_START",
    "DOCKER_CONTAINER_STOP",
    "DATA_GENERATION_FINISHED",
    "TASK_GENERATION_FINISHED",
    "DOCKER_CONTAINER_TERMINATED",
    "START_BENCHMARK_SIGNAL",
    "REQUEST_SYSTEM_RESOURCES_USAGE"
]

COMMAND_EXCHANGE_NAME = 'hobbit.command'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AbstractSystemAdapter:
    connection = None

    commandChannel = None
    dataGenChannel = None
    taskGenChannel = None
    evalStorageChannel = None

    num_channels_ready = 0

    delayedSend = []

    commandQueue = None

    dataGenQueueName = None
    taskGenQueueName = None
    evalStorageQueueName = None

    systemParamModel = None

    query_type = None

    predictor = None

    session_id = None

    def __init__(self, predictor, imageName):
        self.imageName = imageName
        self.predictor = predictor
        self.count = 0

        # from former super
        parameters = pika.ConnectionParameters(host='rabbit')
        self.session_id = os.environ["HOBBIT_SESSION_ID"]
        self.connection = pika.SelectConnection(parameters, self.on_connection_open)

        # from self
        self.dataGenQueueName = 'hobbit.datagen-system.' + self.session_id
        self.taskGenQueueName = 'hobbit.taskgen-system.' + self.session_id
        self.evalStorageQueueName = 'hobbit.system-evalstore.' + self.session_id
        print("evalStorageQueueName %s " % self.evalStorageQueueName)

        self.systemParamModel = os.environ["SYSTEM_PARAMETERS_MODEL"]
        logger.info("Setting up SystemAdapter with parameters '%s'" % str(self.systemParamModel).replace("\n", ""))
        self.query_type = int(json.loads(self.systemParamModel)["queryType"])
        logger.info("Setting query type to %s" % self.query_type)

        self.run()

    def on_connection_open(self, connection):
        logger.info("Successfully opened connection to RabbitMQ host. Opening channels now...")
        self.commandChannel = connection.channel(self.on_command_channel_open)
        self.dataGenChannel = connection.channel(self.on_data_channel_open)
        self.taskGenChannel = connection.channel(self.on_task_channel_open)
        self.evalStorageChannel = connection.channel(self.on_eval_store_channel_open)

    def on_data_channel_open(self, channel):
        self.dataGenChannel.queue_declare(queue=self.dataGenQueueName,
                                          callback=self.on_data_queue_declared,
                                          auto_delete=True)

    def on_data_queue_declared(self, result):
        self.dataGenChannel.basic_consume(self.on_data_received,
                                          queue=self.dataGenQueueName,
                                          no_ack=True)
        self.on_channel_ready(self.dataGenChannel)

    def on_task_channel_open(self, channel):
        self.taskGenChannel.queue_declare(queue=self.taskGenQueueName,
                                          callback=self.on_task_queue_declared,
                                          auto_delete=True)

    def on_task_queue_declared(self, result):
        self.taskGenChannel.basic_consume(self.on_task_received,
                                          queue=self.taskGenQueueName,
                                          no_ack=True)
        self.on_channel_ready(self.taskGenChannel)

    def on_eval_store_channel_open(self, channel):
        self.evalStorageChannel.queue_declare(queue=self.evalStorageQueueName,
                                              callback=self.on_eval_store_queue_declared,
                                              auto_delete=True)

    def on_eval_store_queue_declared(self, result):
        self.on_channel_ready(self.evalStorageChannel)

    def on_channel_ready(self, channel):
        self.num_channels_ready += 1
        if self.num_channels_ready == 4:
            logger.info("All four channels are marked as ready. Sending SYSTEM_READY_SIGNAL to command queue")
            self.send_command_to_queue(SYSTEM_READY_SIGNAL, None)

    def handle_command(self, command, buffer):
        logger.info("Received command %s", commands[command])
        if command == TASK_GENERATION_FINISHED:
            logger.info("Sending DOCKER_CONTAINER_TERMINATED signal")

            stream = bitstring.BitStream()
            stream.append(b"")
            stream.append("int:32=" + str(len(self.imageName)))
            stream.append(bytearray(self.imageName, encoding="utf-8"))
            stream.append("int:8=" + str(0))
            self.send_command_to_queue(DOCKER_CONTAINER_TERMINATED, stream.bytes)
            self.terminate()

        elif command == DOCKER_CONTAINER_START:
            raw_payload = RabbitMQUtils.readString(buffer)
            payload = json.loads(raw_payload)
            if self.imageName == payload["image"]:

                logger.info("Showing environment variables from payload")
                for var in payload["environmentVariables"]:
                    logger.info(var)
                    splitted = var.split("=")
                    os.environ[splitted[0]] = splitted[1]

        #elif command == TASK_GENERATION_FINISHED:
            #self.terminate()

        else:
            logger.info("Not handling command %s", commands[command])

    def on_task_received(self, ch, method, properties, body):
        buffer = io.BytesIO(body)
        taskId = RabbitMQUtils.readString(buffer)
        taskData = RabbitMQUtils.readString(buffer)
        self.handle_task(taskId, taskData)

    def on_data_received(self, ch, method, properties, body):
        logger.info("Received data %s . Doing nothing with it" % body)

    def send_result_to_eval_store(self, task_id, data):
        stream = bitstring.BitStream()
        stream.append("int:32=" + str(len(task_id)))
        stream.append(bytearray(task_id, encoding="utf-8"))
        stream.append("int:32=" + str(len(data)))
        stream.append(bytearray(data, encoding="utf-8"))

        try:
            self.evalStorageChannel.basic_publish(exchange="", routing_key=self.evalStorageQueueName, body=stream.bytes)
        except Exception as e:
            print(" Sending failed: ")

    def explain(self, input):
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

        splitted = input.split(",")
        print("SHIP_ID=", splitted[SHIP_ID])
        print("SHIP_TYPE=", splitted[SHIPTYPE])
        print("SPEED=", splitted[SPEED])
        print("LON=", splitted[LON])
        print("LAT=", splitted[LAT])
        print("COURSE=", splitted[COURSE])
        print("HEADING=", splitted[HEADING])
        print("TIMESTAMP=", splitted[TIMESTAMP])
        print("DEPARTURE_PORT_NAME=", splitted[DEPARTURE_PORT_NAME])
        print("REPORTED_DRAUGHT=", splitted[REPORTED_DRAUGHT])
        print("TRIP_ID=", splitted[TRIP_ID])
        print("ARRIVAL_CALC=", splitted[ARRIVAL_CALC])
        print("ARRIVAL_PORT_CALC=", splitted[ARRIVAL_PORT_CALC])

    def handle_task(self, task_id, data):
        # TODO: Handle query type
       # self.explain(data)
        try:
            if self.count % 1000 == 0:
                print("Received %s tasks" % self.count)

            if self.query_type == 1:
                port, time = self.predictor.predict_port_and_time(data)
                self.send_result_to_eval_store(task_id, port + "\n")
            elif self.query_type == 2:
                port, time = self.predictor.predict_port_and_time(data)
                res = port + "," + time
                self.send_result_to_eval_store(task_id, res)
            else:
                raise Exception("Unknown query type %s" % self.query_type)

        except Exception as e:
            # TODO: Handle this
            print("An exception occurred!", e)
            self.send_result_to_eval_store(task_id, "ALEXANDRIA, 03-05-15 1:33")

        self.count = self.count + 1

    # the channel was opened
    def on_command_channel_open(self, channel):
        print("on_command_channel_open")
        # create / join an exchange
        channel.exchange_declare(exchange=COMMAND_EXCHANGE_NAME,
                                 exchange_type="fanout",
                                 callback=self.on_command_exchange_declared,
                                 durable=False,
                                 auto_delete=True)

    # the exchange was declared
    def on_command_exchange_declared(self, result):
        print("on_command_exchange_declared")
        # declare queue if needed
        self.commandChannel.queue_declare(callback=self.on_command_queue_declared,
                                          exclusive=True)

    # queue was declared
    def on_command_queue_declared(self, result):
        print("on_command_queue_declared")
        self.commandQueue = result.method.queue
        # bind queue to exchange
        self.commandChannel.queue_bind(queue=self.commandQueue,
                                       exchange=COMMAND_EXCHANGE_NAME,
                                       callback=self.on_command_queue_bound)

    # command was bound to queue
    def on_command_queue_bound(self, result):
        print("on_command_queue_bound")
        self.commandChannel.basic_consume(self.on_command_received, queue=self.commandQueue, no_ack=True)
        self.on_channel_ready(self.commandChannel)

    def send_command_to_queue(self, command, data: bytes):

        logger.info("Sending command %s to queue", commands[command])

        stream = bitstring.BitStream()
        stream.append("int:32=" + str(len(self.session_id)))
        stream.append(bytearray(self.session_id, encoding="utf-8"))
        stream.append("int:8=" + str(command))

        if data is not None:
            stream.append(data)

        self.commandChannel.basic_publish(exchange=COMMAND_EXCHANGE_NAME, routing_key="", body=stream.bytes)

    def run(self):
        try:
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            self.terminate()

    def terminate(self):
        print("Closing connections!")
        self.connection.close()

        print("Time to say goodbye! :( ")
        sys.exit(0)

    def print_bytes(self, string):
        hex = bytearray(string, "utf-8").hex()
        print(hex)

    def on_command_received(self, ch, method, properties, body):
        buffer = io.BytesIO(body)
        receiver_session_id = RabbitMQUtils.readString(buffer)
        command = RabbitMQUtils.readByte(buffer)

        if receiver_session_id == self.session_id:
            self.handle_command(command, buffer)
        else:
            print("Ignoring command %s that belongs to session %s" % (command, receiver_session_id))
