import math
import os
import subprocess
import time
from threading import Thread

from sklearn.externals import joblib

from sdk.AbstractSystemAdapter import AbstractSystemAdapter
from solution.Predictor import Predictor


def start_benchmark(image_name, session_id):
    main_class = 'org.hobbit.debs_2018_gc_samples.System.SampleSystemTestRunner'
    jar_file = "target/debs_2018_gc_sample_system-1.0.jar"
    work_dir = "/Users/florianschmidt/dev/SELab/DEBS-GC-2018"
    command = "java -cp {} {} {} {}".format(jar_file, main_class, image_name, session_id)

    try:
        process = subprocess.Popen([command], cwd=work_dir, shell=True, stdout=subprocess.PIPE)
        for line in iter(process.stdout.readline, ''):
            print(line, flush=True)
            if "[org.hobbit.sdk.utils.CommandQueueListener] - <Terminated>" in str(line):
                break
        process.kill()
    except Exception as e:
        print(e, flush=True)


def main():
    # TODO: check/change it
    # Setup environment and config
    print("###### ENV ######", flush=True)
    for key, value in os.environ.items():
        print("%s=%s" % (key, value), flush=True)
    print("###### ENV ENV ######\n", flush=True)

    print("Starting python benchmark solution", flush=True)

    image_name = "git.project-hobbit.eu:4567/florian.schmidt.1994/debs2018solution/system-adapter:latest"

    # Start benchmark thread
    try:
        # try to access env variable, this will raise KeyError if it doesn't exist
        os.environ["LOCAL_TEST"]

        session_id = "session_" + str(math.floor(time.time()))
        os.environ["HOBBIT_SESSION_ID"] = session_id

        thread = Thread(target=start_benchmark, args=(image_name, session_id), daemon=True)
        thread.start()
        print("Started benchmark thread", flush=True)
    except KeyError:
        print("Not starting local benchmark because env variabe LOCAL_TEST is not set", flush=True)

    # Load models from file system
    current_dir = os.path.dirname(os.path.realpath(__file__))
    port_predictor_file = os.path.join(current_dir, "old_models/port_predictor.pkl")
    time_predictor_file = os.path.join(current_dir, "old_models/minutes_predictor.pkl")
    port_predictor = joblib.load(port_predictor_file)
    time_predictor = joblib.load(time_predictor_file)

    # create predictor and system adapter
    predictor = Predictor(port_predictor, time_predictor)
    sanity_check(predictor)

    # this will block and keep running until it terminates itself
    system_adapter = AbstractSystemAdapter(predictor, image_name)

    print("We're done. Shutting down!", flush=True)


def sanity_check(predictor):
    try:
        input_str = "0x7086e9bed7ea1ca1b4ec7ea3955ef732792d29b8,30,6.4,14.50771,35.88743,52,511,11-05-15 3:29,MARSAXLOKK,"
        predictor.predict_port_and_time(input_str)
        print("Sanity check successful: Loaded models can predict values from sample input", flush=True)
    except Exception as e:
        raise Exception("Sanity check failed: can not predict port and time with sample input", e)


if __name__ == "__main__":
    main()
