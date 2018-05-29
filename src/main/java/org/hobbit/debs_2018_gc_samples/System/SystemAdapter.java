package org.hobbit.debs_2018_gc_samples.System;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.sdk.JenaKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hobbit.debs_2018_gc_samples.Constants.QUERY_TYPE_KEY;


public class SystemAdapter extends AbstractSystemAdapter {
    private static final String HOBBIT_SYSTEM_CONTAINER_ID_KEY = "";
    private static final String PREDICTOR_PORT_URL = "http://localhost:5000/predict_port";
    private static JenaKeyValue parameters;

    private int queryType = -1;
    private final Map<String, Integer> tuplesPerShip = new HashMap<String, Integer>();

    private Logger logger = LoggerFactory.getLogger(SystemAdapter.class);

    long lastReportedValue = 0;
    long tuplesReceived=0;
    long errors=0;
    int systemContainerId = 0;
    int systemInstancesCount = 1;

    private OkHttpClient client;
    private Process predictorProcess;

    private Map<String, String> portLookup;
    private Map<String, String> timeLookup;

    private boolean useMock = true;

    private String keyForRow(String row) {
        String[] splitted = row.split(",");
        return splitted[0]
            + splitted[1]
            + splitted[2]
            + splitted[3]
            + splitted[4]
            + splitted[5]
            + splitted[6]
            + splitted[7];
    }

    private String extractTimeFromFow(String row) {
        return row.split(",", -1)[12];
    }

    private String extractPortFromRow(String row) {
        return row.split(",", -1)[11];
    }

    @Override
    public void init() throws Exception {
        super.init();

        try {
            unsafeInit();
        } catch (Exception e) {
            logger.error("An exception happened during init", e);
            e.printStackTrace();

            throw new RuntimeException(e);
        }

    }

    private void unsafeInit() throws Exception {

        if (useMock) {
            this.portLookup = Files
                .readAllLines(Paths.get("data", "debs2018_second_dataset_training_labeled_v7.csv"))
                .stream()
                .collect(Collectors.toMap(this::keyForRow, this::extractTimeFromFow, (p1, p2) -> p2));

            this.timeLookup = Files
                .readAllLines(Paths.get("data", "debs2018_second_dataset_training_labeled_v7.csv"))
                .stream()
                .collect(Collectors.toMap(this::keyForRow, this::extractPortFromRow, (p1, p2) -> p2));
        } else {
            this.predictorProcess = spawnPredictorProcess();
            client = new OkHttpClient();
        }

        // Your initialization code comes here...
        parameters = new JenaKeyValue.Builder().buildFrom(systemParamModel);

        if (parameters.containsKey(HOBBIT_SYSTEM_CONTAINER_ID_KEY)) {
            systemContainerId = parameters.getIntValueFor(HOBBIT_SYSTEM_CONTAINER_ID_KEY);
        }

        queryType = parameters.getIntValueFor(QUERY_TYPE_KEY);
        if(queryType<=0){
            Exception ex = new Exception("Query type is not specified correctly");
            logger.error(ex.getMessage());
            throw ex;
        }

        logger.debug("SystemModel: "+parameters.encodeToString());
        logger.info("Finished initializing!");
    }

    private Process spawnPredictorProcess() {
        try {
            return new ProcessBuilder("/Users/florianschmidt/dev/SELab/DEBS-GC-2018/solution/flask-main.py")
//            return new ProcessBuilder("/usr/src/debs2018solution/solution/flask-main.py")
                .inheritIO()
                .start();
        } catch (IOException e) {
            // not being able to spawn the prediction process
            // is severe enough to fail to whole system
            throw new RuntimeException(e);
        }
    }

    @Override
    public void receiveGeneratedData(byte[] data) {
        // handle the incoming data as described in the benchmark description
        logger.trace("receiveGeneratedData("+new String(data)+"): "+new String(data));
    }

    @Override
    public void receiveGeneratedTask(String taskId, byte[] data) {
        // handle the incoming task and create a result
        String input = new String(data);
        logger.trace("receiveGeneratedTask({})->{}",taskId, input);

        String result = null;
        try {
            result = (useMock)
                ? mockPrediction(input, this.queryType)
                : performPrediction(input, this.queryType);

        } catch (IOException e) {
            logger.error("An error occurred during prediction", e);
            return;
        }

        try {
            sendResultToEvalStorage(taskId, result.getBytes());
        } catch (IOException e) {
            logger.error("An error occurred while trying to send result to eval storage", e);
        }
    }

    private String performPrediction(String input, int queryType) throws IOException {

        String predictionUrl = (queryType == 1)
            ? "http://localhost:5000/predict_port"
            : "http://localhost:5000/predict_port_and_time";

        RequestBody body = RequestBody.create(MediaType.parse("TEXT/PLAIN"), input);
        Request request = new Request.Builder()
            .url(predictionUrl)
            .post(body)
            .build();

        Response response = this.client.newCall(request).execute();
        return response.body().string();
    }

    private String mockPrediction(String input, int queryType) throws IOException {

        String key = keyForRow(input);
        String truePort = portLookup.getOrDefault(keyForRow(input), "");
        String trueTime = timeLookup.getOrDefault(keyForRow(input), "");
        if (queryType == 2) {
            return truePort + "," + trueTime;
        } else {
            return truePort;
        }
    }

    @Override
    public void close() throws IOException {
        // Free the resources you requested here
        logger.debug("close()");

        // Always close the super class after yours!
        super.close();
    }

}

