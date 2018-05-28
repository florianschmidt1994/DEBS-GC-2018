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
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static org.hobbit.debs_2018_gc_samples.Constants.QUERY_TYPE_KEY;


public class SystemAdapter extends AbstractSystemAdapter {
    private static final String HOBBIT_SYSTEM_CONTAINER_ID_KEY = "";
    private static final String PREDICTOR_PORT_URL = "http://localhost:5000/predict_port";
    private static JenaKeyValue parameters;

    private int queryType = -1;
    private final Map<String, Integer> tuplesPerShip = new HashMap<String, Integer>();

    private Logger logger = LoggerFactory.getLogger(SystemAdapter.class);

    Timer timer;
    boolean timerStarted=false;
    long lastReportedValue = 0;
    long tuplesReceived=0;
    long errors=0;
    int timerPeriodSeconds = 5;
    int systemContainerId = 0;
    int systemInstancesCount = 1;

    private OkHttpClient client;
    private Process predictorProcess;

    @Override
    public void init() throws Exception {
        super.init();

        this.predictorProcess = spawnPredictorProcess();
        // this.predictorProcess.waitFor();

        System.out.println("Version 99!");
        logger.debug("Version 100!");

        client = new OkHttpClient();

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

        timer = new Timer();
        logger.info("Finished initializing!");
    }

    private Process spawnPredictorProcess() {
        try {
            return new ProcessBuilder("/Users/florianschmidt/dev/SELab/DEBS-GC-2018/solution/flask-main.py")
            //return new ProcessBuilder("/usr/src/debs2018solution/solution/flask-main.py")
                .inheritIO()
                .start();
        } catch (IOException e) {
            // not being able to spawn the prediction process
            // is severe enough to fail to whole system
            throw new RuntimeException(e);
        }
    }
    private void startTimer(){
        if(timerStarted)
            return;
        timerStarted = true;
        timer.schedule(new TimerTask() {
            @Override
            public void run() {

                long valDiff = (tuplesReceived - lastReportedValue)/timerPeriodSeconds;
                logger.debug("{} tuples received. Curr: {} tuples/s. {}", tuplesReceived, valDiff, (errors>0?errors+" errors":""));
                lastReportedValue = tuplesReceived;

            }
        }, 1000, timerPeriodSeconds*1000);

    }


    @Override
    public void receiveGeneratedData(byte[] data) {
        // handle the incoming data as described in the benchmark description
        logger.trace("receiveGeneratedData("+new String(data)+"): "+new String(data));
    }

    @Override
    public void receiveGeneratedTask(String taskId, byte[] data) {
        startTimer();
        // handle the incoming task and create a result
        String input = new String(data);
        logger.trace("receiveGeneratedTask({})->{}",taskId, input);

        boolean useMock = false;

        String result = null;
        try {
            result = (useMock)
				? mockPrediction(input, queryType)
				: performPrediction(input, queryType);
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
        RequestBody body = RequestBody.create(MediaType.parse("TEXT/PLAIN"), input);
        Request request = new Request.Builder()
            .url(PREDICTOR_PORT_URL)
            .post(body)
            .build();

        Response response = this.client.newCall(request).execute();
        return response.body().string();
    }

    private String mockPrediction(String input, int queryType) {
        String[] splitted = input.split(",");
        String result = splitted[8];
        if (queryType == 2) {
            return result + "," + splitted[7];
        } else {
            return result;
        }
    }

    @Override
    public void close() throws IOException {
        timer.cancel();
        // Free the resources you requested here
        logger.debug("close()");

        // Always close the super class after yours!
        super.close();
    }

}

