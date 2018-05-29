package org.hobbit.debs_2018_gc_samples.System;

import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.sdk.JenaKeyValue;
import org.hobbit.sdk.examples.dummybenchmark.DummySystemAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hobbit.debs_2018_gc_samples.Constants.*;


public class SystemAdapter extends AbstractSystemAdapter {
    private static final String HOBBIT_SYSTEM_CONTAINER_ID_KEY = "";
    private static JenaKeyValue parameters;
    private Logger logger = LoggerFactory.getLogger(SystemAdapter.class);
    private int queryType = -1;
    private final Map<String, Integer> tuplesPerShip = new HashMap<String, Integer>();

    Timer timer;
    boolean timerStarted=false;
    long lastReportedValue = 0;
    long tuplesReceived=0;
    long errors=0;
    int timerPeriodSeconds = 5;
    int systemContainerId = 0;
    int systemInstancesCount = 1;

    @Override
    public void init() throws Exception {
        super.init();

        logger.debug("Init()");
        timer = new Timer();

        // Your initialization code comes here...
        parameters = new JenaKeyValue.Builder().buildFrom(systemParamModel);

        if(!parameters.containsKey(BENCHMARK_URI+"#slaveNode")) {
            JenaKeyValue slaveParameters = new JenaKeyValue(parameters);
            slaveParameters.put(BENCHMARK_URI+"#slaveNode", "TRUE");
            createContainer(SYSTEM_IMAGE_NAME, new String[]{ Constants.SYSTEM_PARAMETERS_MODEL_KEY+"="+ slaveParameters.encodeToString() });
        }else
            logger = LoggerFactory.getLogger(SystemAdapter.class.getCanonicalName()+"_slave");


        queryType = parameters.getIntValueFor(QUERY_TYPE_KEY);
        if(queryType<=0){
            Exception ex = new Exception("Query type is not specified correctly");
            logger.error(ex.getMessage());
            throw ex;
        }

        logger.debug("Init finished. SystemModel: "+parameters.encodeToString()+" sender: "+(this.sender2EvalStore!=null?"not null": "null") );

        startTimer();
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

        // handle the incoming task and create a result
        String input = new String(data);
        logger.trace("receiveGeneratedTask({})->{}",taskId, input);

        tuplesReceived++;

        String[] splitted = input.split(",");
        String shipId = splitted[0];
        String timestamp = splitted[7];

        int tuplesOfTheShip =  (tuplesPerShip.containsKey(shipId)? tuplesPerShip.get(shipId): 0);

        tuplesOfTheShip++;

        String result = "null";
        try {
            // Send the result to the evaluation storage
            // result = Predictor.getInstance().predict(input, queryType);
            result = MockPredictor.getInstance().predict(input, queryType);

        } catch (Exception e) {
            errors++;
            //logger.error("Processing error: {}", e.getMessage());
        }

        logger.trace("sendResultToEvalStorage({})->{}", taskId, result);
        try {
            sendResultToEvalStorage(taskId, result.getBytes());
        } catch (IOException e) {
            logger.error("sendResultToEvalStorage error: {}", e.getMessage());
        }
        tuplesPerShip.put(shipId, tuplesOfTheShip);

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
