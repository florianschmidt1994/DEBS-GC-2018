package org.hobbit.debs_2018_gc_samples.System;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Predictor {

	private static final String BASE_PATH = "/Users/florianschmidt/dev/SELab/DEBS-GC-2018/";

	private static Predictor predictor;

	private final OkHttpClient client;
	private final int numberOfInstances;
	private Set<Process> instances = new HashSet<>();

	private final int portBase = 5000;

	private int currentInstance = 0;
	private Logger logger = LoggerFactory.getLogger(Predictor.class);


	public static synchronized Predictor getInstance() {
		if (Predictor.predictor == null) {
			Predictor.predictor = new Predictor(10);
		}

		return Predictor.predictor;
	}

	private Predictor(int numberOfInstances) {
		this.client = new OkHttpClient();

		for (int i = 0; i < numberOfInstances; i++) {
			instances.add(spawnPredictorProcess(i));
		}
		this.numberOfInstances = numberOfInstances;
	}

	public String predict(String input, int queryType) throws IOException {
		String result = performPrediction(input, queryType, currentInstance % numberOfInstances);
		currentInstance++;
		return result;
	}

	private String performPrediction(String input, int queryType,
		int currentInstance) throws IOException {

		int port = portBase + currentInstance;

		String predictionUrl = (queryType == 1)
			? "http://localhost:" + port + "/predict_port"
			: "http://localhost:" + port + "/predict_port_and_time";

		RequestBody body = RequestBody.create(MediaType.parse("TEXT/PLAIN"), input);
		Request request = new Request.Builder()
			.url(predictionUrl)
			.post(body)
			.build();

		Response response = this.client.newCall(request).execute();
		return response.body().string();
	}

	private Process spawnPredictorProcess(int portOffset) {

		int port = portBase + portOffset;
		Process p;
		try {
			p = new ProcessBuilder(BASE_PATH + "solution/flask-main.py", String.valueOf(port))
				.inheritIO()
				.start();
		} catch (IOException e) {
			// not being able to spawn the prediction process
			// is severe enough to fail to whole system
			logger.error("Error spawning new instance", e);
			throw new RuntimeException(e);
		}

		boolean isUp = false;
		while (!isUp) {
			Request request = new Request.Builder()
				.url("http://localhost:"+port)
				.build();
			try {
				String res = client.newCall(request).execute().body().string();
				if (res.equals("HEALTHY")) {
					isUp = true;
				}
			} catch (IOException e) {
				// waiting for process to come up
				try {
					Thread.sleep(100);
					logger.debug("Waiting for instance to come up");
				} catch (InterruptedException e1) {
					throw new RuntimeException(e);
				}
			}
		}

		return p;
	}

	public void close() {
		for (Process p : instances) {
			p.destroyForcibly();
		}
	}
}