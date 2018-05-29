package org.hobbit.debs_2018_gc_samples.System;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;

public class Predictor {

	private static final String BASE_PATH = "/Users/florianschmidt/dev/SELab/DEBS-GC-2018";

	private static Predictor predictor;

	private final OkHttpClient client;
	private final int numberOfInstances;

	private int currentInstance = 0;

	public static synchronized Predictor getInstance() {
		if (Predictor.predictor == null) {
			Predictor.predictor = new Predictor(10);
		}

		return Predictor.predictor;
	}

	private Predictor(int numberOfInstances) {
		for (int i = 0; i < numberOfInstances; i++) {
			spawnPredictorProcess(i);
		}
		this.numberOfInstances = numberOfInstances;
		this.client = new OkHttpClient();
	}

	public String predict(String input, int queryType) throws IOException {
		String result = performPrediction(input, queryType, currentInstance % numberOfInstances);
		currentInstance++;
		return result;
	}

//	private String mockPrediction(String input, int queryType) throws IOException {
//
//		String key = keyForRow(input);
//
//		if (portLookup == null || timeLookup == null) {
//			setupLookupMaps();
//		}
//
//		String truePort = portLookup.getOrDefault(keyForRow(input), "");
//		String trueTime = timeLookup.getOrDefault(keyForRow(input), "");
//		if (queryType == 2) {
//			return truePort + "," + trueTime;
//		} else {
//			return truePort;
//		}
//	}

	private String performPrediction(String input, int queryType,
		int currentInstance) throws IOException {

		int port = 5000 + currentInstance;

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

//	private void setupLookupMaps() throws IOException {
//		this.portLookup = Files
//			.readAllLines(Paths.get("data", "debs2018_second_dataset_training_labeled_v7.csv"))
//			.stream()
//			.collect(Collectors.toMap(this::keyForRow, this::extractTimeFromFow, (p1, p2) -> p2));
//
//		logger.info("Setup portLookup with " + portLookup.keySet().size() + " keys");
//
//		this.timeLookup = Files
//			.readAllLines(Paths.get("data", "debs2018_second_dataset_training_labeled_v7.csv"))
//			.stream()
//			.collect(Collectors.toMap(this::keyForRow, this::extractPortFromRow, (p1, p2) -> p2));
//
//		logger.info("Setup timeLookup with " + timeLookup.keySet().size() + " keys");
//	}

	private Process spawnPredictorProcess(int portOffset) {
		int portBase = 5000;
		int port = portBase + portOffset;

		try {
			return new ProcessBuilder(BASE_PATH + "solution/flask-main.py" + port)
				.inheritIO()
				.start();
		} catch (IOException e) {
			// not being able to spawn the prediction process
			// is severe enough to fail to whole system
			throw new RuntimeException(e);
		}
	}
}