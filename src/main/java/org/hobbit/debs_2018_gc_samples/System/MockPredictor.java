package org.hobbit.debs_2018_gc_samples.System;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

public class MockPredictor {

	private static final String BASE_PATH = "/Users/florianschmidt/dev/SELab/DEBS-GC-2018";

	private static MockPredictor predictor;

	private Map<String, String> portLookup;
	private Map<String, String> timeLookup;
	private Logger logger = LoggerFactory.getLogger(MockPredictor.class);

	public static synchronized MockPredictor getInstance() {
		if (MockPredictor.predictor == null) {
			MockPredictor.predictor = new MockPredictor();
		}

		return MockPredictor.predictor;
	}

	private MockPredictor() {
		try {
			setupLookupMaps();
		} catch (IOException e) {
			throw new RuntimeException("Failed to setup MockPredictor");
		}
	}

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

	public String predict(String input, int queryType) throws IOException {
		return performPrediction(input, queryType);
	}

	private String performPrediction(String input, int queryType) throws IOException {

		if (portLookup == null || timeLookup == null) {
			setupLookupMaps();
		}

		String truePort = portLookup.getOrDefault(keyForRow(input), "");
		String trueTime = timeLookup.getOrDefault(keyForRow(input), "");
		if (queryType == 2) {
			return truePort + "," + trueTime;
		} else {
			return truePort;
		}
	}

	private void setupLookupMaps() throws IOException {
		this.portLookup = Files
			.readAllLines(Paths.get("data", "debs2018_second_dataset_training_labeled_v7.csv"))
			.stream()
			.collect(Collectors.toMap(this::keyForRow, this::extractTimeFromFow, (p1, p2) -> p2));

		logger.info("Setup portLookup with " + portLookup.keySet().size() + " keys");

		this.timeLookup = Files
			.readAllLines(Paths.get("data", "debs2018_second_dataset_training_labeled_v7.csv"))
			.stream()
			.collect(Collectors.toMap(this::keyForRow, this::extractPortFromRow, (p1, p2) -> p2));

		logger.info("Setup timeLookup with " + timeLookup.keySet().size() + " keys");
	}
}