package org.hobbit.debs_2018_gc_samples.Benchmark;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractTaskGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.hobbit.debs_2018_gc_samples.Constants.ACKNOWLEDGE_QUEUE_NAME;
import static org.hobbit.debs_2018_gc_samples.Constants.ENCRYPTION_KEY_NAME;


/**
 * @author Pavel Smirnov
 */

public class TaskGenerator extends AbstractTaskGenerator {
	private Logger logger;

	private final Boolean sequental = false;
	private final Map<String, Integer> pointIndexes = new HashMap<>();
	private final Map<String, Integer> tripIndexes = new HashMap<>();

	private final Map<String, Integer> shipTuplesCount = new HashMap<>();

	//private final Map<String, String> tupleTimestamps = new HashMap<>();
	private final Map<String, String> taskShipMap = new HashMap<>();
	private Map<String, Map<String, List<DataPoint>>> shipTrips;
	//private final Map<String, Map<String, KeyValue>> labelsPerShips = new HashMap<>();

	private final Map<String, List<String>> shipTasks = new HashMap<>();
	private final Map<String, String> taskExpectations = new HashMap<>();
	private final Map<String, Long> taskSentTimestamps = new HashMap<>();

	private List<String> shipsToSend = new ArrayList<>();

	int queryType = -1;
	int generationTimeoutMin = 10;
	public List<DataPoint> allPoints;

	long allPointsCount = 0;
	Channel evalStorageTaskGenChannel;
	QueueingConsumer exchangeQueueConsumer;

	Channel dataGenTaskGenChannel;
	QueueingConsumer dataGenTaskGenConsumer;

	int recordsSent = 0;
	int recordsLimit = 0;
	long lastReportedValue = 0;
	//long expectationsToStorageTime =0;
	long valDiff = 0;
	long expectationsSent = 0;
	long errors = 0;
	String[] shipIdsToSend;

	String encryptionKey = "encryptionKey";
	String tupleName = "tuples";
	Timer timer;
	int timerPeriodSeconds = 5;
	ExecutorService threadPool;
	Callable<String> executionLoop;

	Boolean gerenationStarted = false;

	public TaskGenerator() {
		super(1);
	}

	@Override
	public void init() throws Exception {
		// Always init the super class first!
		super.init();
		logger = LoggerFactory.getLogger(TaskGenerator.class.getName() + "_" + getGeneratorId());
		logger.debug("Init()");

		if (System.getenv().containsKey(ENCRYPTION_KEY_NAME))
			encryptionKey = System.getenv().get(ENCRYPTION_KEY_NAME);

		if (System.getenv().containsKey("GENERATOR_LIMIT")) {
			logger.debug("GENERATOR_LIMIT={}", System.getenv().get("GENERATOR_LIMIT"));
			recordsLimit = 1500;
			// recordsLimit = Integer.parseInt(System.getenv().get("GENERATOR_LIMIT"));
		}
		recordsLimit = 100000;

		if (System.getenv().containsKey("GENERATOR_TIMEOUT")) {
			logger.debug("GENERATOR_TIMEOUT={}", System.getenv().get("GENERATOR_TIMEOUT"));
			int newTimeout = Integer.parseInt(System.getenv().get("GENERATOR_TIMEOUT"));
			if (newTimeout > 0)
				generationTimeoutMin = newTimeout;
		}

		if (System.getenv().containsKey("QUERY_TYPE"))
			queryType = Integer.parseInt(System.getenv().get("QUERY_TYPE"));


		if (queryType <= 0) {
			Exception ex = new Exception("Query type is not specified correctly");
			logger.error(ex.getMessage());
			throw ex;
		}

		int dataGeneratorId = getGeneratorId();
		int numberOfGenerators = getNumberOfGenerators();

		logger.debug("Init (genId={}, queryType={}, recordsLimit={}, timeout={}", dataGeneratorId, queryType, String.valueOf(recordsLimit), String.valueOf(generationTimeoutMin) + ")");

		String exchangeQueueName = this.generateSessionQueueName(ACKNOWLEDGE_QUEUE_NAME);

		evalStorageTaskGenChannel = this.cmdQueueFactory.getConnection().createChannel();
		//evalStorageTaskGenChannel.queueDeclare(exchangeQueueName, false, false, true, null);
		evalStorageTaskGenChannel.exchangeDeclare(exchangeQueueName, "fanout", false, true, (Map) null);

		String queueName = evalStorageTaskGenChannel.queueDeclare().getQueue();
		evalStorageTaskGenChannel.queueBind(queueName, exchangeQueueName, "");

		exchangeQueueConsumer = new QueueingConsumer(evalStorageTaskGenChannel);
		evalStorageTaskGenChannel.basicConsume(queueName, true, exchangeQueueConsumer);

		threadPool = Executors.newCachedThreadPool();
		executionLoop = () -> {
			Boolean stop = sendData(null);
			while (!stop) {
				QueueingConsumer.Delivery delivery = exchangeQueueConsumer.nextDelivery();
				byte[] body = delivery.getBody();
				if (body.length > 0) {
					String encryptedTaskId = new String(body);
					logger.trace("Received acknowledgement: {}", encryptedTaskId);
					if (taskShipMap.containsKey(encryptedTaskId)) {
						String shipId = taskShipMap.get(encryptedTaskId);
						taskShipMap.remove(encryptedTaskId);
						try {
							stop = sendData(shipId);
						} catch (Exception e) {
							logger.error("Failed to send data: {}", e.getMessage());
						}
					}

				}
				//stop = sendData(null);
			}
			return "";
		};

		initData();

	}

	@Override
	protected void generateTask(byte[] bytes) throws Exception {

	}


	private void initData() throws Exception {
		//getPointsPerShip(Paths.get("data","vessel24hpublic_fixed.csv"),0);
		//getPointsPerShip(Paths.get("data","debs2018_training_fixed_2.csv"), recordsLimit);

		//String[] lines = Utils.readFile(Paths.get("data","1000rowspublic_fixed.csv"), recordsLimit);
		Utils utils = new Utils(this.logger);
		// String[] lines = utils.readFile(Paths.get("data", "debs2018_training_fixed_5.csv"), recordsLimit);
		 String[] lines = utils.readFile(Paths.get("data", "debs2018_second_dataset_training_labeled_v7.csv"), recordsLimit);

		//String[] lines = Utils.readFile(Paths.get("data","debs2018_training_labeled.csv"), recordsLimit);

		shipTrips = utils.getTripsPerShips(lines);
		int generatorId = getGeneratorId();


		for (String shipId : shipTrips.keySet()) {
			//for(String shipId : new String[]{ shipTrips.keySet().iterator().next() }){
			shipsToSend.add(shipId);
			List<DataPoint> shipPoints = shipTrips.get(shipId).values().stream().flatMap(l -> l.stream()).collect(Collectors.toList());
			//pointsPerShip.put(shipId, shipPoints);
			allPointsCount += shipPoints.size();
			shipTuplesCount.put(shipId, shipPoints.size());
			shipTasks.put(shipId, new ArrayList<>());
			if (sequental)
				allPoints.addAll(shipPoints);
		}
	}


	@Override
	public void receiveCommand(byte command, byte[] data) {
		if (command == Commands.TASK_GENERATOR_START_SIGNAL) {
			logger.debug("TASK_GENERATOR_START_SIGNAL received");

			if (gerenationStarted)
				return;
			gerenationStarted = true;
			generateData();
		}
		super.receiveCommand(command, data);
	}


	public void generateData() {
		logger.debug("generateData()");

		startTimer();

		long started = new Date().getTime();

		logger.debug("Start sending tuples(" + shipsToSend.size() + " ships)");
		Future<String> future = threadPool.submit(executionLoop);
		try {
			future.get(generationTimeoutMin, TimeUnit.MINUTES);
			//future.get(10, TimeUnit.SECONDS);
		} catch (ExecutionException e) {
			logger.error("RuntimeException: {}", e.getMessage());
			e.getCause();
			System.exit(1);
		} catch (InterruptedException e) {
			logger.error("InterruptedException: {}", e.getMessage());
			Thread.currentThread().interrupt();
			e.getCause();
			System.exit(1);
		} catch (TimeoutException e) {
			Exception e2 = new Exception("Timeout exception: " + generationTimeoutMin + " min");
			logger.error(e2.getMessage());
			System.exit(1);
		}

		try {
			sendToCmdQueue(Commands.DATA_GENERATION_FINISHED);
		} catch (IOException e) {
			logger.error("Error sending the Commands.DATA_GENERATION_FINISHED command");
		}

		threadPool.shutdown();
		timer.cancel();

		double took = (new Date().getTime() - started) / 1000.0;
		logger.debug("Finished after {} tuples sent. Took {} s. Avg: {} tuples/s", recordsSent, took, Math.round(recordsSent / took));

		timer.cancel();

	}


	private void startTimer() {
		timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				valDiff = (recordsSent - lastReportedValue) / timerPeriodSeconds;
				logger.debug("{} tuples sent. Unfinished ships:{}. Curr: {} tuples/s", recordsSent, shipsToSend.size(), valDiff);
				lastReportedValue = recordsSent;
			}
		}, 1000, timerPeriodSeconds * 1000);
	}

	private Boolean sendData(String shipId) throws Exception {
		Boolean stop = false;
		if (sequental)
			sendSequental();
		else
			sendParallel(shipId);

		if (shipsToSend.size() == 0)
			stop = true;

		return stop;
	}

	private void sendSequental() throws Exception {
		DataPoint dataPoint = allPoints.get(recordsSent);
		sendPoint(dataPoint, "groupId", recordsSent);
	}

	private void sendParallel(String signleShipId) {

		shipIdsToSend = (signleShipId != null ? new String[]{signleShipId} : shipsToSend.toArray(new String[0]));

		for (String shipId : shipIdsToSend) {

			try {
				int tripIndex = (tripIndexes.containsKey(shipId) ? tripIndexes.get(shipId) : 0);
				int pointIndex = (pointIndexes.containsKey(shipId) ? pointIndexes.get(shipId) : 0);

				String encryptedTaskId = null;
				while (encryptedTaskId == null && tripIndex < shipTrips.get(shipId).size()) {
					List<DataPoint> tripPoints = ((List<DataPoint>) (shipTrips.get(shipId).values().toArray()[tripIndex]));
					DataPoint dataPoint = tripPoints.get(pointIndex);

					try {
						encryptedTaskId = sendPoint(dataPoint, shipId + "_" + tripIndex, pointIndex);
						if (encryptedTaskId == null)
							pointIndex = 99999999; //switch to the next trip of the ship
					} catch (Exception e) {
						logger.error("Problem with sendPoint(): " + e.getMessage());
					}

					pointIndex++;

					//if all point of the trip have been sent off switch to the next trip
					if (pointIndex >= tripPoints.size()) {
						tripIndex++;
						pointIndex = 0;
					}
				}

				//finish without waiting the last notification
				if (tripIndex >= shipTrips.get(shipId).size())
					shipsToSend.remove(shipId);


				tripIndexes.put(shipId, tripIndex);
				pointIndexes.put(shipId, pointIndex);

			} catch (Exception e) {
				logger.error("Problem with sendParallel(): " + e.getMessage());
			}
		}

	}

	private String sendPoint(DataPoint dataPoint, String tripId,
		int orderingIndex) throws Exception {

		String taskId = "gen_" + String.valueOf(getGeneratorId()) + "_task_" + String.valueOf(recordsSent);
		String shipId = dataPoint.getValue("ship_id").toString();
		String raw = dataPoint.getStringValueFor("raw");

		String sendToSystem = raw;
		logger.trace("sendTaskToSystemAdapter({})->{}", taskId, sendToSystem);

		String label = dataPoint.get("arrival_port_calc");

		long taskSentTimestamp = System.currentTimeMillis();
		sendTaskToSystemAdapter(taskId, sendToSystem.getBytes());

		String encryptedTaskId = encryptString(taskId, encryptionKey);
		taskShipMap.put(encryptedTaskId, shipId);

		List<String> thisShipTasks = shipTasks.get(shipId);
		thisShipTasks.add(encryptedTaskId);
		shipTasks.put(shipId, thisShipTasks);
		taskSentTimestamps.put(encryptedTaskId, taskSentTimestamp);

		String tupleTimestamp = dataPoint.get("timestamp");
		String expectation = "";
		if (label != null) {
			if (queryType == 2)
				label += "," + dataPoint.get("arrival_calc");
			expectation = (queryType == 1 ? tripId + "," + orderingIndex + "," + tupleTimestamp + "," : "") + label;
			sendExpectation(encryptedTaskId, taskSentTimestamp, expectation);
			//taskExpectations.put(encryptedTaskId, expectation);
		}

		recordsSent++;
		return encryptedTaskId;
	}

	public static String encryptString(String string, String encryptionKey) {
		return string;
	}


	private void sendExpectation(String encTaskId, long taskSentTimestamp,
		String sendToStorage) throws IOException {
		logger.trace("sendTaskToEvalStorage({})=>{}", encTaskId, sendToStorage.getBytes());
		sendTaskToEvalStorage(encTaskId, taskSentTimestamp, sendToStorage.getBytes());
	}

}