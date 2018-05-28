#!/usr/bin/env bash
IMAGE_NAME="git.project-hobbit.eu:4567/florian.schmidt.1994/debs2018solution/system-adapter"
JAR_FILE="local/debs_2018_gc_sample_system-1.0.jar"
MAIN_CLASS="org.hobbit.debs_2018_gc_samples.System.SampleSystemTestRunner"
SESSION_ID=$RANDOM
SHIP_DATA_FILE="$(pwd)/local/debs2018_second_dataset_training_labeled_v7.csv"
RECORD_LIMIT=5000

echo ""
echo "##############################################"
echo "Building docker image"
echo "${IMAGE_NAME}"
echo "##############################################"
echo ""
docker build -t ${IMAGE_NAME} .

# exit 0

echo ""
echo "##############################################"
echo "Starting local benchmark"
echo ""
echo "IMAGE_NAME=${IMAGE_NAME}"
echo "JAR_FILE=${JAR_FILE}"
echo "SESSION_ID=${SESSION_ID}"
echo "MAIN_CLASS=${JAR_FILE}"
echo "SHIP_DATA_FILE=${SHIP_DATA_FILE}"
echo "##############################################"
echo ""

echo "java -cp ${JAR_FILE} ${MAIN_CLASS} ${IMAGE_NAME} ${SESSION_ID}"
java -cp ${JAR_FILE} ${MAIN_CLASS} ${IMAGE_NAME} ${SESSION_ID}