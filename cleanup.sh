#!/bin/bash

REDIS_CONTAINER="redis_container"
KAFKA_CONTAINER="kafka_container"
DEFAULT_TOPIC="input_topic"

check_container_running() {
    docker inspect -f '{{.State.Running}}' $1 2>/dev/null
}

if docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
    echo "Clearing all Redis data..."
    docker exec -it $REDIS_CONTAINER redis-cli FLUSHALL
    echo "Redis data cleared."
else
    echo "Redis container is not running. Skipping Redis cleanup."
fi

if docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
    echo "Deleting Kafka topic: $DEFAULT_TOPIC..."
    
    docker exec -it $KAFKA_CONTAINER kafka-topics --bootstrap-server kafka:9092 --delete --topic $DEFAULT_TOPIC 2>/dev/null

    sleep 5

    EXISTING_TOPICS=$(docker exec -it $KAFKA_CONTAINER kafka-topics --bootstrap-server kafka:9092 --list 2>/dev/null | tr -d '\r')
    if [[ $EXISTING_TOPICS != *"$DEFAULT_TOPIC"* ]]; then
        echo "Kafka topic $DEFAULT_TOPIC deleted successfully."
    else
        echo "Failed to delete Kafka topic $DEFAULT_TOPIC or it still exists."
    fi
else
    echo "Kafka container is not running. Skipping Kafka topic cleanup."
fi

echo "Cleanup complete."
