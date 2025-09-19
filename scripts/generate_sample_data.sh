#!/bin/bash

echo "Generating sample e-commerce data..."

# Load environment variables
source .env

# Parameters
NUM_EVENTS=${1:-5000}
DELAY=${2:-0.1}

echo "Generating $NUM_EVENTS events with $DELAY second delay between events..."

python data-generation/event_generator.py \
    --bootstrap-servers ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
    --topic ${KAFKA_TOPIC_RAW_EVENTS:-raw-events} \
    --num-events $NUM_EVENTS \
    --delay $DELAY

echo "Data generation complete!"

