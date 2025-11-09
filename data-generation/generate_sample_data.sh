#!/bin/bash

echo "Generating sample e-commerce data..."

# Load environment variables
source .env

# Parameters
NUM_EVENTS=${1:-5000}
DELAY=${2:-0.1}
CONTINUOUS=${3:-false}

# Run continuous or batch mode
if [ "$CONTINUOUS" = "true" ] || [ "$CONTINUOUS" = "-c" ] || [ "$CONTINUOUS" = "--continuous" ]; then
    echo "Starting CONTINUOUS event generation (press Ctrl+C to stop)..."
    python3 event_generator.py \
        --bootstrap-servers ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
        --topic ${KAFKA_TOPIC_RAW_EVENTS:-raw-events} \
        --delay $DELAY \
        --continuous
else
    echo "Generating $NUM_EVENTS events with $DELAY second delay between events..."
    python3 event_generator.py \
        --bootstrap-servers ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
        --topic ${KAFKA_TOPIC_RAW_EVENTS:-raw-events} \
        --num-events $NUM_EVENTS \
        --delay $DELAY
    echo "Data generation complete!"
fi

