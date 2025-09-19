from dagster import resource, InitResourceContext
from kafka import KafkaConsumer, KafkaProducer
import json


@resource(
    config_schema={
        "bootstrap_servers": list,
        "consumer_config": dict,
        "producer_config": dict,
    }
)
def kafka_resource(init_context: InitResourceContext):
    """
    Kafka resource for consumers and producers
    """
    config = init_context.resource_config

    bootstrap_servers = config.get("bootstrap_servers", ["localhost:9092"])
    consumer_config = config.get("consumer_config", {})
    producer_config = config.get("producer_config", {})

    # Default configurations
    default_consumer_config = {
        "bootstrap_servers": bootstrap_servers,
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
    }

    default_producer_config = {
        "bootstrap_servers": bootstrap_servers,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "acks": "all",
        "retries": 3,
    }

    # Merge configs
    final_consumer_config = {**default_consumer_config, **consumer_config}
    final_producer_config = {**default_producer_config, **producer_config}

    kafka_clients = {
        "consumer": lambda topic: KafkaConsumer(topic, **final_consumer_config),
        "producer": lambda: KafkaProducer(**final_producer_config),
        "config": {
            "bootstrap_servers": bootstrap_servers,
            "consumer_config": final_consumer_config,
            "producer_config": final_producer_config,
        },
    }

    return kafka_clients
