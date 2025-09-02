#!/usr/bin/env python3
import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List

from confluent_kafka import Producer
from faker import Faker

# Initialize Faker
fake = Faker()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "orders"

# US Store locations (state, city pairs)
STORE_LOCATIONS = [
    ("CA", "Los Angeles"),
    ("CA", "San Francisco"),
    ("NY", "New York City"),
    ("TX", "Houston"),
    ("IL", "Chicago"),
    ("FL", "Miami"),
    ("WA", "Seattle"),
    ("MA", "Boston"),
    ("GA", "Atlanta"),
    ("CO", "Denver"),
]

# Product categories and price ranges
PRODUCT_CATEGORIES = {
    "Electronics": (100, 2000),
    "Clothing": (20, 200),
    "Home & Garden": (30, 500),
    "Books": (10, 50),
    "Sports": (25, 300),
    "Toys": (15, 100),
}


def generate_store_id(state: str, city: str) -> str:
    """Generate a store ID based on location."""
    return f"{state}-{city.replace(' ', '')}-{random.randint(1, 3)}"


def generate_product() -> Dict:
    """Generate a random product with category and price."""
    category = random.choice(list(PRODUCT_CATEGORIES.keys()))
    min_price, max_price = PRODUCT_CATEGORIES[category]
    return {
        "product_id": fake.uuid4(),
        "category": category,
        "name": fake.product_name(),
        "unit_price": round(random.uniform(min_price, max_price), 2),
    }


def generate_order_items(num_items: int = None) -> List[Dict]:
    """Generate a list of order items."""
    if num_items is None:
        num_items = random.randint(1, 5)

    items = []
    for _ in range(num_items):
        product = generate_product()
        items.append(
            {
                "product_id": product["product_id"],
                "product_name": product["name"],
                "category": product["category"],
                "quantity": random.randint(1, 3),
                "unit_price": product["unit_price"],
            }
        )
    return items


def generate_order() -> Dict:
    """Generate a single order with location and customer information."""
    state, city = random.choice(STORE_LOCATIONS)
    store_id = generate_store_id(state, city)
    items = generate_order_items()
    total_amount = sum(item["unit_price"] * item["quantity"] for item in items)

    return {
        "order_id": fake.uuid4(),
        "customer_id": f"c-{fake.random_number(digits=6)}",
        "store_id": store_id,
        "store_state": state,
        "store_city": city,
        "items": items,
        "amount": round(total_amount, 2),
        "currency": "USD",
        "payment_method": random.choice(
            ["credit_card", "debit_card", "cash", "mobile_payment"]
        ),
        "event_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def delivery_report(err, msg):
    """Kafka delivery report callback."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def main():
    # Configure Kafka producer
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "retail-data-generator",
    }

    producer = Producer(producer_config)

    try:
        while True:
            order = generate_order()
            producer.produce(
                KAFKA_TOPIC,
                key=order["order_id"],
                value=json.dumps(order),
                callback=delivery_report,
            )
            producer.poll(0)  # Trigger delivery reports

            # Random delay between 1 and 5 seconds
            delay = random.uniform(1, 5)
            time.sleep(delay)

    except KeyboardInterrupt:
        print("Stopping data generation...")
    finally:
        # Flush any remaining messages
        producer.flush()


if __name__ == "__main__":
    main()
