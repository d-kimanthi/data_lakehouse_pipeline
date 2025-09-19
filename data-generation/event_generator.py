# data-generation/event_generator.py

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any
import numpy as np
from faker import Faker
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EcommerceEventGenerator:
    """
    Generates realistic e-commerce events for streaming analytics
    """

    def __init__(self):
        self.fake = Faker()
        self.products = self._generate_product_catalog()
        self.users = self._generate_user_base()
        self.categories = [
            "Electronics",
            "Clothing",
            "Home & Garden",
            "Sports",
            "Books",
        ]

    def _generate_product_catalog(self) -> List[Dict[str, Any]]:
        """Generate a realistic product catalog"""
        products = []
        categories = {
            "Electronics": ["Laptop", "Phone", "Tablet", "Headphones", "Camera"],
            "Clothing": ["Shirt", "Pants", "Dress", "Shoes", "Jacket"],
            "Home & Garden": ["Sofa", "Table", "Plant", "Lamp", "Rug"],
            "Sports": ["Running Shoes", "Yoga Mat", "Weights", "Ball", "Racket"],
            "Books": ["Fiction", "Non-fiction", "Biography", "Technical", "Children"],
        }

        for category, items in categories.items():
            for item in items:
                for i in range(random.randint(5, 15)):  # 5-15 products per item type
                    product = {
                        "product_id": str(uuid.uuid4()),
                        "name": f"{item} {self.fake.word().title()}",
                        "category": category,
                        "brand": self.fake.company(),
                        "price": round(random.uniform(10, 2000), 2),
                        "rating": round(random.uniform(1, 5), 1),
                        "inventory_count": random.randint(0, 1000),
                    }
                    products.append(product)

        return products

    def _generate_user_base(self) -> List[Dict[str, Any]]:
        """Generate a user base with different behavior patterns"""
        users = []
        user_types = ["casual", "frequent", "premium", "bargain_hunter"]

        for i in range(1000):  # 1000 users
            user_type = random.choice(user_types)
            user = {
                "user_id": str(uuid.uuid4()),
                "email": self.fake.email(),
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "registration_date": self.fake.date_between(
                    start_date="-2y", end_date="today"
                ),
                "user_type": user_type,
                "location": {
                    "city": self.fake.city(),
                    "state": self.fake.state(),
                    "country": self.fake.country(),
                    "zip_code": self.fake.zipcode(),
                },
                "preferences": random.sample(self.categories, random.randint(1, 3)),
            }
            users.append(user)

        return users

    def generate_page_view_event(self) -> Dict[str, Any]:
        """Generate a realistic page view event"""
        user = random.choice(self.users)
        product = random.choice(self.products)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "page_view",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": user["user_id"],
            "session_id": str(uuid.uuid4())[:8],
            "product_id": product["product_id"],
            "page_type": random.choice(
                ["product_detail", "category", "search_results", "home"]
            ),
            "referrer": random.choice(
                ["google.com", "facebook.com", "direct", "email", "advertisement"]
            ),
            "user_agent": self.fake.user_agent(),
            "ip_address": self.fake.ipv4(),
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "metadata": {
                "page_load_time": round(random.uniform(0.5, 5.0), 2),
                "scroll_depth": random.randint(10, 100),
                "time_on_page": random.randint(5, 300),
            },
        }
        return event

    def generate_add_to_cart_event(self) -> Dict[str, Any]:
        """Generate an add to cart event"""
        user = random.choice(self.users)
        product = random.choice(self.products)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.1, 0.05, 0.05])[
            0
        ]

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "add_to_cart",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": user["user_id"],
            "session_id": str(uuid.uuid4())[:8],
            "product_id": product["product_id"],
            "quantity": quantity,
            "price": product["price"],
            "total_value": round(product["price"] * quantity, 2),
            "cart_id": str(uuid.uuid4())[:8],
            "metadata": {
                "source": random.choice(
                    ["product_page", "search_results", "recommendations"]
                ),
                "cart_size_before": random.randint(0, 10),
            },
        }
        return event

    def generate_purchase_event(self) -> Dict[str, Any]:
        """Generate a purchase event"""
        user = random.choice(self.users)

        # Generate multiple items for the purchase
        num_items = random.choices([1, 2, 3, 4], weights=[0.5, 0.3, 0.15, 0.05])[0]
        items = []
        total_amount = 0

        for _ in range(num_items):
            product = random.choice(self.products)
            quantity = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
            item_total = product["price"] * quantity

            item = {
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "total_price": round(item_total, 2),
                "category": product["category"],
                "brand": product["brand"],
            }
            items.append(item)
            total_amount += item_total

        # Apply discount sometimes
        discount_percent = 0
        if random.random() < 0.2:  # 20% chance of discount
            discount_percent = random.choice([5, 10, 15, 20, 25])

        discount_amount = total_amount * (discount_percent / 100)
        final_amount = total_amount - discount_amount

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "purchase",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": user["user_id"],
            "session_id": str(uuid.uuid4())[:8],
            "order_id": str(uuid.uuid4()),
            "items": items,
            "subtotal": round(total_amount, 2),
            "discount_percent": discount_percent,
            "discount_amount": round(discount_amount, 2),
            "total_amount": round(final_amount, 2),
            "payment_method": random.choice(
                ["credit_card", "debit_card", "paypal", "apple_pay"]
            ),
            "shipping_method": random.choice(["standard", "expedited", "overnight"]),
            "shipping_address": {
                "street": self.fake.street_address(),
                "city": self.fake.city(),
                "state": self.fake.state(),
                "zip_code": self.fake.zipcode(),
                "country": "US",
            },
            "metadata": {
                "is_mobile_purchase": random.choice([True, False]),
                "payment_processor": random.choice(["stripe", "paypal", "square"]),
                "currency": "USD",
            },
        }
        return event

    def generate_user_session_event(self) -> Dict[str, Any]:
        """Generate user session events (login, logout)"""
        user = random.choice(self.users)
        session_type = random.choice(["login", "logout", "session_timeout"])

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "user_session",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": user["user_id"],
            "session_id": str(uuid.uuid4())[:8],
            "session_type": session_type,
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "ip_address": self.fake.ipv4(),
            "user_agent": self.fake.user_agent(),
            "metadata": {
                "login_method": (
                    random.choice(["email", "google", "facebook", "guest"])
                    if session_type == "login"
                    else None
                ),
                "session_duration": (
                    random.randint(60, 3600)
                    if session_type in ["logout", "session_timeout"]
                    else None
                ),
            },
        }
        return event

    def generate_product_update_event(self) -> Dict[str, Any]:
        """Generate product update events"""
        product = random.choice(self.products)
        update_type = random.choice(
            ["price_change", "inventory_update", "rating_update"]
        )

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "product_update",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "product_id": product["product_id"],
            "update_type": update_type,
            "metadata": {},
        }

        if update_type == "price_change":
            old_price = product["price"]
            # 70% chance of small change, 30% chance of significant change
            if random.random() < 0.7:
                change_percent = random.uniform(-10, 10) / 100
            else:
                change_percent = random.uniform(-30, 30) / 100

            new_price = max(5.0, old_price * (1 + change_percent))
            event["metadata"] = {
                "old_price": old_price,
                "new_price": round(new_price, 2),
                "change_percent": round(change_percent * 100, 2),
            }

        elif update_type == "inventory_update":
            old_inventory = product["inventory_count"]
            # Simulate inventory changes
            change = random.randint(-50, 100)
            new_inventory = max(0, old_inventory + change)

            event["metadata"] = {
                "old_inventory": old_inventory,
                "new_inventory": new_inventory,
                "change": change,
            }

        elif update_type == "rating_update":
            old_rating = product["rating"]
            new_rating = max(1.0, min(5.0, old_rating + random.uniform(-0.5, 0.5)))

            event["metadata"] = {
                "old_rating": old_rating,
                "new_rating": round(new_rating, 1),
            }

        return event

    def generate_event(self) -> Dict[str, Any]:
        """Generate a random event based on realistic probabilities"""
        event_weights = {
            "page_view": 0.6,
            "add_to_cart": 0.2,
            "purchase": 0.05,
            "user_session": 0.1,
            "product_update": 0.05,
        }

        event_type = random.choices(
            list(event_weights.keys()), weights=list(event_weights.values())
        )[0]

        if event_type == "page_view":
            return self.generate_page_view_event()
        elif event_type == "add_to_cart":
            return self.generate_add_to_cart_event()
        elif event_type == "purchase":
            return self.generate_purchase_event()
        elif event_type == "user_session":
            return self.generate_user_session_event()
        elif event_type == "product_update":
            return self.generate_product_update_event()


class KafkaEventProducer:
    """
    Kafka producer for streaming e-commerce events
    """

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            key_serializer=lambda x: x.encode("utf-8") if x else None,
            acks="all",
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432,
        )
        self.event_generator = EcommerceEventGenerator()

    def send_event(self, event: Dict[str, Any]):
        """Send a single event to Kafka"""
        try:
            # Use user_id as key for proper partitioning
            key = event.get("user_id") or event.get("product_id", "unknown")

            future = self.producer.send(topic=self.topic, key=key, value=event)

            # Optional: Wait for confirmation (comment out for better performance)
            # record_metadata = future.get(timeout=10)
            # logger.info(f"Event sent to partition {record_metadata.partition}")

        except Exception as e:
            logger.error(f"Error sending event: {e}")

    def generate_and_send_events(self, num_events: int = 100, delay: float = 0.1):
        """Generate and send multiple events"""
        logger.info(f"Starting to generate {num_events} events...")

        for i in range(num_events):
            try:
                event = self.event_generator.generate_event()
                self.send_event(event)

                if i % 100 == 0:
                    logger.info(f"Sent {i} events...")

                if delay > 0:
                    import time

                    time.sleep(delay)

            except KeyboardInterrupt:
                logger.info("Stopping event generation...")
                break
            except Exception as e:
                logger.error(f"Error generating event {i}: {e}")
                continue

        self.producer.flush()
        logger.info(f"Finished sending {num_events} events")

    def close(self):
        """Close the producer"""
        self.producer.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate e-commerce events")
    parser.add_argument(
        "--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topic", default="raw-events", help="Kafka topic to send events to"
    )
    parser.add_argument(
        "--num-events", type=int, default=1000, help="Number of events to generate"
    )
    parser.add_argument(
        "--delay", type=float, default=0.1, help="Delay between events in seconds"
    )

    args = parser.parse_args()

    producer = KafkaEventProducer(args.bootstrap_servers, args.topic)

    try:
        producer.generate_and_send_events(args.num_events, args.delay)
    finally:
        producer.close()
