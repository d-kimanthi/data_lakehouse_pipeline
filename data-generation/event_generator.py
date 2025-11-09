# data-generation/event_generator.py

import json
import logging
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List

import numpy as np
from faker import Faker
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ===============================
# Event Dataclasses
# ===============================


@dataclass
class PageViewEvent:
    """Page view event with all required fields"""

    event_id: str
    event_type: str
    timestamp: str
    user_id: str
    session_id: str
    product_id: str
    page_type: str
    referrer: str
    user_agent: str
    ip_address: str
    device_type: str
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass to dictionary for JSON serialization"""
        return asdict(self)


@dataclass
class AddToCartEvent:
    """Add to cart event with all required fields"""

    event_id: str
    event_type: str
    timestamp: str
    user_id: str
    session_id: str
    product_id: str
    quantity: int
    price: float
    total_value: float
    cart_id: str
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass to dictionary for JSON serialization"""
        return asdict(self)


@dataclass
class PurchaseItem:
    """Individual item in a purchase"""

    product_id: str
    quantity: int
    unit_price: float
    total_price: float
    category: str
    brand: str


@dataclass
class ShippingAddress:
    """Shipping address for purchases"""

    street: str
    city: str
    state: str
    zip_code: str
    country: str


@dataclass
class PurchaseEvent:
    """Purchase event with all required fields"""

    event_id: str
    event_type: str
    timestamp: str
    user_id: str
    session_id: str
    order_id: str
    items: List[PurchaseItem]
    subtotal: float
    discount_percent: int
    discount_amount: float
    total_amount: float
    payment_method: str
    shipping_method: str
    shipping_address: ShippingAddress
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass to dictionary for JSON serialization"""
        return asdict(self)


@dataclass
class UserSessionEvent:
    """User session event (login/logout/timeout)"""

    event_id: str
    event_type: str
    timestamp: str
    user_id: str
    session_id: str
    session_type: str
    device_type: str
    ip_address: str
    user_agent: str
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass to dictionary for JSON serialization"""
        return asdict(self)


@dataclass
class ProductUpdateEvent:
    """Product update event (price/inventory/rating changes)"""

    event_id: str
    event_type: str
    timestamp: str
    product_id: str
    update_type: str
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass to dictionary for JSON serialization"""
        return asdict(self)


class EcommerceEventGenerator:
    """
    Generates realistic e-commerce events for streaming analytics
    """

    def __init__(self):
        self.fake = Faker()
        # Define categories first, before generating products and users
        self.categories = [
            "Electronics",
            "Clothing",
            "Home & Garden",
            "Sports",
            "Books",
        ]
        self.products = self._generate_product_catalog()
        self.users = self._generate_user_base()

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

    def generate_page_view_event(self) -> PageViewEvent:
        """Generate a realistic page view event"""
        user = random.choice(self.users)
        product = random.choice(self.products)

        event = PageViewEvent(
            event_id=str(uuid.uuid4()),
            event_type="page_view",
            timestamp=datetime.utcnow().isoformat() + "Z",
            user_id=user["user_id"],
            session_id=str(uuid.uuid4())[:8],
            product_id=product["product_id"],
            page_type=random.choice(
                ["product_detail", "category", "search_results", "home"]
            ),
            referrer=random.choice(
                ["google.com", "facebook.com", "direct", "email", "advertisement"]
            ),
            user_agent=self.fake.user_agent(),
            ip_address=self.fake.ipv4(),
            device_type=random.choice(["mobile", "desktop", "tablet"]),
            metadata={
                "page_load_time": round(random.uniform(0.5, 5.0), 2),
                "scroll_depth": random.randint(10, 100),
                "time_on_page": random.randint(5, 300),
            },
        )
        return event

    def generate_add_to_cart_event(self) -> AddToCartEvent:
        """Generate an add to cart event"""
        user = random.choice(self.users)
        product = random.choice(self.products)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.1, 0.05, 0.05])[
            0
        ]

        event = AddToCartEvent(
            event_id=str(uuid.uuid4()),
            event_type="add_to_cart",
            timestamp=datetime.utcnow().isoformat() + "Z",
            user_id=user["user_id"],
            session_id=str(uuid.uuid4())[:8],
            product_id=product["product_id"],
            quantity=quantity,
            price=product["price"],
            total_value=round(product["price"] * quantity, 2),
            cart_id=str(uuid.uuid4())[:8],
            metadata={
                "source": random.choice(
                    ["product_page", "search_results", "recommendations"]
                ),
                "cart_size_before": random.randint(0, 10),
            },
        )
        return event

    def generate_purchase_event(self) -> PurchaseEvent:
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

            item = PurchaseItem(
                product_id=product["product_id"],
                quantity=quantity,
                unit_price=product["price"],
                total_price=round(item_total, 2),
                category=product["category"],
                brand=product["brand"],
            )
            items.append(item)
            total_amount += item_total

        # Apply discount sometimes
        discount_percent = 0
        if random.random() < 0.2:  # 20% chance of discount
            discount_percent = random.choice([5, 10, 15, 20, 25])

        discount_amount = total_amount * (discount_percent / 100)
        final_amount = total_amount - discount_amount

        shipping_address = ShippingAddress(
            street=self.fake.street_address(),
            city=self.fake.city(),
            state=self.fake.state(),
            zip_code=self.fake.zipcode(),
            country="US",
        )

        event = PurchaseEvent(
            event_id=str(uuid.uuid4()),
            event_type="purchase",
            timestamp=datetime.utcnow().isoformat() + "Z",
            user_id=user["user_id"],
            session_id=str(uuid.uuid4())[:8],
            order_id=str(uuid.uuid4()),
            items=items,
            subtotal=round(total_amount, 2),
            discount_percent=discount_percent,
            discount_amount=round(discount_amount, 2),
            total_amount=round(final_amount, 2),
            payment_method=random.choice(
                ["credit_card", "debit_card", "paypal", "apple_pay"]
            ),
            shipping_method=random.choice(["standard", "expedited", "overnight"]),
            shipping_address=shipping_address,
            metadata={
                "is_mobile_purchase": random.choice([True, False]),
                "payment_processor": random.choice(["stripe", "paypal", "square"]),
                "currency": "USD",
            },
        )
        return event

    def generate_user_session_event(self) -> UserSessionEvent:
        """Generate a user session event (login/logout)"""
        user = random.choice(self.users)
        action = random.choice(["login", "logout"])

        event = UserSessionEvent(
            event_id=str(uuid.uuid4()),
            event_type="user_session",
            timestamp=datetime.utcnow().isoformat() + "Z",
            user_id=user["user_id"],
            session_id=str(uuid.uuid4())[:8],
            session_type=action,
            device_type=random.choice(["desktop", "mobile", "tablet"]),
            ip_address=self.fake.ipv4(),
            user_agent=random.choice(
                [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15",
                    "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0 Firefox/88.0",
                ]
            ),
            metadata={
                "login_method": (
                    random.choice(["email", "social", "sso"])
                    if action == "login"
                    else None
                ),
                "session_duration": (
                    random.randint(300, 7200) if action == "logout" else None
                ),
                "referrer": random.choice(
                    [None, "google.com", "facebook.com", "direct"]
                ),
                "location": f"{self.fake.city()}, {self.fake.country()}",
            },
        )
        return event

    def generate_product_update_event(self) -> ProductUpdateEvent:
        """Generate product update events"""
        product = random.choice(self.products)
        update_type = random.choice(
            ["price_change", "inventory_update", "rating_update"]
        )

        metadata = {}

        if update_type == "price_change":
            old_price = product["price"]
            # 70% chance of small change, 30% chance of significant change
            if random.random() < 0.7:
                change_percent = random.uniform(-10, 10) / 100
            else:
                change_percent = random.uniform(-30, 30) / 100

            new_price = max(5.0, old_price * (1 + change_percent))
            metadata = {
                "old_price": old_price,
                "new_price": round(new_price, 2),
                "change_percent": round(change_percent * 100, 2),
            }

        elif update_type == "inventory_update":
            old_inventory = product["inventory_count"]
            # Simulate inventory changes
            change = random.randint(-50, 100)
            new_inventory = max(0, old_inventory + change)

            metadata = {
                "old_inventory": old_inventory,
                "new_inventory": new_inventory,
                "change": change,
            }

        elif update_type == "rating_update":
            old_rating = product["rating"]
            new_rating = max(1.0, min(5.0, old_rating + random.uniform(-0.5, 0.5)))

            metadata = {
                "old_rating": old_rating,
                "new_rating": round(new_rating, 1),
            }

        event = ProductUpdateEvent(
            event_id=str(uuid.uuid4()),
            event_type="product_update",
            timestamp=datetime.utcnow().isoformat() + "Z",
            product_id=product["product_id"],
            update_type=update_type,
            metadata=metadata,
        )
        return event

    def generate_event(self) -> Dict[str, Any]:  # type: ignore
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
            return self.generate_page_view_event().to_dict()
        elif event_type == "add_to_cart":
            return self.generate_add_to_cart_event().to_dict()
        elif event_type == "purchase":
            return self.generate_purchase_event().to_dict()
        elif event_type == "user_session":
            return self.generate_user_session_event().to_dict()
        elif event_type == "product_update":
            return self.generate_product_update_event().to_dict()


class KafkaEventProducer:
    """
    Kafka producer for streaming e-commerce events
    Routes events to topic-specific queues based on event type
    """

    def __init__(self, bootstrap_servers: str, base_topic: str = "raw-events"):
        self.base_topic = base_topic
        self.bootstrap_servers = bootstrap_servers

        # Define topic mapping for different event types
        self.topic_mapping = {
            "page_view": "page-views",
            "add_to_cart": "order-events",
            "purchase": "purchases",
            "user_session": "user-sessions",
            "product_update": "product-updates",
        }

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

    def get_topic_for_event(self, event: Dict[str, Any]) -> str:
        """Determine the appropriate topic for an event based on its type"""
        event_type = event.get("event_type", "unknown")

        # Route to specific topic based on event type
        specific_topic = self.topic_mapping.get(event_type)
        if specific_topic:
            return specific_topic

        # Fallback to base topic for unknown event types
        logger.warning(
            f"Unknown event type '{event_type}', routing to base topic '{self.base_topic}'"
        )
        return self.base_topic

    def send_event(self, event: Dict[str, Any], target_topic: str):
        """Send a single event to the appropriate Kafka topic"""
        try:

            # Use user_id as key for proper partitioning
            key = event.get("user_id") or event.get("product_id", "unknown")

            # Send to the specific topic
            self.producer.send(topic=target_topic, key=key, value=event)

            # Log the routing decision
            logger.debug(
                f"Sent {event.get('event_type')} event to topic: {target_topic} with key: {key}"
            )

        except Exception as e:
            logger.error(f"Error sending event: {e}")

    def generate_and_send_events(
        self, num_events: int = 100, delay: float = 0.1, continuous: bool = False
    ):
        """Generate and send multiple events"""
        if continuous:
            logger.info(
                "Starting continuous event generation (press Ctrl+C to stop)..."
            )
            self._generate_events_continuously(delay)
        else:
            logger.info(f"Starting to generate {num_events} events...")
            self._generate_events_batch(num_events, delay)

    def _generate_events_batch(self, num_events: int, delay: float):
        """Generate a fixed number of events"""
        topic_counts = {}

        for i in range(num_events):
            try:
                event = self.event_generator.generate_event()
                target_topic = self.get_topic_for_event(event)

                # Track events per topic
                topic_counts[target_topic] = topic_counts.get(target_topic, 0) + 1

                self.send_event(event, target_topic)

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
        logger.info(f"Topic distribution: {dict(sorted(topic_counts.items()))}")

    def _generate_events_continuously(self, delay: float):
        """Generate events continuously until interrupted"""
        import time

        event_count = 0
        start_time = time.time()
        last_report_time = start_time
        topic_counts = {}

        try:
            while True:
                try:
                    event = self.event_generator.generate_event()
                    target_topic = self.get_topic_for_event(event)

                    # Track events per topic
                    topic_counts[target_topic] = topic_counts.get(target_topic, 0) + 1

                    self.send_event(event, target_topic)
                    event_count += 1

                    current_time = time.time()

                    # Report progress every 60 seconds
                    if current_time - last_report_time >= 60:
                        elapsed_minutes = (current_time - start_time) / 60
                        events_per_minute = (
                            event_count / elapsed_minutes if elapsed_minutes > 0 else 0
                        )
                        logger.info(
                            f"Generated {event_count} events total ({events_per_minute:.1f} events/min)"
                        )
                        logger.info(
                            f"Topic distribution: {dict(sorted(topic_counts.items()))}"
                        )
                        last_report_time = current_time

                    if delay > 0:
                        time.sleep(delay)

                except Exception as e:
                    logger.error(f"Error generating event: {e}")
                    continue

        except KeyboardInterrupt:
            total_elapsed = (time.time() - start_time) / 60
            logger.info(
                f"Stopping continuous generation. Generated {event_count} events in {total_elapsed:.1f} minutes"
            )
            logger.info(
                f"Final topic distribution: {dict(sorted(topic_counts.items()))}"
            )
        finally:
            self.producer.flush()

    def close(self):
        """Close the producer"""
        self.producer.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate e-commerce events with topic-specific routing"
    )
    parser.add_argument(
        "--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topic",
        default="raw-events",
        help="Base Kafka topic (events will be routed to specific topics based on type)",
    )
    parser.add_argument(
        "--num-events",
        type=int,
        default=1000,
        help="Number of events to generate (ignored in continuous mode)",
    )
    parser.add_argument(
        "--delay", type=float, default=0.1, help="Delay between events in seconds"
    )
    parser.add_argument(
        "--continuous",
        "-c",
        action="store_true",
        help="Run continuously until interrupted (ignores --num-events). Shows topic distribution every 60 seconds.",
    )

    args = parser.parse_args()

    producer = KafkaEventProducer(args.bootstrap_servers, args.topic)

    try:
        producer.generate_and_send_events(args.num_events, args.delay, args.continuous)
    finally:
        producer.close()
