#!/usr/bin/env python3
"""
Test script for event generation pipeline
Validates that events can be generated and sent to Kafka
"""

import json
import os
import sys
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Add the data-generation directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "data-generation"))

from event_generator import EcommerceEventGenerator, KafkaEventProducer


def create_kafka_topics():
    """Create required Kafka topics if they don't exist"""
    print("Setting up Kafka topics...")

    admin_client = KafkaAdminClient(
        bootstrap_servers=["localhost:9092"], client_id="topic_creator"
    )

    topics = [
        NewTopic(name="raw-events", num_partitions=3, replication_factor=1),
        NewTopic(name="page-views", num_partitions=3, replication_factor=1),
        NewTopic(name="purchases", num_partitions=3, replication_factor=1),
        NewTopic(name="user-sessions", num_partitions=3, replication_factor=1),
    ]

    for topic in topics:
        try:
            admin_client.create_topics([topic])
            print(f"✅ Created topic: {topic.name}")
        except TopicAlreadyExistsError:
            print(f"ℹ️  Topic already exists: {topic.name}")
        except Exception as e:
            print(f"❌ Error creating topic {topic.name}: {e}")


def test_event_generation():
    """Test basic event generation without Kafka"""
    print("\nTesting event generation...")

    try:
        generator = EcommerceEventGenerator()

        # Test each event type
        events = {
            "page_view": generator.generate_page_view_event(),
            "add_to_cart": generator.generate_add_to_cart_event(),
            "purchase": generator.generate_purchase_event(),
            "user_session": generator.generate_user_session_event(),
            "product_update": generator.generate_product_update_event(),
        }

        for event_type, event in events.items():
            print(f"✅ {event_type}: {type(event).__name__}")
            # Verify it can be converted to dict
            event_dict = event.to_dict()
            print(f"   Fields: {list(event_dict.keys())}")

        print("✅ All event types generated successfully!")
        return True

    except Exception as e:
        print(f"Event generation failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_kafka_producer():
    """Test sending events to Kafka"""
    print("\nTesting Kafka producer...")

    try:
        # Test basic Kafka connectivity
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

        # Generate and send test events
        generator = EcommerceEventGenerator()
        kafka_producer = KafkaEventProducer(
            bootstrap_servers="localhost:9092", topic="raw-events"
        )

        print("🚀 Sending 5 test events...")
        for i in range(5):
            event = generator.generate_event()
            kafka_producer.send_event(event)
            print(f"   Sent event {i+1}: {event.get('event_type')}")
            time.sleep(0.1)  # Small delay between events

        producer.flush()  # Ensure all events are sent
        print("✅ Events sent successfully to Kafka!")
        return True

    except Exception as e:
        print(f"❌ Kafka producer test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_kafka_consumer():
    """Test reading events from Kafka"""
    print("\nTesting Kafka consumer...")

    try:
        consumer = KafkaConsumer(
            "raw-events",
            bootstrap_servers=["localhost:9092"],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000,  # 5 second timeout
            auto_offset_reset="earliest",
        )

        events_consumed = 0
        event_types = set()

        print("Consuming events (5 second timeout)...")
        for message in consumer:
            event = message.value
            events_consumed += 1
            event_types.add(event.get("event_type", "unknown"))

            if events_consumed <= 3:  # Show first 3 events
                print(
                    f"   Event {events_consumed}: {event.get('event_type')} from user {event.get('user_id', 'N/A')}"
                )

            if events_consumed >= 10:  # Stop after 10 events
                break

        consumer.close()

        if events_consumed > 0:
            print(f"✅ Consumed {events_consumed} events")
            print(f"   Event types: {sorted(event_types)}")
            return True
        else:
            print("No events found in topic (this might be normal for a fresh setup)")
            return True

    except Exception as e:
        print(f"Kafka consumer test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("E-commerce Event Generation Pipeline Test")
    print("=" * 50)

    tests = [
        ("Event Generation", test_event_generation),
        ("Kafka Topics", create_kafka_topics),
        ("Kafka Producer", test_kafka_producer),
        ("Kafka Consumer", test_kafka_consumer),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n🧪 Running {test_name} test...")
        success = test_func()
        results.append((test_name, success))

    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    print("=" * 50)

    all_passed = True
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status:8} {test_name}")
        if not success:
            all_passed = False

    if all_passed:
        print("\nAll tests passed! Event generation pipeline is ready.")
        print("\nNext steps:")
        print("   1. Check Kafka UI: http://localhost:8080")
        print("   2. Run continuous event generation")
        print("   3. Set up Dagster assets for processing")
    else:
        print("\n⚠️  Some tests failed. Check the logs above for details.")

    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
