"""
Kafka Data Producer for StreamFlow Demo
========================================
Generates sample user_events and transaction_events for the StreamFlow pipeline demo.

This script can run:
- Locally: python kafka_data_producer.py --bootstrap-servers localhost:9094
- In container: python kafka_data_producer.py --bootstrap-servers kafka:9092

Usage:
    python kafka_data_producer.py [--bootstrap-servers HOST:PORT] [--duration SECONDS] [--rate MSGS_PER_SEC]
"""

import argparse
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError


# Sample data for generating realistic events
USERS = ["user_001", "user_002", "user_003", "user_004", "user_005"]
ACTIONS = ["login", "view_product", "add_to_cart", "checkout", "logout"]
PRODUCTS = ["laptop", "phone", "headphones", "tablet", "watch"]
CATEGORIES = ["electronics", "accessories", "wearables"]


def create_producer(bootstrap_servers):
    """Create and return a Kafka producer."""
    print(f"Connecting to Kafka at {bootstrap_servers}...")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3
    )
    
    print("Producer connected successfully!")
    return producer


def generate_user_event():
    """Generate a random user event."""
    user_id = random.choice(USERS)
    return {
        "event_id": f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        "user_id": user_id,
        "action": random.choice(ACTIONS),
        "product": random.choice(PRODUCTS),
        "category": random.choice(CATEGORIES),
        "timestamp": datetime.now().isoformat(),
        "session_id": f"sess_{user_id}_{random.randint(100, 999)}"
    }


def generate_transaction_event():
    """Generate a random transaction event."""
    user_id = random.choice(USERS)
    amount = round(random.uniform(10.0, 500.0), 2)
    return {
        "transaction_id": f"txn_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        "user_id": user_id,
        "amount": amount,
        "currency": "USD",
        "product": random.choice(PRODUCTS),
        "status": random.choice(["completed", "completed", "completed", "pending"]),
        "timestamp": datetime.now().isoformat()
    }


def produce_events(producer, duration, rate):
    """Produce events to both topics for the specified duration."""
    print(f"\nProducing events for {duration} seconds at ~{rate} msgs/sec per topic...")
    print("Press Ctrl+C to stop early.\n")
    
    start_time = time.time()
    user_event_count = 0
    transaction_event_count = 0
    interval = 1.0 / rate if rate > 0 else 1.0
    
    try:
        while time.time() - start_time < duration:
            # Generate and send user event
            user_event = generate_user_event()
            producer.send(
                "user_events",
                key=user_event["user_id"],
                value=user_event
            )
            user_event_count += 1
            
            # Generate and send transaction event (less frequent)
            if random.random() < 0.5:  # 50% chance
                txn_event = generate_transaction_event()
                producer.send(
                    "transaction_events",
                    key=txn_event["user_id"],
                    value=txn_event
                )
                transaction_event_count += 1
            
            # Progress indicator
            if (user_event_count + transaction_event_count) % 10 == 0:
                elapsed = time.time() - start_time
                print(f"  [{elapsed:.1f}s] user_events: {user_event_count}, "
                      f"transaction_events: {transaction_event_count}")
            
            time.sleep(interval)
        
        # Flush remaining messages
        producer.flush()
        
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.flush()
    
    print(f"\nProduction complete!")
    print(f"  user_events: {user_event_count} messages")
    print(f"  transaction_events: {transaction_event_count} messages")
    
    return user_event_count, transaction_event_count


def main():
    parser = argparse.ArgumentParser(
        description="Produce sample events to Kafka for StreamFlow demo"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:9092",
        help="Kafka bootstrap servers (default: kafka:9092, use localhost:9094 for local)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds to produce events (default: 60)"
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=2,
        help="Approximate messages per second per topic (default: 2)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("StreamFlow Demo - Kafka Data Producer")
    print("=" * 60)
    print(f"Bootstrap Servers: {args.bootstrap_servers}")
    print(f"Duration: {args.duration} seconds")
    print(f"Rate: ~{args.rate} msgs/sec per topic")
    print("=" * 60)
    
    try:
        producer = create_producer(args.bootstrap_servers)
        produce_events(producer, args.duration, args.rate)
        producer.close()
        print("\nProducer closed. Ready for pipeline demo!")
        
    except KafkaError as e:
        print(f"\nKafka Error: {e}")
        print("\nTroubleshooting:")
        print("  - Is Kafka running? (docker-compose up)")
        print("  - For local: use --bootstrap-servers localhost:9094")
        print("  - For container: use --bootstrap-servers kafka:9092")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
