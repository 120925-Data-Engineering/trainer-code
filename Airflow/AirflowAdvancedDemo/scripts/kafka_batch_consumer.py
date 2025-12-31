"""
Kafka Batch Consumer for StreamFlow Pipeline
=============================================
Consumes messages from Kafka topics for a specified duration and writes
them to JSON files in the landing zone.

This script is called by the StreamFlow DAG's BashOperator task.
It can also be run locally for testing.

Usage:
    python kafka_batch_consumer.py --topics user_events,transaction_events \
        --duration 60 --output /opt/spark-data/landing/2024-01-15

Arguments:
    --topics: Comma-separated list of Kafka topics to consume
    --duration: How long to consume in seconds
    --output: Directory to write JSON files
    --bootstrap-servers: Kafka broker address (default: kafka:9092)
"""

import argparse
import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError


def create_consumer(bootstrap_servers, topics, group_id):
    """Create and return a Kafka consumer."""
    print(f"Connecting to Kafka at {bootstrap_servers}...")
    print(f"Topics: {topics}")
    print(f"Consumer Group: {group_id}")
    
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000  # Return after 5 seconds of no messages
    )
    
    print("Consumer connected successfully!")
    return consumer


def consume_to_files(consumer, topics, output_dir, duration):
    """Consume messages from topics and write to JSON files."""
    print(f"\nConsuming for {duration} seconds...")
    print(f"Output directory: {output_dir}")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize message storage per topic
    messages = {topic: [] for topic in topics}
    start_time = time.time()
    total_messages = 0
    
    try:
        while time.time() - start_time < duration:
            # Poll for messages
            records = consumer.poll(timeout_ms=1000)
            
            for topic_partition, msgs in records.items():
                topic = topic_partition.topic
                for msg in msgs:
                    messages[topic].append(msg.value)
                    total_messages += 1
            
            # Progress indicator
            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                print(f"  [{elapsed:.0f}s] Total messages consumed: {total_messages}")
        
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    
    # Write messages to JSON files
    print("\nWriting files...")
    files_written = []
    
    for topic, topic_messages in messages.items():
        if topic_messages:
            output_file = os.path.join(output_dir, f"{topic}.json")
            
            # Write as JSON Lines format (one JSON object per line)
            with open(output_file, "w") as f:
                for msg in topic_messages:
                    f.write(json.dumps(msg) + "\n")
            
            files_written.append(output_file)
            print(f"  {output_file}: {len(topic_messages)} records")
        else:
            print(f"  {topic}: No messages received")
    
    return files_written, total_messages


def main():
    parser = argparse.ArgumentParser(
        description="Batch consume from Kafka topics to JSON files"
    )
    parser.add_argument(
        "--topics",
        required=True,
        help="Comma-separated list of topics to consume"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds to consume (default: 60)"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for JSON files"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:9092",
        help="Kafka bootstrap servers (default: kafka:9092)"
    )
    parser.add_argument(
        "--group-id",
        default="streamflow_batch_consumer",
        help="Consumer group ID (default: streamflow_batch_consumer)"
    )
    
    args = parser.parse_args()
    topics = [t.strip() for t in args.topics.split(",")]
    
    print("=" * 60)
    print("StreamFlow Pipeline - Kafka Batch Consumer")
    print("=" * 60)
    print(f"Bootstrap Servers: {args.bootstrap_servers}")
    print(f"Topics: {topics}")
    print(f"Duration: {args.duration} seconds")
    print(f"Output: {args.output}")
    print("=" * 60)
    
    try:
        consumer = create_consumer(
            args.bootstrap_servers,
            topics,
            args.group_id
        )
        
        files_written, total_messages = consume_to_files(
            consumer,
            topics,
            args.output,
            args.duration
        )
        
        consumer.close()
        
        print("\n" + "=" * 60)
        print("Batch consumption complete!")
        print(f"Total messages: {total_messages}")
        print(f"Files written: {len(files_written)}")
        for f in files_written:
            print(f"  - {f}")
        print("=" * 60)
        
        # Exit with error if no messages consumed (helps DAG detect issues)
        if total_messages == 0:
            print("\nWARNING: No messages consumed. Is data being produced?")
            # Don't fail - empty topics are valid in some scenarios
        
        return 0
        
    except KafkaError as e:
        print(f"\nKafka Error: {e}")
        print("\nTroubleshooting:")
        print("  - Is Kafka running?")
        print("  - Are the topics created?")
        print("  - For local testing: use --bootstrap-servers localhost:9094")
        return 1
    except Exception as e:
        print(f"\nError: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
