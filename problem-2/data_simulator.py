"""
Simple Order Data Simulator
Generates order events and sends them to Kafka
"""

from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

# Kafka config
KAFKA_SERVERS = "localhost:9094"
TOPIC = "orders"

# Sample data
PRODUCT_IDS = [f"PROD-{str(i).zfill(5)}" for i in range(1, 11)]
USER_IDS = [f"USER-{str(i).zfill(4)}" for i in range(1, 51)]
STATUSES = ["CANCEL", "COMPLETE", "PENDING"]

def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_SERVERS
    })

def generate_order(order_id):
    return {
        "order_id": f"ORD-{order_id}",
        "order_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": random.choice(USER_IDS),
        "product_id": random.choice(PRODUCT_IDS),
        "quantity": random.randint(1, 3),
        "status": random.choice(STATUSES)
    }

def main():
    producer = create_producer()
    order_id = 1

    print("Starting order data simulator...")
    print("Press Ctrl+C to stop")

    try:
        while True:
            order = generate_order(order_id)
            producer.produce(
                TOPIC,
                value=json.dumps(order)
            )
            producer.flush()

            print(f"Sent order: {order}")
            order_id += 1

            time.sleep(2)  # send 1 order every 2 seconds

    except KeyboardInterrupt:
        print("\nSimulator stopped")

if __name__ == "__main__":
    main()
