from kafka import KafkaConsumer
import json
import pandas as pd
import os
import uuid

KAFKA_TOPIC = ['demo-topic', 'test']
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
CSV_FILE = 'messages.csv'
CONSUMER_GROUP_ID = 'unique-consumer-group'

def consume_messages():
    consumer = KafkaConsumer(
        *KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: safe_json_deserialize(m)
    )

    try:
        print(f"[+] Listening to topics: {KAFKA_TOPIC}")
        for message in consumer:
            data = message.value
            topic = message.topic

            # Attach UUID per message for uniqueness
            data['_uuid'] = str(uuid.uuid4())
            print(f"[✓] Consumed from {topic} | Data: {data}")

            df = pd.DataFrame([data])
            df['source_topic'] = topic

            if not os.path.exists(CSV_FILE):
                df.to_csv(CSV_FILE, index=False)
            else:
                df.to_csv(CSV_FILE, mode='a', header=False, index=False)

    except KeyboardInterrupt:
        print("[!] Consumer stopped manually.")
    finally:
        consumer.close()
        print("[x] Kafka consumer connection closed.")

def safe_json_deserialize(message_bytes):
    try:
        return json.loads(message_bytes.decode('utf-8'))
    except json.JSONDecodeError:
        return {"error": "Invalid JSON format", "raw": message_bytes.decode('utf-8')}

if __name__ == "__main__":
    consume_messages()
