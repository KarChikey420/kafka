from kafka import KafkaConsumer
import json
import pandas as pd
import os

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
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    try:
        for message in consumer:
            data = message.value
            topic = message.topic
            
            print(f"Consumed: {data}")

            df = pd.DataFrame([data])
            df['source_topic'] = topic
            if not os.path.exists(CSV_FILE):
                df.to_csv(CSV_FILE, index=False)
            else:
                df.to_csv(CSV_FILE, mode='a', header=False, index=False)

    except KeyboardInterrupt:
        print("Consumer stopped manually.")

    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()
