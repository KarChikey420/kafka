from kafka import KafkaProducer
import json
import time
import random

KAFKA_TOPIC_1 = 'demo-topic'
KAFKA_TOPIC_2 = 'test'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

def produce_messages():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    i = 0
    try:
        while True:
            message_topic_1 = {
                "id": i,
                "text": f"hello how are you - {i}",
                "timestamp": time.time()
            }
            print(f"Producing to {KAFKA_TOPIC_1}: {message_topic_1}")
            producer.send(KAFKA_TOPIC_1, value=message_topic_1)

            message_topic_2 = {
                "id": i,
                "text": f"All goood! - {i}",
                "random_number": random.randint(1, 100),
                "timestamp": time.time()
            }
            print(f"Producing to {KAFKA_TOPIC_2}: {message_topic_2}")
            producer.send(KAFKA_TOPIC_2, value=message_topic_2)

            i += 1
            time.sleep(2)  

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    produce_messages()
