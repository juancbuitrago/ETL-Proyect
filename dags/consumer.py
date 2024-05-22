"""Consumer kafka games stream"""
import json
import sys
from kafka import KafkaConsumer
import six


if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

def consume_messages(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='games_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Consuming messages from topic: {topic_name}")

    for message in consumer:
        try:
            # Print raw message
            print(f"Raw message: {message.value}")

            # Handle empty messages
            if not message.value:
                print("Received empty message")
                continue

            # Deserialize and print message
            data = json.loads(message.value.decode('utf-8'))
            print(f"Received message: {data}")

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e} - Raw message: {message.value}")


if __name__ == "__main__":
    consume_messages('games_stream')
