import json
import sys
import requests
import pandas as pd
from kafka import KafkaConsumer
import six

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

# Power BI streaming dataset endpoint URL
REST_API_URL = "https://api.powerbi.com/beta/693cbea0-4ef9-4254-8977-76e05cb5f556/datasets/ef202dbc-50ba-48a0-a751-a8a7bffe11b2/rows?experience=power-bi&key=gkakhRw%2BpsKPZZzdtf9MJhb0B7hDzSZN6yfPqbnnsbHEVWHCbYzyrYJP3VGZDUkFpDtpA9fqPpMyPj%2BKCInXoA%3D%3D"

# Kafka Consumer
consumer = KafkaConsumer(
    'games_stream',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='games_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consuming messages from topic: games_stream")

while True:
    data_raw = []

    for i in range(1):  # Adjust the range as needed for batch size
        message = next(consumer)
        row = json.loads(message.value)  # Convert JSON string to dictionary
        data_raw.append(row)
        print("Raw data - ", data_raw)

    # Set the header record
    HEADER = ["title", "released", "rating", "ratings_count", "genres", "platforms"]

    data_df = pd.DataFrame(data_raw, columns=HEADER)
    data_json = bytes(data_df.to_json(orient='records'), encoding='utf-8')
    print("JSON dataset", data_json)

    # Post the data on the Power BI API
    req = requests.post(REST_API_URL, data_json)
    print("Data posted in Power BI API")
