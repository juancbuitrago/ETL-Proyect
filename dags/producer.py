from kafka import KafkaProducer
from json import dumps
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt
import time

def load_config(filename="../config/database.ini", section="postgresql"):
    from configparser import ConfigParser
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
        return config
    else:
        raise Exception(f"Section {section} not found in the {filename} file")

def kafka_producer(config, table_name, topic_name):
    """Kafka producer games db"""
    db_url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    engine = create_engine(db_url)

    # Read data from the specified table
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)

    producer = KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092']  
    )

    for _, row in df.iterrows():
        row_json = row.to_json()
        producer.send(topic_name, value=row_json)
        print(f"Message sent at {dt.datetime.utcnow()}")
        time.sleep(2)

    print("All rows were sent successfully!")

if __name__ == "__main__":
    config = load_config()
    kafka_producer(config, 'merged_data', 'games_stream')
