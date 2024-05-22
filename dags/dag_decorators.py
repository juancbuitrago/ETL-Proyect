''' Decorators'''

from datetime import timedelta, datetime
from airflow.decorators import dag, task
import etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(default_args=default_args, description='ETL DAG for merging dataset and API data', schedule_interval='@daily', tags=['data merging'])


def data_merging_etl_dag():
    '''Game data workflow'''
    @task
    def extract_metacritic_data():
        return etl.extract_metacritic_data()

    @task
    def extract_api_data():
        return etl.extract_api_data()

    @task
    def transform_metacritic_data(metacritic_data):
        return etl.transform_metacritic_data(metacritic_data)

    @task
    def transform_api_data(api_data):
        return etl.transform_api_data(api_data)

    @task
    def merge_data(transformed_metacritic_data, transformed_api_data):
        return etl.merge_data(transformed_metacritic_data, transformed_api_data)

    @task
    def load_data(merged_data):
        etl.load_data(merged_data)

    @task
    def send_to_kafka():
        etl.send_to_kafka()

    metacritic_data = extract_metacritic_data()
    api_data = extract_api_data()
    transformed_metacritic_data = transform_metacritic_data(metacritic_data)
    transformed_api_data = transform_api_data(api_data)
    merged_data = merge_data(transformed_metacritic_data, transformed_api_data)
    loaded_data = load_data(merged_data)
    send_to_kafka_task = send_to_kafka()
    loaded_data.set_downstream(send_to_kafka_task)


ETL_WORKFLOW = data_merging_etl_dag()
