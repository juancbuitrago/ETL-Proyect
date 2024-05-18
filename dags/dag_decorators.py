# dag.py
from datetime import timedelta, datetime
from airflow.decorators import dag, task
import etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 16),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(default_args=default_args, description='ETL DAG for merging Spotify and Grammy data', schedule_interval='@daily', tags=['data merging'])
def data_merging_etl_dag():
    @task
    def extract_spotify_data():
        return etl.read_spotify_data()

    @task
    def load_and_fetch_grammy_data():
        etl.read_and_store_grammy_data()
        return etl.fetch_grammy_data()

    @task
    def transform_spotify_data(spotify_data):
        return etl.apply_transformations_to_spotify(spotify_data)

    @task
    def transform_grammy_data(grammy_data):
        return etl.clean_grammy_data(grammy_data)

    @task
    def merge_data(spotify_data, grammy_data):
        return etl.merge_data_sets(spotify_data, grammy_data)
    
    @task
    def upload_data_to_database(merged_data):
        table_name = 'merged_data_table'
        etl.upload_data_to_db(merged_data, table_name)

    @task
    def upload_to_drive(merged_data):
        filename = 'merged_data.csv'
        folder_id = '1ado1F0k-g0rE5ESvfMdqqhpvVXj048fj'
        etl.upload_data_to_drive(merged_data, filename, folder_id)

    spotify_data = extract_spotify_data()
    grammy_data = load_and_fetch_grammy_data()
    transformed_spotify = transform_spotify_data(spotify_data)
    transformed_grammy = transform_grammy_data(grammy_data)
    merged_data = merge_data(transformed_spotify, transformed_grammy)
    upload_data_to_database(merged_data)
    upload_to_drive(merged_data)

spotify_grammy_etl_workflow = data_merging_etl_dag()