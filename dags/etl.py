''' ETL DAG '''

import json
import logging
import os
from dotenv import load_dotenv
import psycopg2
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

# Configuration paths and file names
Config_Path = os.getenv('DB_PATH')

"""def load_config():
    with open(Config_Path, 'r') as file:
        return json.load(file)"""


def extract_metacritic_data():
    """read the original dataset"""
    config = Config_Path

    db_url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    engine = create_engine(db_url)

    try:
        conn = engine.connect()
        print("Database connection successful.")
    except Exception as e:
        print("Database connection failed:", e)
        sys.exit(1)


def read_and_store_grammy_data():
    config = load_config()
    db_url = f"postgresql+pg8000://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    engine = create_engine(db_url)
    grammy_data = pd.read_csv(grammy_file)
    grammy_data.to_sql('grammy_awards', engine, index=False, if_exists='replace')
    logging.info("Grammy data loaded to DB.")

def fetch_grammy_data():
    config = load_config()
    db_url = f"postgresql+pg8000://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    engine = create_engine(db_url)
    return pd.read_sql_table('grammy_awards', engine)

def apply_transformations_to_spotify(data):
    # Define the genre categories with their respective sub-genres
    genre_mapping = {
        'Rock': ['alt-rock', 'grunge', 'hard-rock', 'punk-rock', 'rock', 'rock-n-roll', 'goth', 'punk', 'psych-rock', 'j-rock', 'acoustic', 'british', 'rockabilly'],
        'Pop': ['pop', 'power-pop', 'pop-film', 'k-pop', 'j-pop', 'cantopop', 'children', 'disney', 'happy', 'kids', 'mandopop', 'mpb', 'power-pop', 'romance', 'sad', 'singer-songwriter', 'spanish'],
        'Electronic/Dance': ['electronic', 'dubstep', 'edm', 'electro', 'techno', 'trance', 'house', 'deep-house', 'disco', 'dancehall', 'chicago-house', 'detroit-techno', 'hardstyle', 'minimal-techno', 'j-dance', 'party', 'breakbeat', 'drum-and-bass', 'dub', 'progressive-house', 'trip-hop'],
        'Hip-Hop/R&B': ['hip-hop', 'r-n-b', 'j-idol', 'afrobeat'],
        'Metal': ['black-metal', 'death-metal', 'heavy-metal', 'metal', 'metalcore', 'grindcore', 'industrial', 'hardcore'],
        'Jazz/Blues': ['jazz', 'blues'],
        'Folk/Country': ['folk', 'country', 'bluegrass', 'forro', 'honky-tonk'],
        'Latin': ['latin', 'salsa', 'samba', 'reggaeton', 'latino'],
        'Classical/Opera': ['classical', 'opera', 'piano'],
        'Indie/Alternative': ['alternative', 'indie', 'indie-pop', 'singer-songwriter', 'emo', 'ska'],
        'World Music': ['world-music', 'brazil', 'indian', 'iranian', 'malay', 'mandopop', 'swedish', 'turkish', 'french', 'german', 'reggae', 'synth-pop'],
        'Ambient/Chill/Downtempo': ['ambient', 'chill', 'new-age', 'sleep', 'tango', 'study'],
        'Funk/Soul': ['funk', 'soul', 'gospel', 'groove']
    }

    # Initialize all tracks as 'Other'
    data['genre'] = 'Other'

    # Assign genres based on the mapping
    for genre, sub_genres in genre_mapping.items():
        data.loc[data['track_genre'].isin(sub_genres), 'genre'] = genre

    # Drop duplicates based on track ID
    data.drop_duplicates(subset=['track_id'], inplace=True)

    # Ensure that the 'artists' column does not contain NaN values
    data['artists'] = data['artists'].fillna('Unknown Artist')

    # Extract the lead artist from 'artists' (assuming multiple artists are separated by semicolons)
    data['lead_artist'] = data['artists'].apply(lambda x: x.split(';')[0])

    # Categorize 'popularity' into three levels
    data['popularity_level'] = pd.cut(data['popularity'], bins=[0, 33, 66, 100], labels=['Low', 'Medium', 'High'])

    # Define columns that are unnecessary and drop them
    unwanted_columns = [
        'Unnamed: 0',
        'track_id',
        'key',
        'mode',
        'instrumentalness',
        'time_signature',
        'liveness',
        'valence'
    ]
    data.drop(columns=unwanted_columns, inplace=True)

    # Further data cleanup and transformations can be added here
    logging.info("Transformations applied to Spotify data")

    return data

def clean_grammy_data(data):
    data['artist_clean'] = data['artist'].fillna(data['workers']).str.extract(r'([^\+]+)')[0]
    data['category_clean'] = data['category'].str.replace(r'\[|\]', '', regex=True)
    return data

def merge_data_sets(spotify, grammy):
    merged_data = pd.merge(spotify, grammy, left_on='track_name', right_on='nominee', how='left')
    merged_data.drop(columns=['workers', 'artist', 'year', 'img', 'updated_at', 'published_at'], inplace=True)
    merged_data.fillna({'title': 'not nominated', 'category': 'not nominated', 'nominee': 'not nominated', 'winner': False}, inplace=True)
    return merged_data

def upload_data_to_db(merged_data, table_name):
    try:
        # Load the database configuration
        config = load_config()
        db_url = f"postgresql+pg8000://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
        engine = create_engine(db_url)
        
        # Upload the DataFrame to the specified table
        merged_data.to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        logging.info(f"Data successfully uploaded to the database table {table_name}.")
    except Exception as e:
        logging.error(f"Failed to upload data to the database: {e}")
        raise

def upload_data_to_drive(data, filename, folder_id):
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = '/opt/airflow/config/service_account.json'
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)

    csv_bytes = data.to_csv(index=False).encode('utf-8')
    csv_io = BytesIO(csv_bytes)
    file_metadata = {'name': filename, 'parents': [folder_id]}
    media = MediaIoBaseUpload(csv_io, mimetype='text/csv', resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    logging.info(f"File {filename} uploaded with ID: {file.get('id')}")