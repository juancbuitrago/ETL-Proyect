''' ETL DAG '''

import logging
import json
from configparser import ConfigParser
import re
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


def load_config(filename="config/database.ini", section="postgresql"):
    """Load configuration from .ini file"""
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
        logging.info("Configuration file loaded successfully.")
        return config
    else:
        logging.error("Section not found%s", section)
        return None


config = load_config()


def extract_metacritic_data():
    """Extract data from Metacritic"""
    if config is None:
        logging.error("Configuration not loaded.")
        return None
    db_url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    engine = create_engine(db_url)
    try:
        query = "SELECT * FROM games_data"
        metacritic_data = pd.read_sql(query, engine)
        logging.info("Metacritic data extracted successfully")
        return metacritic_data
    except ImportError as e:
        logging.error("Database connection failed:%s", e)
        return None


def extract_api_data():
    """Extract data from the API"""
    if config is None:
        logging.error("Configuration not loaded.")
        return None
    db_url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    engine = create_engine(db_url)
    try:
        query = "SELECT * FROM api_data"
        api_data = pd.read_sql(query, engine)
        logging.info("API data extracted successfully")
        return api_data
    except ImportError as e:
        logging.error("Database connection failed: %s", e)
        return None


def transform_metacritic_data(metacritic_data):
    """Transform metacritic data"""
    metacritic_data['release date'] = pd.to_datetime(metacritic_data['release date'], format='%m/%d/%Y', errors='coerce')
    metacritic_data['user ratings count'] = pd.to_numeric(metacritic_data['user ratings count'], errors='coerce').fillna(0).astype(int)
    metacritic_data['user score'] = pd.to_numeric(metacritic_data['user score'], errors='coerce').fillna(0).astype(float)

    def extract_first_platform_name(json_str):
        if json_str == 'Platforms Info' or json_str == '':
            return None
        try:
            json_data = json.loads(json_str.replace("'", '"'))
            if json_data:
                first_platform_name = json_data[0]['Platform']
                return first_platform_name
        except json.JSONDecodeError as e:
            logging.error("Failed to load %s", e)
        return None

    metacritic_data['platforms'] = metacritic_data['platforms info'].apply(extract_first_platform_name)
    metacritic_data.drop(columns=['platforms info'], inplace=True)
    metacritic_data.rename(columns={
        'product rating': 'rating',
        'release date': 'released',
        'user ratings count': 'ratings_count'
    }, inplace=True)
    metacritic_data['title'] = metacritic_data['title'].str.lower() \
                             .str.replace(r"\(.*\)", "", regex=True) \
                             .str.replace(r"[-:,$#.//'\[\]()]", "", regex=True)
    metacritic_data = metacritic_data.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    return metacritic_data


def transform_api_data(api_data):
    """Transform API data"""
    def extract_classification_names(json_str):
        if isinstance(json_str, str):
            try:
                classification_data = json.loads(json_str.replace("'", '"'))
                if isinstance(classification_data, list):
                    classification_names = [classification['name'] for classification in classification_data]
                    return ', '.join(classification_names)
                elif 'name' in classification_data:
                    return classification_data['name']
                else:
                    return None
            except json.JSONDecodeError:
                return None
        else:
            return None
    api_data['rating'] = api_data['esrb_rating'].apply(extract_classification_names)
    
    def extract_first_genre_name(json_str):
        if isinstance(json_str, str):
            try:
                genre_data = json.loads(json_str.replace("'", '"'))
                if isinstance(genre_data, dict):
                    return genre_data['name']
                elif isinstance(genre_data, list) and len(genre_data) > 0:
                    return genre_data[0]['name']
                else:
                    return None
            except json.JSONDecodeError:
                return None
        else:
            return None
    api_data['genres'] = api_data['genres'].apply(extract_first_genre_name)

    def extract_first_platform_name_from_string(json_str, index):
        if not json_str or json_str.strip() in ['platforms', '']:
            print(f"Row {index}: Empty or placeholder string")
            return None
        
        # Check if the string is a plain text value
        if json_str.isalpha():
            print(f"Row {index}: Plain text value: {json_str}")
            return json_str
        
        try:
            # Replace single quotes with double quotes to form valid JSON
            json_str = json_str.replace("'", '"')
            
            # Find the platform names using regular expressions
            matches = re.findall(r'"name":\s*"([^"]+)"', json_str)
            if matches:
                return matches[0]  # Return the first match
        except ImportError as e:
            print("Error processing string:%S", e)
            return None

    api_data['platforms'] = api_data.apply(lambda row: extract_first_platform_name_from_string(row['platforms'], row.name), axis=1)

    api_relevant_columns = [
        'name', 'released', 'rating', 'ratings_count', 'metacritic', 'genres', 'platforms'
    ]
    api_data_relevant = api_data[api_relevant_columns].copy()
    api_data_relevant['name'] = api_data_relevant['name'].str.strip().str.upper()
    api_data_relevant['released'] = pd.to_datetime(api_data_relevant['released']).dt.date
    api_data_relevant['name'] = api_data_relevant['name'].str.lower() \
                             .str.replace(r"\(.*\)", "", regex=True) \
                             .str.replace(r"[-:,$#.//'\[\]()]", "", regex=True)
    api_data_relevant.rename(columns={
        'name': 'title',
        'rating': 'rating',
        'metacritic': 'metacritic_score',
        'ratings_count': 'ratings_count'
    }, inplace=True)

    rating_mapping = {
        "Adults Only": "RATED AO FOR ADULTS ONLY",
        "Everyone": "RATED E FOR EVERYONE",
        "Everyone +10": "RATED E +10 FOR EVERYONE +10",
        "Mature": "RATED M FOR MATURE",
        "Rating Pending": "RATED RP FOR RATE PENDING",
        "Teen": "RATED T FOR TEEN"
    }
    api_data_relevant['rating'] = api_data_relevant['rating'].map(rating_mapping)
    api_data_relevant = api_data_relevant.map(lambda x: x.upper() if isinstance(x, str) else x)
    return api_data


def merge_data(metacritic_data, api_data):
    """Merge dataset and API data"""
    merged_data = pd.merge(api_data, metacritic_data, on=['title', 'released', 'genres', 'platforms'], how='outer')
    merged_data.drop(columns=['metacritic_score', 'user score', 'developer', 'publisher'], inplace=True)
    cleaned_merged_data = merged_data.dropna()
    return cleaned_merged_data


def load_data(merged_data):
    """Load data into PostgreSQL"""
    config = Config_Path
    db_url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    engine = create_engine(db_url)

    try:
        conn = engine.connect()
        merged_data.to_sql('merged_data', conn, if_exists='replace', index=False)
        logging.info("Data loaded into the PostgreSQL database successfully.")
    except ImportError as e:
        logging.debug("Failed to load data into the PostgreSQL database: %s", e)
