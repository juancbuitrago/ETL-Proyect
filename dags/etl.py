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
    api_data = api_data[api_relevant_columns].copy()
    api_data['name'] = api_data['name'].str.strip().str.upper()
    api_data['released'] = pd.to_datetime(api_data['released']).dt.date
    api_data['name'] = api_data['name'].str.lower() \
                             .str.replace(r"\(.*\)", "", regex=True) \
                             .str.replace(r"[-:,$#.//'\[\]()]", "", regex=True)

    api_data.rename(columns={
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

    api_data['rating'] = api_data['rating'].map(rating_mapping)

    api_data = api_data.map(lambda x: x.upper() if isinstance(x, str) else x)

    return api_data


def merge_data(metacritic_data, api_data):
    """Merge dataset and API data"""
    logging.info("Columns in metacritic data before merge: %s", metacritic_data.columns.tolist())
    logging.info("Columns in API data before merge: %s", api_data.columns.tolist())
    
    metacritic_data['title'] = metacritic_data['title'].str.strip().str.upper()
    metacritic_data['released'] = pd.to_datetime(metacritic_data['released']).dt.date

    merged_data = pd.merge(api_data, metacritic_data, on=['title', 'released', 'genres', 'platforms', 'rating', 'ratings_count'], how='outer')
    merged_data.drop(columns=['metacritic_score', 'user score', 'developer', 'publisher'], inplace=True)
    merged_data = merged_data.dropna()
    logging.info("Merged data columns: %s", merged_data.columns.tolist())
    return merged_data


def load_data(merged_data):
    """Load data into PostgreSQL and create the dimensional model"""
    # Load configuration
    config = load_config()
    db_url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    engine = create_engine(db_url)
    conn = engine.connect()

    # Create dimension tables
    conn.execute("""
    CREATE TABLE IF NOT EXISTS platform (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS classification (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS genre (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS released (
        id SERIAL PRIMARY KEY,
        day INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        UNIQUE(day, month, year)
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS fact_game (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        platform_id INT REFERENCES platform(id),
        classification_id INT REFERENCES classification(id),
        genre_id INT REFERENCES genre(id),
        released_id INT REFERENCES released(id),
        ratings_count INT NOT NULL
    );
    """)

    def populate_dimension_table(table_name, column_name):
        distinct_values = merged_data[column_name].dropna().unique()
        for value in distinct_values:
            conn.execute(f"INSERT INTO {table_name} (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (value,))

    populate_dimension_table('platform', 'platforms')
    populate_dimension_table('classification', 'rating')
    populate_dimension_table('genre', 'genres')

    # Ensure 'released' column is in datetime format
    merged_data['released'] = pd.to_datetime(merged_data['released'], errors='coerce')

    # Populate released dimension
    released_data = merged_data[['released']].dropna().drop_duplicates()
    released_data['day'] = released_data['released'].dt.day
    released_data['month'] = released_data['released'].dt.month
    released_data['year'] = released_data['released'].dt.year
    for _, row in released_data.iterrows():
        conn.execute(
            "INSERT INTO released (day, month, year) VALUES (%s, %s, %s) ON CONFLICT (day, month, year) DO NOTHING",
            (row['day'], row['month'], row['year'])
        )

    # Populate fact table
    for _, row in merged_data.iterrows():
        platform_id = conn.execute("SELECT id FROM platform WHERE name = %s", (row['platforms'],)).fetchone()[0]
        classification_id = conn.execute("SELECT id FROM classification WHERE name = %s", (row['rating'],)).fetchone()[0]
        genre_id = conn.execute("SELECT id FROM genre WHERE name = %s", (row['genres'],)).fetchone()[0]
        release_id = conn.execute(
            "SELECT id FROM released WHERE day = %s AND month = %s AND year = %s",
            (row['released'].day, row['released'].month, row['released'].year)
        ).fetchone()[0]

        conn.execute(
            "INSERT INTO fact_game (title, platform_id, classification_id, genre_id, released_id, ratings_count) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (row['title'], platform_id, classification_id, genre_id, release_id, row['ratings_count'])
        )

    logging.info("Data loaded into the dimensional model successfully.")
