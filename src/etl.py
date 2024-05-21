""" ETL PROCESS"""

import pandas as pd
import psycopg2
import json
from dotenv import load_dotenv
import tempfile
import os
import sys
from io import StringIO

sys.path.append('../config/')
import dbconfig


load_dotenv()
metacritic_games = os.getenv("ORIGIN_DS1")
rawg_games = os.getenv("ORIGIN_DS2")
metacritic_data = os.getenv("DS1_PATH")
rawg_data = os.getenv("DS2_PATH")
merge_data = os.getenv("MERGE_PATH")


def create_insert_metacritic_data():
    ''' This function create and insert the metacritic csv data'''
    dataset_location = metacritic_games
    connection = None

    try:
        params = dbconfig.configuration()
        print('Connecting to the postgreSQL database ...')
        with psycopg2.connect(**params) as connection:

            with connection.cursor() as crsr:

                crsr.execute('DROP TABLE IF EXISTS metacritic_data')
                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS metacritic_data (
                        title VARCHAR(255),
                        release_date VARCHAR(255),
                        developer VARCHAR(255),
                        publisher VARCHAR(255),
                        genres VARCHAR(255),
                        product_rating VARCHAR(255),
                        user_score FLOAT,
                        user_ratings_count INT,
                        platforms_info VARCHAR(2000)
                    )
                """
                crsr.execute(create_table_query)
                connection.commit()
                print("metacritic_data table created successfully")

                # Crear un archivo temporal para escribir las líneas limpiadas
                with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
                    with open(dataset_location, 'r', encoding="utf-8") as file:
                        next(file)  # Omitir la primera línea (encabezado)
                        for line in file:
                            cleaned_line = line.replace('";"', '","')
                            temp_file.write(cleaned_line)

                # Usar el archivo temporal para copiar los datos a la base de datos
                with open(temp_file.name, 'r', encoding='utf-8') as cleaned_file:
                    crsr.copy_from(cleaned_file, 'metacritic_data', sep=';', null='')
                    connection.commit()
                    print("metacritic_data inserted successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        # Cerrar la conexión
        if connection is not None:
            connection.close()
            print('Database metacritic_data connection terminated.')


def transform_load_metacritic():
    ''' This function do the transformation required for the merge'''

    connection = None
    try:
        params = dbconfig.configuration()
        print('Connecting to the postgreSQL database ...')
        with psycopg2.connect(**params) as connection:

            with connection.cursor() as crsr:

                crsr.execute("SELECT * FROM metacritic_data")
                games_data = crsr.fetchall()

                # Convertir games_data en un DataFrame de Pandas
                games_data = pd.DataFrame(games_data, columns=['title', 'release_date', 'developer', 'publisher', 'genres', 'product_rating', 'user_score', 'user_ratings_count', 'platforms_info'])


                games_data.replace('', pd.NA, inplace = True)
                games_data['release_date'] = pd.to_datetime(games_data['release_date'], format='%m/%d/%Y', errors='coerce')
                games_data['user_ratings_count'] = pd.to_numeric(games_data['user_ratings_count'], errors='coerce').fillna(0).astype(int)
                games_data['user_score'] = pd.to_numeric(games_data['user_score'], errors='coerce').fillna(0).astype(float)

                def convert_platforms_info(platforms_info_str):
                    try:
                        platforms_info_json = json.loads(platforms_info_str.replace("'", '"'))

                        platform_names = [platform['Platform'] for platform in platforms_info_json]
                        return ', '.join(platform_names)
                    except (json.JSONDecodeError, TypeError):
                        return None

                games_data['platforms_info'] = games_data['platforms_info'].apply(convert_platforms_info)

                def extract_platform_names_from_json(json_str):
                    if json_str == 'platforms_info' or json_str == '':
                        return None
                    try:
                        json_data = json.loads(json_str.replace("'", '"'))
                        if isinstance(json_data, list):
                            platform_names = [item.get('Platform', '') for item in json_data]
                            return ', '.join(platform_names)
                        else:
                            return None
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"Error parsing JSON: {e}. Input: {json_str}")
                        return None
                    
                games_data['platforms_info'] = games_data['platforms_info'].apply(extract_platform_names_from_json)

                games_data.drop(columns=['platforms_info'], inplace=True)

                games_data.rename(columns={
                    'product_rating': 'rating',
                    'release_date': 'released',
                    'user_ratings_count': 'ratings_count'
                }, inplace=True)
                
                games_data['title'] = games_data['title'].str.lower() \
                                    .str.replace(r"\(.*\)", "", regex=True) \
                                    .str.replace(r"[-:,#.//\[\]()]", "", regex=True)
                
                games_data = games_data.applymap(lambda x: x.upper() if isinstance(x, str) else x)

                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS metacritic_data_transformed (
                        title VARCHAR(255),
                        released DATE,
                        developer VARCHAR(255),
                        publisher VARCHAR(255),
                        genres VARCHAR(255),
                        classification VARCHAR(255),
                        user_score FLOAT,
                        ratings_count INT,
                        platforms_info VARCHAR(1500)
                    )
                """
                crsr.execute(create_table_query)
                connection.commit()
                print("Table created successfully")

                csv_data = games_data.to_csv(index=False, header=False, sep=';')
                csv_file = StringIO(csv_data)
                next(csv_file)
                crsr.copy_from(csv_file, 'metacritic_data', sep=';', null='')
                connection.commit()

    except (ImportError, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')


def extract_data_API():
    '''This function extract the data from the api'''

    rawg_games_data = pd.read_csv(rawg_games, delimiter=',', encoding='utf-8')

    connection = None
    try:
        params = dbconfig.configuration()
        print('Connecting to the postgreSQL database ...')
        with psycopg2.connect(**params) as connection:

            with connection.cursor() as crsr:

                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS rawg_data (
                        id INTEGER,
                        slug VARCHAR(1000),
                        title VARCHAR(1000),
                        released DATE,
                        tba BOOLEAN,
                        background_image TEXT,
                        rating FLOAT,
                        rating_top INTEGER,
                        ratings TEXT,
                        ratings_count INTEGER,
                        reviews_text_count INTEGER,
                        added TIMESTAMP,
                        added_by_status TEXT,
                        metacritic FLOAT,
                        playtime INTEGER,
                        suggestions_count INTEGER,
                        updated TIMESTAMP,
                        user_game BOOLEAN,
                        reviews_count INTEGER,
                        saturated_color VARCHAR(7),
                        dominant_color VARCHAR(7),
                        platforms JSONB,
                        parent_platforms JSONB,
                        genres JSONB,
                        stores JSONB,
                        clip TEXT,
                        tags JSONB,
                        esrb_rating JSONB,
                        short_screenshots JSONB
                    )
                """
                crsr.execute(create_table_query)
                connection.commit()
                print("rawg_games_data Table created successfully")

                crsr.copy_from(rawg_games_data, 'rawg_data', sep=',', null='')
                connection.commit()
                print("rawg_games_data inserted successfully.")
                
    except (ImportError, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')
    
    return rawg_raw_data


def transformations_rawg(rawg_raw_data):
    ''' This function transform the raw data from api'''

    rawg_games_data['released'] = pd.to_datetime(rawg_games_data['released'], errors='coerce')
    
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

    rawg_games_data['rating'] = rawg_games_data['esrb_rating'].apply(extract_classification_names)

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

    rawg_games_data['genres'] = rawg_games_data['genres'].apply(extract_first_genre_name)

    def preprocess_json_string(json_str):
        # Escape newline characters
        json_str = json_str.replace('\n', '\\n')
        
        # Remove HTML tags
        json_str = re.sub(r'<[^>]*>', '', json_str)
        
        # Replace other escape sequences with their corresponding characters
        json_str = json_str.replace('\\r\\n', '\\n').replace('\\t', '\\t').replace('\\xa0', ' ')
        
        return json_str

    def extract_platform_names_clean(platforms_str):
        try:
            platforms_str = preprocess_json_string(platforms_str)
            
            cleaned_str = re.sub(r'(<.*?>)', '', platforms_str) 
            cleaned_str = re.sub(r'"requirements_en": \{.*?\},', '', cleaned_str)

            formatted_str = cleaned_str.replace("'", '"').replace('None', 'null').replace('\r\n', ' ').replace('\n', ' ')

            platforms_data = json.loads(formatted_str)
            
            platform_names = [platform['platform']['name'] for platform in platforms_data]
            return platform_names
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error: {e} - {platforms_str}")
            return []
        except KeyError as e:
            print(f"Key Error: {e} - {platforms_str}")
            return []
        except Exception as e:
            print(f"Unexpected Error: {e} - {platforms_str}")
            return []

    rawg_games_data['platforms'] = rawg_games_data['platforms'].apply(extract_platform_names_clean)

    def get_first_component(arr):
        if isinstance(arr, list) and len(arr) > 0:
            return arr[0]
        else:
            return None

    # Apply the function to the 'platforms' column and overwrite the values in the same column
    rawg_games_data['platforms'] = rawg_games_data['platforms'].apply(get_first_component)

    columns_to_drop = ['id', 'tags', 'ratings', 'esrb_rating', 'stores', 'slug', 'tba', 'background_image', 'rating_top', 'reviews_text_count', 'added', 'added_by_status', 'parent_platforms', 'clip', 'short_screenshots', 'playtime', 'suggestions_count', 'user_game', 'metacritic', 'updated', 'reviews_count', 'saturated_color', 'dominant_color']
    rawg_games_data = rawg_games_data.drop(columns_to_drop, axis=1)

    rawg_games_data = rawg_games_data.rename(columns={'name': 'title'})

    connection = None
    try:
        params = configuration()
        print('Connecting to the postgreSQL database ...')
        with psycopg2.connect(**params) as connection:

            with connection.cursor() as crsr:
        
                table_query = f"""
                    CREATE TABLE IF NOT EXISTS rawg_data_transformed(
                        title VARCHAR(1000),
                        released DATE,
                        rating VARCHAR(255),
                        ratings_count INT,
                        platforms VARCHAR(255),
                        genres VARCHAR(255)
                    )
                """
                crsr.execute(table_query)
                connection.commit()
                print('Table created successfully')

                rawg_games_data.to_sql(name=rawg_data_transformed, con=connection, if_exists='append', index=False)
                print('Rawg Games Transformed Dataframe was saved in the dataset successfully')

    except (ImportError, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')


def merge_data():
    '''This function do the merge between metacritic data and rawg data'''

    connection = None
    try:
        params = configuration()
        print('Connecting to the postgreSQL database ...')
        with psycopg2.connect(**params) as connection:

            with connection.cursor() as crsr:
                create_table = """
                    CREATE TABLE IF NOT EXISTS merge_data (
                        title VARCHAR(255),
                        released_x DATE,
                        developer VARCHAR(255),
                        publisher VARCHAR(255),
                        genres_x VARCHAR(255),
                        rating_x VARCHAR(255),
                        user_score FLOAT,
                        ratings_count_x INT,
                        platforms_x VARCHAR(1500),
                        released_y DATE,
                        rating_y VARCHAR(255),
                        ratings_count_y INT,
                        platforms_y VARCHAR(1500),
                        genres_y VARCHAR(255)
                    )
                """
                crsr.execute(create_table)
                connection.commit()
                print("Table merge_data created successfully")

                crsr.execute("SELECT * FROM rawg_data_transformed")
                rawg_data = crsr.fetchall()

                crsr.execute("SELECT * FROM metacritic_data_transformed")
                metacritic_data = crsr.fetchall()

                metacritic_data['title'] = metacritic_data['title'].str.upper()
                rawg_data['title'] = rawg_data['title'].str.upper()

                merged_data = pd.merge(metacritic_data, rawg_data, how='outer', on='title')

                merged_data.to_csv('../data/merge_data.csv', index=False)

                merged_data.to_sql(name='merge_data', con=connection, if_exists='replace', index=False)

    except (ImportError, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')

print("Starting ETL process...")
create_insert_metacritic_data()
print("Metacritic data processing completed.")

print("Transforming and loading Metacritic data...")
transform_load_metacritic()
print("Metacritic data transformation completed.")

print("Extracting data from API...")
extract_data_API()
print("Data extraction from API completed.")

print("Transforming RAWG data...")
transformations_rawg(rawg_raw_data)
print("RAWG data transformation completed.")

print("Merging data...")
merge_data()
print("Data merge completed.")

print("ETL process completed successfully.")