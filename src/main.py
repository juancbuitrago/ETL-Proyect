"""PostgreSQL Connection"""

import sys
import csv
import psycopg2
sys.path.append('../config/')
from configuration import config


def connect():
    """ Connection with the Database """
    connection = None
    try:
        params = config()
        print('Connecting to the postgreSQL database ...')
        with psycopg2.connect(**params) as connection:

            # create a cursor
            with connection.cursor() as crsr:

                # drop in case exist
                crsr.execute('DROP TABLE IF EXISTS games')

                # create table
                create_table = """
                    CREATE TABLE IF NOT EXISTS games (
                        title VARCHAR(255) NOT NULL,
                        release_date VARCHAR(255) ,
                        developer VARCHAR(255) NOT NULL,
                        publisher VARCHAR(255) NOT NULL,
                        genres VARCHAR(255) NOT NULL,
                        product_rating VARCHAR(255) NOT NULL,
                        user_score VARCHAR(255),
                        user_ratings_count VARCHAR(255),
                        platforms_info VARCHAR(1500) ); """
                crsr.execute(create_table)

                with open('../data/Dataset.csv', newline='', encoding='utf-8') as file:
                    data = csv.reader(file, delimiter=';')
                    for row in data:
                        insert_data = crsr.mogrify(
                            """ INSERT INTO games ( Title, release_date, developer, publisher, genres, product_rating, user_score, user_ratings_count, platforms_info) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s );""", row)
                        crsr.execute(insert_data)
                        connection.commit()
                connection.commit()

    except (ImportError, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')


connect()
