import os
import sqlite3
from contextlib import contextmanager
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dc import *
from postgres_saver import PostgresSaver
from sqllite_extractor import SQLiteExtractor
from dotenv import load_dotenv

load_dotenv('../movies_admin/config/.env')


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)
    try:
        for table_name in ['film_work', 'genre', 'person', 'genre_film_work', 'person_film_work']:
            data = sqlite_extractor.fetch_batch_data(table_name)
            postgres_saver.inset_data(table_name, data)
        postgres_saver.cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()


if __name__ == '__main__':
    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': int(os.environ.get('DB_PORT')),
    }
    with conn_context('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
