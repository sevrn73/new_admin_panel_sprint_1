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

    data = sqlite_extractor.extract()
    postgres_saver.save_data(data)


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
        # sqlite3.connect('db.sqlite')
        load_from_sqlite(sqlite_conn, pg_conn)
