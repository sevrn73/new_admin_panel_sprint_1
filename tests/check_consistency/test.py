import os
import psycopg2
from datetime import datetime
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
import sqlite3
from contextlib import contextmanager

load_dotenv('movies_admin/config/.env')

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': int(os.environ.get('DB_PORT')),
}


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()


def test_consistency_filmwork():
    """
    Тест полноты переноса данных таблицы Filmwork из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_sqlite.execute('SELECT COUNT(*) FROM film_work;')
        count_sqlite = curs_sqlite.fetchall()[0][0]
        curs_pg = pg_conn.cursor()
        curs_pg.execute('SELECT COUNT(*) FROM content.film_work;')
        count_pq = curs_pg.fetchall()[0][0]
    assert count_sqlite == count_pq


def test_consistency_genre():
    """
    Тест полноты переноса данных таблицы Genre из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_sqlite.execute('SELECT COUNT(*) FROM genre;')
        count_sqlite = curs_sqlite.fetchall()[0][0]
        curs_pg = pg_conn.cursor()
        curs_pg.execute('SELECT COUNT(*) FROM content.genre;')
        count_pq = curs_pg.fetchall()[0][0]
    assert count_sqlite == count_pq


def test_consistency_person():
    """
    Тест полноты переноса данных таблицы Person из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_sqlite.execute('SELECT COUNT(*) FROM person;')
        count_sqlite = curs_sqlite.fetchall()[0][0]
        curs_pg = pg_conn.cursor()
        curs_pg.execute('SELECT COUNT(*) FROM content.person;')
        count_pq = curs_pg.fetchall()[0][0]
    assert count_sqlite == count_pq


def test_consistency_genrefilmwork():
    """
    Тест полноты переноса данных таблицы GenreFilmwork из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_sqlite.execute('SELECT COUNT(*) FROM genre_film_work;')
        count_sqlite = curs_sqlite.fetchall()[0][0]
        curs_pg = pg_conn.cursor()
        curs_pg.execute('SELECT COUNT(*) FROM content.genre_film_work;')
        count_pq = curs_pg.fetchall()[0][0]
    assert count_sqlite == count_pq


def test_consistency_personfilmwork():
    """
    Тест полноты переноса данных таблицы PersonFilmwork из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_sqlite.execute('SELECT COUNT(*) FROM person_film_work;')
        count_sqlite = curs_sqlite.fetchall()[0][0]
        curs_pg = pg_conn.cursor()
        curs_pg.execute('SELECT COUNT(*) FROM content.person_film_work;')
        count_pq = curs_pg.fetchall()[0][0]
    assert count_sqlite == count_pq


def test_correct_genre():
    """
    Тест корректности переноса данных таблицы Genre из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_pg = pg_conn.cursor()
        curs_pg.execute('SELECT id, name, description, created, modified FROM content.genre;')
        data_pq = curs_pg.fetchall()
        for pq in data_pq:
            curs_sqlite = sqlite_conn.cursor()
            curs_pg = pg_conn.cursor()
            curs_sqlite.execute(f'SELECT name, description, created_at, updated_at FROM genre WHERE id="{pq[0]}";')
            sq = curs_sqlite.fetchall()
            assert sq[0][0] == pq[1]
            assert sq[0][1] == pq[2]
            created_at = datetime.strptime(sq[0][2], '%Y-%m-%d %H:%M:%S.%f+00')
            assert created_at.strftime('%Y-%m-%d %H:%M:%S.%f+00') == pq[3].strftime('%Y-%m-%d %H:%M:%S.%f+00')
            updated_at = datetime.strptime(sq[0][3], '%Y-%m-%d %H:%M:%S.%f+00')
            assert updated_at.strftime('%Y-%m-%d %H:%M:%S.%f+00') == pq[4].strftime('%Y-%m-%d %H:%M:%S.%f+00')


def test_correct_filmwork():
    """
    Тест корректности переноса данных таблицы Filmwork из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_pg = pg_conn.cursor()
        curs_pg.execute(
            'SELECT id, title, description, creation_date, rating, type, created, modified FROM content.film_work;'
        )
        data_pq = curs_pg.fetchall()
        for pq in data_pq:
            curs_sqlite = sqlite_conn.cursor()
            curs_pg = pg_conn.cursor()
            curs_sqlite.execute(
                f'SELECT title, description, creation_date, rating, type, created_at, updated_at  FROM film_work WHERE id="{pq[0]}";'
            )
            sq = curs_sqlite.fetchall()
            assert sq[0][0] == pq[1]
            assert sq[0][1] == pq[2]
            assert sq[0][2] == pq[3]
            assert sq[0][3] == pq[4]
            assert sq[0][4] == pq[5]
            created_at = datetime.strptime(sq[0][5], '%Y-%m-%d %H:%M:%S.%f+00')
            assert created_at.strftime('%Y-%m-%d %H:%M:%S.%f+00') == pq[6].strftime('%Y-%m-%d %H:%M:%S.%f+00')
            updated_at = datetime.strptime(sq[0][6], '%Y-%m-%d %H:%M:%S.%f+00')
            assert updated_at.strftime('%Y-%m-%d %H:%M:%S.%f+00') == pq[7].strftime('%Y-%m-%d %H:%M:%S.%f+00')


def test_correct_person():
    """
    Тест корректности переноса данных таблицы Person из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_pg = pg_conn.cursor()
        curs_pg.execute('SELECT id, full_name, created, modified FROM content.person;')
        data_pq = curs_pg.fetchall()
        for pq in data_pq:
            curs_sqlite = sqlite_conn.cursor()
            curs_pg = pg_conn.cursor()
            curs_sqlite.execute(f'SELECT full_name, created_at, updated_at FROM person WHERE id="{pq[0]}";')
            sq = curs_sqlite.fetchall()
            assert sq[0][0] == pq[1]
            created_at = datetime.strptime(sq[0][1], '%Y-%m-%d %H:%M:%S.%f+00')
            assert created_at.strftime('%Y-%m-%d %H:%M:%S.%f+00') == pq[2].strftime('%Y-%m-%d %H:%M:%S.%f+00')
            updated_at = datetime.strptime(sq[0][2], '%Y-%m-%d %H:%M:%S.%f+00')
            assert updated_at.strftime('%Y-%m-%d %H:%M:%S.%f+00') == pq[3].strftime('%Y-%m-%d %H:%M:%S.%f+00')


def test_correct_genrefilmwork():
    """
    Тест корректности переноса данных таблицы GenreFilmwork из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_pg = pg_conn.cursor()
        curs_pg.execute('SELECT id, genre_id, film_work_id, created FROM content.genre_film_work;')
        data_pq = curs_pg.fetchall()
        for pq in data_pq:
            curs_sqlite = sqlite_conn.cursor()
            curs_pg = pg_conn.cursor()
            curs_sqlite.execute(f'SELECT genre_id, film_work_id, created_at FROM genre_film_work WHERE id="{pq[0]}";')
            sq = curs_sqlite.fetchall()
            assert sq[0][0] == pq[1]
            assert sq[0][1] == pq[2]
            created_at = datetime.strptime(sq[0][2], '%Y-%m-%d %H:%M:%S.%f+00')
            assert created_at.strftime('%Y-%m-%d %H:%M:%S.%f+00') == pq[3].strftime('%Y-%m-%d %H:%M:%S.%f+00')


def test_correct_personfilmwork():
    """
    Тест корректности переноса данных таблицы PersonFilmwork из SQLite в Postgres
    """
    with conn_context('sqlite_to_postgres/db.sqlite') as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        curs_sqlite = sqlite_conn.cursor()
        curs_pg = pg_conn.cursor()
        curs_pg.execute('SELECT id, person_id, film_work_id, role, created  FROM content.person_film_work;')
        data_pq = curs_pg.fetchall()
        for pq in data_pq:
            curs_sqlite = sqlite_conn.cursor()
            curs_pg = pg_conn.cursor()
            curs_sqlite.execute(
                f'SELECT person_id, film_work_id, role, created_at FROM person_film_work WHERE id="{pq[0]}";'
            )
            sq = curs_sqlite.fetchall()
            assert sq[0][0] == pq[1]
            assert sq[0][1] == pq[2]
            assert sq[0][2] == pq[3]
            created_at = datetime.strptime(sq[0][3], '%Y-%m-%d %H:%M:%S.%f+00')
            assert created_at.strftime('%Y-%m-%d %H:%M:%S.%f+00') == pq[4].strftime('%Y-%m-%d %H:%M:%S.%f+00')
