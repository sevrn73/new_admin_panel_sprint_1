import psycopg2
from psycopg2.extras import execute_batch
from dataclasses import asdict


class PostgresSaver:
    def __init__(self, pg_conn) -> None:
        self.pg_conn = pg_conn
        self.cur = self.pg_conn.cursor()
        self.page_size = 1000

    def save_data(self, data) -> None:
        try:
            for table_name, data_rows in data.items():
                self.inset_data(table_name, data_rows)
            self.cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def inset_data(self, table_name, data_rows):
        columns = ''.join([f'{_}, ' for _ in asdict(data_rows[0]).keys()])[0:-2]
        values = ''.join(tuple(['%s, ' for v in asdict(data_rows[0]).values()]))[0:-2]
        sql = f'INSERT INTO content.{table_name} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING;'
        execute_batch(self.cur, sql, [tuple(asdict(_).values()) for _ in data_rows], page_size=self.page_size)
        self.pg_conn.commit()
