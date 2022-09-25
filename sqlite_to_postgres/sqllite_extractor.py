from dc import Filmwork, Genre, Person, GenreFilmWork, PersonFilmWork
import itertools


class SQLiteExtractor:
    def __init__(self, connection) -> None:
        self.curs = connection.cursor()
        self.dc_map = {
            'film_work': Filmwork,
            'genre': Genre,
            'person': Person,
            'genre_film_work': GenreFilmWork,
            'person_film_work': PersonFilmWork,
        }
        self.page_size = 1000

    def get_rows(self) -> list:
        columns = [data[0] for data in self.curs.description]

        modif_columns = {'created_at': 'created', 'updated_at': 'modified'}
        for col in modif_columns.keys():
            if col in columns:
                columns[columns.index(col)] = modif_columns[col]

        rows = []
        while True:
            fetch_rows = self.curs.fetchmany(self.page_size)
            if fetch_rows:
                rows.extend(fetch_rows)
            else:
                break

        return [dict(zip(columns, row)) for row in rows]

    def get_table_data(self, dc: type, table_name) -> list:
        sql = f'SELECT * FROM {table_name};'
        self.curs.execute(sql)

        return [dc(**row) for row in self.get_rows()]

    def extract(self) -> dict:
        data = {}
        for table_name, dc in self.dc_map.items():
            data.update({table_name: self.get_table_data(dc, table_name)})

        return data
