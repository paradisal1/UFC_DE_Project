import logging
import duckdb
from pathlib import Path

from data.databaseutils.datafunctions import get_field_list_from_json


class DuckDBInterface:
    logger = logging.getLogger(__name__)

    def __init__(self, db_name):
        self.db_name = f'{db_name if db_name.endswith(".db") else db_name + ".db"}'
        data_folder = Path('data/').absolute().resolve()
        data_folder.mkdir(parents=True, exist_ok=True)

        self.conn = duckdb.connect(f'{data_folder}/{self.db_name}')
        self.conn.execute('CREATE SCHEMA IF NOT EXISTS raw')

    def execute(self, query):
        self.conn.execute(query)
        return self.conn.fetchall()

    def __del__(self):
        self.conn.close()

    def __call__(self, query):
        return self.execute(query)

    def fieldlist_to_table(self, item_name):
        fieldlist = get_field_list_from_json(item_name)
        table_name = item_name.lower().replace('item', 'table')

        sql_string = f'CREATE OR REPLACE TABLE raw.{table_name} ('
        for field in fieldlist:
            sql_string += f'{field.lower()} TEXT, '
        sql_string = sql_string[:-2] + ')'
        self.conn.execute(sql_string)

    def insert_item(self, item):
        table_name = item.__class__.__name__.lower().replace('item', 'table')

        sql_string = f'INSERT INTO raw.{table_name} VALUES ('
        for field in item.values():
            sql_string += f"""'{field}'""" + ', '
        sql_string = sql_string[:-2] + ')'

        self.conn.execute(sql_string)