import logging
import duckdb

from data.utils.datafunctions import get_field_list_from_json


class DuckDBInterface:
    logger = logging.getLogger(__name__)

    def __init__(self, db_name):
        self.db_name = f'data/{db_name if db_name.endswith(".db") else db_name + ".db"}'
        self.connection = duckdb.connect(self.db_name)
        self.cursor = self.connection.cursor()

    def execute(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def __del__(self):
        self.connection.close()

    def __call__(self, query):
        return self.execute(query)

    def fieldlist_to_table(self, item_name):
        fieldlist = get_field_list_from_json(item_name)
        table_name = item_name.lower().replace('item', 'table')

        sql_string = f'CREATE OR REPLACE TABLE {table_name} ('
        for field in fieldlist:
            sql_string += f'{field.lower()} TEXT, '
        sql_string = sql_string[:-2] + ')'
        self.cursor.execute(sql_string)

    def insert_item(self, item):
        table_name = item.__class__.__name__.lower().replace('item', 'table')

        sql_string = f'INSERT INTO {table_name} VALUES ('
        for field in item.values():
            sql_string += f"""'{field}'""" + ', '
        sql_string = sql_string[:-2] + ')'

        self.cursor.execute(sql_string)