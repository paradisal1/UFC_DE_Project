import duckdb
import logging
import json
from pathlib import Path


def get_field_list_from_json(item_name):
    with open(f"UFCStatsScraper/itemfieldlists/{item_name.lower().replace('item','')}_fields.json", 'r') as f:
        fieldlist = []
        fielddict = json.load(f)
        for cat in fielddict.keys():
            for key in fielddict[cat].keys():
                fieldlist.append(key)
        return fieldlist


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

def remove_single_quotes(text):
    """Remove single quotes from a string"""
    import logging

    try:
        text = text.replace("'", "")
    except Exception as e:
        logging.debug(e)
    else:
        return text.replace("'", "")
    return [item.replace("'", "") for item in text]

def remove_double_quotes(text):
    """Remove double quotes from a string"""
    import logging

    try:
        text = text.replace('"', '')
    except Exception as e:
        logging.debug(e)
    else:
        return text.replace('"', '')
    return [item.replace('"', '') for item in text]

def is_datetime(text):
    """Check if a string is a datetime object"""
    from datetime import datetime as dt

    return isinstance(text, dt)


def remove_whitespace(text):
    """Remove whitespace from a string"""
    import logging

    try:
        text = text.strip()
    except Exception as e:
        logging.debug(e)

    else:
        return text.strip()
    return [item.strip() for item in text]


def remove_non_numeric(text):
    """Remove non-numeric characters from a string"""
    import logging

    try:
        text.isalnum()
    except Exception as e:
        logging.debug(e)
    else:
        return "".join([char for char in text if char.isnumeric()])
    return ["".join([char for char in item if char.isnumeric()]) for item in text]


def remove_non_alphanumeric(text):
    """Remove non-alphanumeric characters from a string"""
    import logging

    try:
        text.isalnum()
    except Exception as e:
        logging.debug(e)
    else:
        return "".join([char for char in text if char.isalnum()])
    return ["".join([char for char in item if char.isalnum()]) for item in text]


def remove_newlines(text):
    """Remove newlines from a string"""
    import logging

    try:
        text.isalnum()
    except Exception as e:
        logging.debug(e)
    else:
        return text.replace("\n", "")
    return [item.replace("\n", "") for item in text]


def format_date(text):
    """Format a date string"""
    from datetime import datetime as dt
    import logging

    try:
        text.isalnum()
    except Exception as e:
        logging.debug(e)
    else:
        if is_datetime(text):
            return text
        elif text == "---":
            return "---"
        else:
            try:
                return dt.strptime(text, "%b %d, %Y")
            except Exception as e:
                logging.debug(e)
    try:
        if is_datetime(text):
            return text
        elif text == "---":
            return ["---" for _ in text]
        else:
            return [dt.strptime(item, "%b %d, %Y") for item in text]
    except Exception as e:
        logging.debug(e)


def replace_empty_string(text):
    """Replace empty strings with ---"""
    import logging

    try:
        text.isalnum()
    except Exception as e:
        logging.debug(e)
    else:
        return "---" if text == "" else text
    return ["---" if item == "" else item for item in text]



if __name__ == "__main__":
    db = DuckDBInterface('ufcstats.db')
    db.execute('select * from ufcstatsfighttable')