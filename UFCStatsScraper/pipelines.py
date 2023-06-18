# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import logging

from data.databaseutils.DuckDBInterface import DuckDBInterface

class DuckDBPipeline:

    table_item_dict = {
        'ufcstats': ['ufcstatsfighteritem', 'ufcstatsfightitem']
    }

    logger = logging.getLogger(__name__)

    def open_spider(self, spider):
        self.db = DuckDBInterface(spider.name)
        self.db.execute('CREATE SCHEMA IF NOT EXISTS raw')

        for item in self.table_item_dict[spider.name]:
            self.db.fieldlist_to_table(item)

    def process_item(self, item, spider):

        self.db.insert_item(item)
        pass


    def close_spider(self, spider):
        self.db.connection.close()




class UfcstatsscraperPipeline:
    def process_item(self, item, spider):
        return item



if __name__ == '__main__':
    import duckdb

    db = duckdb.connect('data/ufcstats.db')
    cursor = db.cursor()
    cursor.execute('SELECT * FROM ufcstatsfighttable').fetchall()