import logging
from scrapy import loader, Item, Field
from itemloaders.processors import MapCompose, TakeFirst
import json

from UFCStatsScraper.utils import remove_whitespace, remove_newlines, replace_empty_string

logger = logging.getLogger(__name__)


class BaseItem(Item):
    default_input_processor = MapCompose(
        remove_whitespace,
        remove_newlines,
        replace_empty_string,
    )
    default_output_processor = TakeFirst()

    def _get_fields_path(self):
        return f"UFCStatsScraper/itemfieldlists/{self.__class__.__name__.lower().replace('item','')}_fields.json"

    def get_fields(self):
        with open(self._get_fields_path(), 'r') as f:
            return json.load(f)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for cat in self.get_fields().keys():
            for key in self.get_fields()[cat].keys():
                self.fields[key] = Field()


class BaseItemLoader(loader.ItemLoader):
    failed_fields = []

    def __init__(self, response, item=None, **kwargs):
        super().__init__(response=response, item=item)
        self.xpath_field_dict = self.get_fields()

        self.response = response

        self.failed_fields = []
        self.loaded_separately = []

    def _get_fields_path(self):
        return f"UFCStatsScraper/itemfieldlists/{self.__class__.__name__.lower().replace('itemloader','')}_fields.json"

    def get_fields(self):
        with open(self._get_fields_path(), 'r') as f:
            return json.load(f)

    def add_all_fields(self):
        for cat in self.xpath_field_dict.keys():
            for key, value in self.xpath_field_dict[cat].items():
                if not value or value == '':
                    self.loaded_separately.append(key)
                else:
                    self._add_field(key, self.get_xpath_value(self.response, value))


    def _add_field(self, key, value):
        try:
            if value:
                locals()[key] = self.add_value(key, value)
            else:
                locals()[key] = self.add_value(key, '---')
                self.failed_fields.append(key)
        except Exception as e:
            self.failed_fields.append(key)
            locals()[key] = self.add_value(key, '---')

    def get_xpath_value(self, response, value):
        try:
            return response.xpath(value).get()
        except Exception as e:
            logger.error(f"Error in get_xpath_value: {e}")
            return None


class UFCStatsFightItem(BaseItem):
    default_input_processor = MapCompose(
        remove_whitespace,
        remove_newlines,
        replace_empty_string,
    )
    default_output_processor = TakeFirst()

    def __init__(self):
        super().__init__()

class UFCStatsFightItemLoader(BaseItemLoader):


    def __init__(self, response, item=UFCStatsFightItem, event_info_dict=None):
        item = item()
        super().__init__(response=response, item=item)
        self.failed_fields = []

        if event_info_dict:
            self.add_event_fields(response, event_info_dict)
        self.add_other_fields(response)
        logger.warning(f"Failed fields: {self.failed_fields} for {response.url}")

    def add_event_fields(self, response, event_info_dict):
        for key, value in event_info_dict.items():
            self._add_field(key, value)

    def add_other_fields(self, response):
        cats = {key: value for key, value in self.xpath_field_dict.items() if key != "eventxpaths"}
        for cat in cats:
            for key, value in self.xpath_field_dict[cat].items():
                self._add_field(key, self.get_xpath_value(response, value))

    @staticmethod
    def get_event_field_paths():
        with open("UFCStatsScraper/itemfieldlists/ufcstatsfight_fields.json", 'r') as f:
            return json.load(f)['eventxpaths']



class UFCStatsFighterItem(BaseItem):
    default_input_processor = MapCompose(
        remove_whitespace,
        remove_newlines,
        replace_empty_string,
    )
    default_output_processor = TakeFirst()

    def __init__(self):
        super().__init__()





class UFCStatsFighterItemLoader(BaseItemLoader):
    def __init__(self, response, item=UFCStatsFighterItem):
        item = item()
        super().__init__(response=response, item=item)

        self.failed_fields = []
        self.loaded_separately = []

        self.add_all_fields()
        self.load_separate_fields()

        logger.warning(f"Failed fields: {self.failed_fields} for {response.url}")

    def load_separate_fields(self):
        self._add_field('fighterurl', self.response.url)


if __name__ == "__main__":
    UFCStatsFighterItem().fields
