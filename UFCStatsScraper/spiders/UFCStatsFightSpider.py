import logging
import scrapy

from UFCStatsScraper.items import UFCStatsFightItemLoader, UFCStatsFighterItemLoader


logger = logging.getLogger(__name__)

class UFCStatsFightScraper(scrapy.Spider):
    name = "ufcstats_fights"

    def start_requests(self):
        yield scrapy.Request(url="http://www.ufcstats.com/statistics/events/completed?page=all", callback=self.parse_event_links)

    def parse_event_links(self, response):
        yield from response.follow_all(xpath="//tr[@class='b-statistics__table-row'][td/i]//a/@href", callback=self.parse_event_pages)


    def parse_event_pages(self, response):
        event_info = {}
        for event_field, xpath in UFCStatsFightItemLoader.get_event_field_paths().items():
            event_info[event_field] = response.xpath(xpath).get()

        fight_linkextractor = scrapy.linkextractors.LinkExtractor(allow=r"/fight-details/.*")

        yield from response.follow_all(fight_linkextractor.extract_links(response), self.parse_fight_pages, cb_kwargs=event_info)

    def parse_fight_pages(self, response, **event_info):
        yield UFCStatsFightItemLoader(response, event_info_dict=event_info).load_item()
        yield from response.follow_all(xpath="//a[@class='b-link b-fight-details__person-link']", callback=self.parse_fighter_pages)

    def parse_fighter_pages(self, response):
        yield UFCStatsFighterItemLoader(response).load_item()
