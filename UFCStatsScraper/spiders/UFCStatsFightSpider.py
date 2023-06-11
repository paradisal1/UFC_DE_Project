import logging
import scrapy

from UFCStatsScraper.items import UFCStatsFightItemLoader, UFCStatsFighterItemLoader


logger = logging.getLogger(__name__)

class UFCStatsFightScraper(scrapy.Spider):

    name = "ufcstats"

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
        '''
        This function is called for each fight page. It yields a UFCStatsFightItemLoader object, which is then used to load a UFCStatsFightItem object.
        It then yields a request for each fighter page.
        @url http://www.ufcstats.com/fight-details/0a0c6e8b0c0c9b1a
        @cb_kwargs {"eventname": "UFC 65: Bad Intentions", "eventdate": "November 18, 2006", "eventlocation": "Sacramento, California, USA"}
        @returns items 1
        @returns requests 2
        @scrapes eventname eventdate eventlocation id weightclass titlefight method round time timeformat referee fighter1 fighter1nickname fighter2 fighter2nickname fighter1kd fighter2kd fighter1sigstr fighter2sigstr fighter1sigstrpct fighter2sigstrpct fighter1totalstr fighter2totalstr fighter1td fighter2td fighter1tdpct fighter2tdpct fighter1subatt fighter2subatt fighter1rev fighter2rev fighter1ctrl fighter2ctrl fighter1head fighter2head fighter1body fighter2body fighter1leg fighter2leg fighter1distance fighter2distance fighter1clinch fighter2clinch fighter1ground fighter2ground fighter1id fighter2id winningfighter perf sub fight ko Judge1 Judge1_Score Judge2 Judge2_Score Judge3 Judge3_Score
        '''
        yield UFCStatsFightItemLoader(response, event_info_dict=event_info).load_item()
        yield from response.follow_all(xpath="//a[@class='b-link b-fight-details__person-link']", callback=self.parse_fighter_pages)

    def parse_fighter_pages(self, response):
        '''
        @url http://www.ufcstats.com/fighter-details/6506c1d34da9c013
        @returns items 1
        @returns requests 0
        @scrapes fighterurl fightername height weight reach stance dob slpm stracc sapm strdef tdavg tdacc tddef subavg
        '''
        yield UFCStatsFighterItemLoader(response).load_item()




if __name__ == "__main__":
    pass