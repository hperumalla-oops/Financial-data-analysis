import asyncio
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from twisted.internet import asyncioreactor
asyncioreactor.install()

from scrapy import Spider

from google.cloud import pubsub_v1
from config import user_agent, time_zone, empty_ind_dict, pubsub_topics
from datetime import datetime
from collections import defaultdict
import logging
import re
import json
import pickle

# Set logger level
logging.basicConfig(level=logging.ERROR)




class IndicatorCollectorPipeline:
    """Implementation of the Scrapy Pipeline that sends scraped and filtered indicator values
    through Kafka producer.

    Filtering encompasses removing scraped items that already have been sent to Kafka.

    Parameters
    ----------
    server: list
        List of Kafka brokers addresses.
    topic: str
        Specify Kafka topic to which the stream of data records will be published.
    current_dt: datetime.datetime()
        Timestamp of real-time data (EST).

    """
    def __init__(self, topic, current_dt):
        # self.server = server
        self.topic = topic
        self.current_dt = current_dt
        self.items_dict = defaultdict()
        self.prev_items = defaultdict()

        # Read the dictionary of previously sent items
        try:
            with open(r"items.pickle", "rb") as output_file:
                self.prev_items = pickle.load(output_file)
        except (OSError, IOError):
            with open(r"items.pickle", "wb") as output_file:
                pickle.dump(defaultdict(), output_file)

        # Instantiate Kafka producer
        self.publisher = pubsub_v1.PublisherClient()

        self.topic = topic

    def process_item(self, item, spider):
        self.item = item
        print("😍😍😍😍😍😍😍😍PUBLISHING :", self.topic, item)
        self.publisher.publish(self.topic, json.dumps(dict(item)).encode())


        # Create dictionary of current items (keyed by release time and event name)
        self.items_dict.setdefault((item['Schedule_datetime'], item['Event']), item)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(topic=crawler.spider.topic,
            current_dt=crawler.spider.current_dt)

    def close_spider(self, spider):
        # Extract only the new items by performing set difference operation
        new_items = [self.items_dict[k] for k in set(self.items_dict) - set(self.prev_items)]

        # Load economic indicators message template
        items_to_send = empty_ind_dict
        # Set template Timestamp to contain current datetime
        items_to_send["Timestamp"] = datetime.strftime(self.current_dt, "%Y-%m-%d %H:%M:%S")

        if new_items:

            # Remove "Schedule_datetime" and "Event" fields from new items,
            # and then insert them into message template (replace 0 values)
            for item in new_items:

                self.prev_items.setdefault((item['Schedule_datetime'], item['Event']), item)

                del item['Schedule_datetime']
                del item['Event']

                items_to_send.update(item)



        # Save sent items to file
        with open(r"items.pickle", "wb") as output_file:
            pickle.dump(self.prev_items, output_file)

        try:
            future = self.publisher.publish(self.topic, json.dumps(items_to_send).encode())
            future.result()
            print("PUBLISHED INTRADAY:", items_to_send)
        except Exception as e:
            print("PUBLISH FAILED:", e)


class EconomicIndicatorsSpiderSpider(Spider):
    """Implementation of the Scrapy Spider that extracts economic indicators from
    Investing.com Economic Calendar.

    Parameters
    ----------
    countries: list
        List of country names of which indicators will be scraped.
    importance: list
        List of indicator importance levels to use (possible values (1,2,3)).
    event_list: list
        List of economic indicators to scrap.
    current_dt: datetime.datetime()
        Timestamp of real-time data (EST).
    topic: str
        Specify Kafka topic to which the stream of data records will be published.

    Yields
    ------
    dict
        Dictionary that represents scraped item.

    """
    name = 'economic_indicators_spider'
    allowed_domains = ['www.investing.com']
    start_urls = ['https://www.investing.com/economic-calendar/']
    custom_settings = {
        'ITEM_PIPELINES': {
            'economic_indicators_spider.IndicatorCollectorPipeline': 100
        }
    }
    # install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")


    def __init__(self, countries, importance, event_list, current_dt, topic):

        super(EconomicIndicatorsSpiderSpider, self).__init__()

        self.countries = countries
        self.importance = ['bull' + x for x in importance]
        self.event_list = event_list
        self.current_dt = current_dt
        # self.server = server
        self.topic = topic

    def parse(self, response):

        events = response.xpath("//tr[contains(@id, 'eventRowId')]")

        for event in events:

            # Extract event datetime in format: '2019/11/26 16:30:00' (EST)
            datetime_str = event.xpath(".//@data-event-datetime").extract_first()

            if not datetime_str:
                continue

            event_datetime = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S")
            event_datetime = event_datetime.replace(tzinfo=time_zone['EST'])

            current_dt_str = datetime.strftime(self.current_dt, "%Y-%m-%d %H:%M:%S")

            # Return only events that passed
            if not self.current_dt >= event_datetime:
                continue

            country = event.xpath(".//td/span/@title").extract_first()

            importance_label = event.xpath(".//td[@class='left textNum sentiment noWrap']/@data-img_key")\
                .extract_first()

            if country not in self.countries or importance_label not in self.importance:
                continue

            if not importance_label:
                logging.warning("Empty importance label for: {} {}".format(country, datetime_str))
                continue

            event_name = event.xpath(".//td[@class='left event']/a/text()").extract_first()
            event_name = event_name.strip(' \r\n\t ')
            event_name_regex = re.findall(r"(.*?)(?=.\([a-zA-Z]{3}\))", event_name)

            if event_name_regex:
                event_name = event_name_regex[0].strip()

            if event_name not in self.event_list:
                continue

            actual = event.xpath(".//td[contains(@id, 'eventActual')]/text()").extract_first().strip('%M BK')

            previous = event.xpath(".//td[contains(@id, 'eventPrevious')]/span/text()").extract_first().strip('%M BK')

            forecast = event.xpath(".//td[contains(@id, 'eventForecast')]/text()").extract_first().strip('%M BK')

            if actual == '\xa0':
                continue

            previous_actual_diff = float(previous) - float(actual)

            if forecast != '\xa0':
                forecast_actual_diff = float(forecast) - float(actual)

            yield {'Timestamp': current_dt_str,
                'Schedule_datetime': datetime_str,
                'Event': event_name.replace(" ", "_"),
                '{}'.format(event_name.replace(" ", "_")): {
                    'Actual': float(actual),
                    'Prev_actual_diff': previous_actual_diff,
                    'Forc_actual_diff': forecast_actual_diff if forecast != '\xa0' else None
                    }
                }


import subprocess
import sys

def run_indicator_spider(countries, importance, event_list, current_dt, topic):
    print("💕💕💕💕💕💕 topic:🤩🤩🤩🤩🤩🤩🤩🤩😶‍🌫️", topic)
    subprocess.run([
        sys.executable, "run_spider.py",
        "--topic", topic,
        "--current_dt", current_dt.strftime("%Y-%m-%d %H:%M:%S")
    ])



