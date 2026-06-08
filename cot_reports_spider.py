
import asyncio
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# from twisted.internet import asyncioreactor
# asyncioreactor.install()

from scrapy import Spider, Request

from google.cloud import pubsub_v1
# from twisted.internet import reactor
from config import user_agent, pubsub_topics
from datetime import datetime
import logging
import json

# Set logger level
logging.basicConfig(level=logging.ERROR)


class COTCollectorPipeline:
    """Implementation of the Scrapy Pipeline that sends scraped COT data
    through Kafka producer.

    Parameters
    ----------
    server: list
        List of Kafka brokers addresses.
    topic: str
        Specify Kafka topic to which the stream of data records will be published.

    """
    def __init__(self,  topic):
        # self.server = server
        self.topic = topic
        self.items = {}
        self.publisher = pubsub_v1.PublisherClient()
        self.topic = topic

    def process_item(self, item, spider):
        self.items.update(item)
        print(" PUBLISHING COT TO TOPIC:", self.topic)
        print("😍😍😍😍😍😍😍😍PUBLISHING COT:", item)
        try:
            future = self.publisher.publish(self.topic, json.dumps(dict(item)).encode())
            result = future.result()
            print("PUBLISH SUCCESS, message id:", result)
        except Exception as e:
            print("PUBLISH FAILED:", e)
        return item

    @classmethod
    def from_crawler(cls, crawler):
        return cls(topic=crawler.spider.topic)

    def close_spider(self, spider):
        # self.publisher.publish(self.topic, data=json.dumps(self.item).encode('utf-8'))
        # self.publisher.publish(pubsub_topics['cot'], json.dumps(self.item).encode())
        pass




def to_number(v):
    """Casts string to int or float

    """
    try:
        if v.isdigit():
            return int(v)
        else:
            return float(v)
    except ValueError:
            return v


class COTreportsSpiderSpider(Spider):
    """Implementation of the Scrapy Spider that extracts COT data from
    tradingster.com

    Parameters
    ----------
    report_subject: str
        Specify COT report subject (for example: 'S&P 500 STOCK INDEX' or 'BRITISH POUND STERLING')
    current_dt: datetime.datetime()
        Timestamp of real-time data (EST).
    server: list
        List of Kafka brokers addresses.
    topic: str
        Specify Kafka topic to which the stream of data records will be published.

    Yields
    ------
    dict
        Dictionary that represents scraped item.

    """
    name = 'cot_reports_spider'
    allowed_domains = ['www.tradingster.com']
    start_urls = ['https://www.tradingster.com/cot']
    custom_settings = {
        'ITEM_PIPELINES': {
            'cot_reports_spider.COTCollectorPipeline': 100
        }
    }

    def __init__(self, report_subject, current_dt, topic):

        super(COTreportsSpiderSpider, self).__init__()

        self.report_subject = report_subject
        self.current_dt = datetime.strftime(current_dt, "%Y-%m-%d %H:%M:%S")
        # self.server = server
        self.topic = topic

    def parse(self, response):
        """Yields the request to URL that contains report_subject's COT report.

        """
        tables = response.xpath(".//table")

        for table in tables:

            rows = table.xpath(".//tr")

            for row in rows:
                name = row.xpath(".//td[1]/text()").extract_first().strip()

                if self.report_subject != name:
                    continue

                report_url = row.xpath(".//td[3]/a/@href").extract_first()

                report_url = response.urljoin(report_url)

                yield Request(url=report_url, callback=self.parse_report, dont_filter=True)

    def parse_report(self, response):
        """Scraps items from report_subject's COT report.

        """
        rows = response.xpath("//table/tbody/tr")

        for row in rows:
            name = row.xpath(".//strong/text()").extract_first().strip(' /')

            if not(('Asset Manager' in name) or ('Leveraged' in name) or ('Managed Money' in name)):
                continue

            name = name.split()[0]

            long_positions = row.xpath(".//td[2]/text()").extract_first().strip().replace(",", "")
            long_positions_change = row.xpath(".//td[2]/span/text()").extract_first().replace(",", "")
            long_open_int = row.xpath(".//td[3]/text()").extract_first().strip(' %').replace(",", "")

            short_positions = row.xpath(".//td[5]/text()").extract_first().strip().replace(",", "")
            short_positions_change = row.xpath(".//td[5]/span/text()").extract_first().replace(",", "")
            short_open_int = row.xpath(".//td[6]/text()").extract_first().strip(' %').replace(",", "")

            yield {'Timestamp':  self.current_dt,
                '{}'.format(name): {
                    '{}_long_pos'.format(name): to_number(long_positions),
                    '{}_long_pos_change'.format(name): to_number(long_positions_change),
                    '{}_long_open_int'.format(name): to_number(long_open_int),
                    '{}_short_pos'.format(name): to_number(short_positions),
                    '{}_short_pos_change'.format(name): to_number(short_positions_change),
                    '{}_short_open_int'.format(name): to_number(short_open_int)
                    }
                }



import subprocess
import sys

def run_cot_spider(report_subject, current_dt, topic):
    subprocess.run([
        sys.executable, "run_cot_spider.py",
        "--topic", topic,
        "--report_subject", report_subject,
        "--current_dt", current_dt.strftime("%Y-%m-%d %H:%M:%S")
    ])


# def run_cot_spider(report_subject, current_dt, topic):

#     crawler = CrawlerScript(report_subject, current_dt,  topic)

#     # the script will block here until the crawling is finished
#     crawler.start()
#     crawler.join()

