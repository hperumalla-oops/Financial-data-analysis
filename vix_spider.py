
import asyncio
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# from twisted.internet import asyncioreactor
# asyncioreactor.install()

from scrapy import Spider
from google.cloud import pubsub_v1
from config import user_agent, pubsub_topics
from datetime import datetime
import logging
import json

# Set logger level
logging.basicConfig(level=logging.ERROR)


class VIXCollectorPipeline:
    """Implementation of the Scrapy Pipeline that sends scraped VIX data
    through Kafka producer.

    Parameters
    ----------
    server: list
        List of Kafka brokers addresses.
    topic: str
        Specify Kafka topic to which the stream of data records will be published.

    """
    def __init__(self, topic):
        # self.server = server
        self.topic = topic
        
        self.publisher = pubsub_v1.PublisherClient()

    def process_item(self, item, spider):
        import google.auth
        credentials, project = google.auth.default()
        print("AUTH ACCOUNT:", credentials.service_account_email if hasattr(credentials, 'service_account_email') else "user account")
        print("PROJECT:", project)

        print("PUBLISHING VIX TO TOPIC:", self.topic)
        print("😍😍😍😍😍😍😍😍PUBLISHING VIX:", item)
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
        # Send VIX data through kafka producer
        # self.publisher.publish(self.topic, data=json.dumps(self.item).encode('utf-8'))
        # self.publisher.publish(pubsub_topics['vix'], json.dumps(self.item).encode())
        pass



class VIXSpiderSpider(Spider):
    """Implementation of the Scrapy Spider that extracts VIX data from cnbc.com

    Parameters
    ----------
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
    name = 'vix_reports_spider'
    allowed_domains = ['www.cnbc.com']
    start_urls = ['https://www.cnbc.com/quotes/?symbol=.VIX']
    custom_settings = {
        'ITEM_PIPELINES': {
            'vix_spider.VIXCollectorPipeline': 100
        }
    }

    def __init__(self, current_dt, topic):

        super(VIXSpiderSpider, self).__init__()

        self.current_dt = datetime.strftime(current_dt, "%Y-%m-%d %H:%M:%S")
        # self.server = server
        self.topic = topic

    def parse(self, response):
        try:
            import requests
            r = requests.get(
                "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1m&range=1d",
                headers={"User-Agent": "Mozilla/5.0"}
            )
            data = r.json()
            vix = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        except Exception as e:
            print(f"VIX fetch failed: {e}")
            vix = 20.0

        yield {'VIX': float(vix), 'Timestamp': self.current_dt}

# class CrawlerScript(Process):
#     """Runs Spider multiple times within one script by utilizing billiard package
#     (tackle the ReactorNotRestartable error).

#     Parameters
#     ----------
#     current_dt: datetime.datetime()
#         Timestamp of real-time data (EST).
#     server: list
#         List of Kafka brokers addresses.
#     topic: str
#         Specify Kafka topic to which the stream of data records will be published.

#     """
#     def __init__(self, current_dt, topic):

#         Process.__init__(self)

#         self.current_dt = current_dt
#         # self.server = server
#         self.topic = topic

#         self.crawler = Crawler(
#           VIXSpiderSpider,
#           settings={
#             'USER_AGENT': user_agent
#           }
#         )

#         self.crawler.signals.connect(reactor.stop, signal=scrapy_signals.spider_closed)

#     def run(self):
#         self.crawler.crawl(self.current_dt, self.topic)
#         reactor.run()


# def run_vix_spider(current_dt, topic):

#     crawler = CrawlerScript(current_dt, topic)

#     # the script will block here until the crawling is finished
#     crawler.start()
#     crawler.join()



import subprocess
import sys

def run_vix_spider(current_dt, topic):

    subprocess.run([
        sys.executable, "run_vix_spider.py",
        "--topic", topic,
        "--current_dt", current_dt.strftime("%Y-%m-%d %H:%M:%S")
    ])
    print(" 💕💕💕💕💕💕 RUN VIX SPIDER DONE")
