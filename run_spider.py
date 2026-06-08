import argparse
from scrapy.crawler import CrawlerProcess
from economic_indicators_spider import EconomicIndicatorsSpiderSpider, user_agent
from config import event_list

economic_data = {'countries': ['United States'], 'importance': ['1', '2', '3'], 'event_list': event_list, 'cot': 'S&P 500 STOCK INDEX'}

parser = argparse.ArgumentParser()
parser.add_argument("--topic")
parser.add_argument("--current_dt")
args = parser.parse_args()

import datetime
current_dt = datetime.datetime.strptime(args.current_dt, "%Y-%m-%d %H:%M:%S")

process = CrawlerProcess(settings={
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "USER_AGENT": user_agent
})
process.crawl(EconomicIndicatorsSpiderSpider, 
    countries=economic_data['countries'],
    importance=economic_data['importance'],
    event_list=economic_data['event_list'],
    current_dt=current_dt,
    topic=args.topic)
process.start()