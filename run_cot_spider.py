import argparse, datetime
from scrapy.crawler import CrawlerProcess
from cot_reports_spider import COTreportsSpiderSpider, user_agent

parser = argparse.ArgumentParser()
parser.add_argument("--topic")
parser.add_argument("--report_subject")
parser.add_argument("--current_dt")
args = parser.parse_args()

current_dt = datetime.datetime.strptime(args.current_dt, "%Y-%m-%d %H:%M:%S")

process = CrawlerProcess(settings={
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "USER_AGENT": user_agent
})
process.crawl(COTreportsSpiderSpider, report_subject=args.report_subject, current_dt=current_dt, topic=args.topic)
process.start()