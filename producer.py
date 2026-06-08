import datetime
import time
import pytz
import logging
import json
import pickle
# import time
import yfinance as yf

from flask import Flask, Response
from google.cloud import pubsub_v1
from collections import defaultdict
from config import tokens, time_zone, pubsub_topics, event_list
from config import get_cot, get_vix, get_stock_volume
from getMarketData import GetData, get_market_calendar

from economic_indicators_spider import run_indicator_spider
from cot_reports_spider import run_cot_spider
from vix_spider import run_vix_spider
import requests


app = Flask(__name__)
publisher = pubsub_v1.PublisherClient()



def get_data_point(source, tokens, timestamp, request=None, function=None, symbol=None, interval=None,
                   output_format='json'):
    get = GetData(tokens, output_format)
    raw_data = None
    try:
        if source == 'IEX':
            raw_data = get.get_iex_data(request, timestamp)
        elif source == 'AV':
            raw_data = get.get_av_data(timestamp, function, symbol, interval, request)
        else:
            logging.warning('Source not recognized')
    except Exception as e:
        logging.error('API call failed: {}'.format(str(e)))
    return raw_data


def last_day_of_month(date):
    """Returns the last day of the month of passed datetime object.

    """
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)


import datetime
from zoneinfo import ZoneInfo

def market_hour_to_dt(current_datetime, hour_str):
    dt = datetime.datetime.strptime(hour_str, '%H:%M')
    mh, mm = dt.hour, dt.minute

    # Ensure current_datetime is timezone-aware
    if current_datetime.tzinfo is None:
        current_datetime = current_datetime.replace(tzinfo=ZoneInfo("America/New_York"))

    dt = current_datetime.replace(hour=mh, minute=mm, second=0, microsecond=0)
    return dt


def intraday_data(freq, market_hours, current_datetime, source, tokens, economic_data, cot=False, vix=False, request=None,\
                  function=None, symbol=None, interval=None, output_format='json', get_stock_volume=None):
    """Gets the intraday market data from Alpha Vantage, IEX APIs, economic indicators, COT data and VIX.
     Function will call the source API with the frequency of 'freq' until market is closed.

    Parameters
    ----------
    freq: int
        Frequency of data transmissions in seconds.
    market_hours: dict
        Dictionary of today's market start and end times.
    current_datetime: datetime.datetime()
        Initial current time (start of the session time).
    source: str
        Specify the source of data. Available values: 'AV' or 'IEX'.
    token: dict
        Dictionary of API (keys) tokens. Dictionary keys: 'av_token' and 'iex_token'.
    economic_data: dict
        Dict of economic input data required to parse indicators, such as countries of interest ['counties'],
        importance of data ['importance'], event types to get ['event_list'], cot report subject ['cot'].
    cot: boolean, optional (default=False)
        Whether to get COT data.
    vix: boolean, optional (default=False)
        Whether to get CBOE VIX .
    request: str, optional (default=None)
        If source == 'AV':
            Specify request to call API with more complex queries, instead of using: function, symbol, interval.
            For example, call TECHNICAL INDICATORS API:
            'function=RSI&symbol=MSFT&interval=weekly&time_period=10&series_type=open'
        if source == 'IEX':
            Specify the API HTTP request, for instance '/deep/book?symbols={symbol}{?or&}',
            '/data-points/market/{symbol}{?or&}'. Check in documentation whether token should be added using & or ?.
            Available HTTP requests: https://iexcloud.io/docs/api/
            The full list of symbols can be found here: https://iextrading.com/trading/eligible-symbols/
    function: str, optional (default=None)
        Use only with source == 'AV'. Specify the API function to use, for instance: TIME_SERIES_INTRADAY,
        FX_INTRADAY, STOCH.
    symbol: str, optional (default=None)
        Use only with source == 'AV'. The name of the equity. For example: MSFT, SPY. In case of using FOREX API
        pass a string that specifies pair of currencies, for example: EURUSD (length of 6 chars).
    interval: str, optional (default=None)
        Use only with source == 'AV'. Time interval between two consecutive data points in the time series.
        Specify only for INTRADAY data and TECHNICAL INDICATORS: 1min, 5min, 15min, 30min, 60min.
    output_format: str, optional (default='json')
        Specify the output format. Available formats: 'json' or 'csv'.
    get_stock_volume: str, optional (default=None)
        Pass symbol to return it's volume from AV.

    """

    # publisher is already initialized at module level


    # Create economic indicators registry at the start of each session
    with open(r"items.pickle", "wb") as output_file:
        pickle.dump(defaultdict(), output_file)
    
    print("one")

    while (current_datetime >= market_hours['market_start']) and (current_datetime <= market_hours['market_end']):
        print("tooooo")

        try:
            print("tooooo")
            

            process_start_time = time.time()

            # Get market data
            market_data = get_data_point(source, tokens, current_datetime, request=request, function=function, symbol=symbol,\
                                      interval=interval, output_format=output_format)

            # Fetch the Stock Volume from AV (which is not included in IEX)
            if get_stock_volume and (source != 'AV' and function != 'TIME_SERIES_INTRADAY'):
                time.sleep(10)
                # hist = yf.download(get_stock_volume, period="1d", interval="1m", progress=False)
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{get_stock_volume}?interval=1m&range=1d"
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                r = requests.get(url, headers=headers, timeout=10)
                data = r.json()
                vol = data["chart"]["result"][0]["indicators"]["quote"][0]["volume"][-1] or 0
                volume = {"Volume": int(vol), "Timestamp": current_datetime.strftime("%Y-%m-%d %H:%M:%S")}


            publisher.publish(pubsub_topics['volume'], json.dumps(volume).encode())
            print("publish two!!!!!!")
            print("before indicator spider")

            run_indicator_spider(economic_data['countries'], economic_data['importance'], economic_data['event_list'],\
                                 current_datetime,pubsub_topics['intraday'])
            
            print("crossed this")

            if cot:
                print("cotttt")
                run_cot_spider(economic_data['cot'], current_datetime,pubsub_topics['cot'])

            if vix:
                print("vizzzxxx")
                run_vix_spider(current_datetime, pubsub_topics['vix'])

            process_end_time = time.time()
            process_time = process_end_time - process_start_time

            time.sleep(freq - process_time)

            # Update current time
            current_datetime = pytz.utc.localize(datetime.datetime.utcnow()).astimezone(time_zone['EST'])

        except KeyboardInterrupt:
            logging.warning('Stopped by the user.')
            break

    else:
        print("maket_closed")
        print('Market is closed.')
        print('Current time: {} {}'.format(datetime.datetime.strftime(current_datetime, "%Y-%m-%d %I:%M %p"),\
            current_datetime.tzname()))
        print('Market trade hours: from {} to {} {}'.format(datetime.datetime.strftime(market_hours['market_start'],\
            "%Y-%m-%d %I:%M %p"), datetime.datetime.strftime(market_hours['market_end'], "%Y-%m-%d %I:%M %p"),\
            market_hours['market_end'].tzname()))


def start_day_session(freq, source, tokens, economic_data, cot=False, vix=False, request=None, function=None, symbol=None,\
                      interval=None, output_format='json', get_stock_volume=None):
    """Get the single day market session data from Alpha Vantage or IEX APIs.

    Parameters
    ----------
    freq: int
        Frequency of data transmissions in seconds.
    source: str
        Specify the source of data. Available values: 'AV' or 'IEX'.
    token: dict
        Dictionary of API (keys) tokens. Dictionary keys: 'av_token' and 'iex_token'.
    economic_data: dict
        Dict of economic input data required to parse indicators, such as countries of interest ['counties'],
        importance of data ['importance'], event types to get ['event_list'], cot report subject ['cot'].
    cot: boolean, optional (default=False)
        Whether to get COT data.
    vix: boolean, optional (default=False)
        Whether to get CBOE VIX .
    request: str, optional (default=None)
        If source == 'AV':
            Specify request to call API with more complex queries, instead of using: function, symbol, interval.
            For example, call TECHNICAL INDICATORS API:
            'function=RSI&symbol=MSFT&interval=weekly&time_period=10&series_type=open'
        if source == 'IEX':
            Specify the API HTTP request, for instance '/deep/book?symbols={symbol}{?or&}',
            '/data-points/market/{symbol}{?or&}'. Check in documentation whether token should be added using & or ?.
            Available HTTP requests: https://iexcloud.io/docs/api/
            The full list of symbols can be found here: https://iextrading.com/trading/eligible-symbols/
    function: str, optional (default=None)
        Use only with source == 'AV'. Specify the API function to use, for instance: TIME_SERIES_INTRADAY,
        FX_INTRADAY, STOCH.
    symbol: str, optional (default=None)
        Use only with source == 'AV'. The name of the equity. For example: MSFT, SPY. In case of using FOREX API
        pass a string that specifies pair of currencies, for example: EURUSD (length of 6 chars).
    interval: str, optional (default=None)
        Use only with source == 'AV'. Time interval between two consecutive data points in the time series.
        Specify only for INTRADAY data and TECHNICAL INDICATORS: 1min, 5min, 15min, 30min, 60min.
    output_format: str, optional (default='json')
        Specify the output format. Available formats: 'json' or 'csv'.
    get_stock_volume: str, optional (default=None)
        Pass symbol to return it's volume from AV.

    """
    current_datetime = pytz.utc.localize(datetime.datetime.utcnow()).astimezone(time_zone['EST'])
    current_date = current_datetime.date()

    market_calendar = get_market_calendar()

    # Check if market is open today.
    market_day = list(filter(lambda date_dict: date_dict.get('date') == current_date.strftime('%Y-%m-%d'),\
                        market_calendar))[0]
    is_open = market_day.get('status') == 'open'

    if is_open:
        print(" ✅✅✅✅✅✅✅✅✅✅ hello market is open")
        if source == 'IEX':
            # Extract Stock market hours as strings
            premarket_start, premarket_end = market_day.get('premarket').values()
            market_start, market_end = market_day.get('open').values()
            postmarket_start, postmarket_end = market_day.get('postmarket').values()

            # Convert market hours to datetime objects (EST)
            hours = [('premarket_start', premarket_start), ('premarket_end', premarket_end), ('market_start', market_start),\
                     ('market_end', market_end), ('postmarket_start', postmarket_start), ('postmarket_end', postmarket_end)]
            market_hours = {key: market_hour_to_dt(current_datetime, value) for key, value in hours}

        else:
            market_hours = {}

            # FOREX market trade hours
            market_start = current_datetime.replace(hour=17, minute=0, second=0, microsecond=0, tzinfo=time_zone['EST'])
            market_hours['market_start'] = market_start - datetime.timedelta(days=(current_datetime.weekday() + 1))

            market_end = current_datetime.replace(hour=16, minute=0, second=0, microsecond=0, tzinfo=time_zone['EST'])
            market_hours['market_end'] = market_end + datetime.timedelta(days=-(current_datetime.weekday() - 4))


        # Call the function that is responsible for fetching the intraday data
        print("intraday!!!")
        intraday_data(freq, market_hours, current_datetime, source, tokens, economic_data, cot=get_cot, vix=get_vix, request=request,
                      function=function, symbol=symbol, interval=interval, output_format=output_format,
                      get_stock_volume=get_stock_volume)

    else:
        logging.warning('Current time: {} {}'.format(datetime.datetime.strftime(current_datetime, "%Y-%m-%d %I:%M %p"),\
            current_datetime.tzname()))
        logging.warning('Today market is closed')


# Data fetching frequency in seconds
freq = 60 * 5

economic_data = {'countries': ['United States'], 'importance': ['1', '2', '3'], 'event_list': event_list, 'cot': 'S&P 500 STOCK INDEX'}


@app.route('/run', methods=['GET'])

def run():
    start_day_session(freq, 'IEX', tokens, economic_data, cot=get_cot, vix=get_vix, request='/deep/book?symbols=spy&',
        get_stock_volume=get_stock_volume)
    print("START DAY SESSION DONE")

    return Response('Session complete', status=200)

import threading
import os

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    threading.Thread(target=lambda: start_day_session(
        freq, 'IEX', tokens, economic_data,
        cot=get_cot, vix=get_vix,
        request='/deep/book?symbols=spy&',
        get_stock_volume=get_stock_volume
    ), daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))


