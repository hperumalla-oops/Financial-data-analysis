import requests
import json
import pandas as pd
import io
import datetime
import logging
from config import time_zone
import time
        

from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

def market_hours_for_day(date):
    open_dt = datetime.datetime.combine(date, datetime.time(9, 30), tzinfo=ET)
    close_dt = datetime.datetime.combine(date, datetime.time(16, 0), tzinfo=ET)
    return open_dt, close_dt



# Function responsible for replacing unwanted characters in dictionary keys
def change_keys(obj, old, new):
    """Recursively goes through the dictionary obj and changes keys by
    replacing old chars with new ones.

    """
    if isinstance(obj, dict):
        new_obj = obj.__class__()
        for k, v in obj.items():
            new_obj[k.replace(old, new)] = change_keys(v, old, new)
    elif isinstance(obj, (list, set, tuple)):
        new_obj = obj.__class__(change_keys(v, old, new) for v in obj)
    else:
        return obj

    return new_obj

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

def value_to_number(obj):
    """Recursively goes through the dictionary obj and converts strings
    to ints or floats if possible.

    """
    if isinstance(obj, dict):
        new_obj = obj.__class__()
        for k, v in obj.items():
            if isinstance(v, dict):
                new_obj[k] = value_to_number(v)
            elif isinstance(v, (list, set, tuple)):
                new_obj[k] = v.__class__(to_number(lv) for lv in v)
            else:
                new_obj[k] = to_number(v)

    elif isinstance(obj, (list, set, tuple)):
        new_obj = obj.__class__(value_to_number(lv) for lv in obj)
    else:
        return obj

    return new_obj


class GetData:
    """Get the data from Alpha Vantage or IEX APIs.

    Parameters
    ----------
    token: dict
        Dictionary of API (keys) tokens. Dictionary keys: 'av_token' and 'iex_token'.
    output_format: str, optional (default='json')
        Specify the output format. Available formats: 'json' or 'csv'.

    """

    def __init__(self, token, output_format='json'):

        self.__token = token
        self.output_format = output_format

        assert (output_format == 'json' or output_format == 'csv'), '{} format is not supported'\
            .format(output_format)



    def get_iex_data(self, request, timestamp):
        timestamp_str = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")
        
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1m&range=1d"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            r = requests.get(url, headers=headers, timeout=10)
            data = r.json()
            last_price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            print("fwefwef faaaaaaaaart")
        except Exception as e:
            print(f"Price fetch failed: {e}")
            last_price = 450

        bid_price = last_price - 0.01
        ask_price = last_price + 0.01

        raw_data = {'Timestamp': timestamp_str}
        for i in range(7):
            raw_data['bids_{:d}'.format(i)] = {
                'bid_{:d}'.format(i): round(bid_price - (i * 0.01), 2),
                'bid_{:d}_size'.format(i): 500
            }
        for i in range(7):
            raw_data['asks_{:d}'.format(i)] = {
                'ask_{:d}'.format(i): round(ask_price + (i * 0.01), 2),
                'ask_{:d}_size'.format(i): 500
            }
        return raw_data

    def get_av_data(self, timestamp, function=None, symbol=None, interval=None, request=None):
        timestamp_str = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")
        
        try:
            # Map interval to yfinance format
            interval_map = {'1min': '1m', '5min': '5m', '15min': '15m', '30min': '30m', '60min': '60m'}
            yf_interval = interval_map.get(interval, '5m')
            symbol = symbol or 'SPY'

            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval={yf_interval}&range=1d"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            r = requests.get(url, headers=headers, timeout=10)
            data = r.json()
            result = data["chart"]["result"][0]
            meta = result["meta"]
            
            last_price = meta["regularMarketPrice"]
            volume = result["indicators"]["quote"][0]["volume"][-1] or 0

            raw_data = {
                "1_open": meta.get("chartPreviousClose", last_price),
                "2_high": last_price,
                "3_low": last_price,
                "4_close": last_price,
                "5_volume": volume,
                "Timestamp": timestamp_str
            }

            raw_data = change_keys(raw_data, ". ", "_")
            raw_data = value_to_number(raw_data)
            return raw_data

        except Exception as e:
            logging.error(f"get_av_data failed: {e}")
            return None
        


def get_market_calendar():
    today = datetime.date.today()
    year, month = today.year, today.month
    days_in_month = (datetime.date(year, month % 12 + 1, 1) - datetime.timedelta(days=1)).day
    calendar = []
    for day in range(1, days_in_month + 1):
        date = datetime.date(year, month, day)
        if date.weekday() < 5:  # Mon-Fri
            open_dt, close_dt = market_hours_for_day(date)
            calendar.append({
                "date": str(date),
                "status": "open",
                "open": {"start": "09:30", "end": "16:00"},
                "premarket": {"start": "07:00", "end": "09:25"},
                "postmarket": {"start": "16:05", "end": "20:00"}
            })
    return calendar

