from calendar import monthrange
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from db_settings import MONGO_HOST, MONGO_PORT, DB


def load_raw_data(currency='EURUSD', year=2017, month=0):
    '''
    - Download raw minute data at: http://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes
      and store in ../raw_data/
    - Unzip the downloaded file. The name for full year data is DAT_ASCII_CCCCCC_M1_yyyy.csv for full year and
      for the current year DAT_ASCII_EURUSD_M1_yyyymm.csv
    - load_raw_data() stares the raw quotes in mongodb
    - Delimiter is ';'
    '''

    if month == 0:
        f = 'DAT_ASCII_{}_M1_{}.csv'.format(currency, year)
    else:
        f = 'DAT_ASCII_{}_M1_{}{}.csv'.format(currency, year, str(month).zfill(2))

    path = '../raw_data/download/'  # Todo: move path to settings.py
    ohlc = pd.read_csv(filepath_or_buffer=path + f, delimiter=';', header=None, index_col=0, parse_dates=True)
    ohlc.columns = ['open', 'high', 'low', 'close', 'volume']
    ohlc.index.name = 'date'

    # Convert EST to UTC
    # "Eastern Standard Time (EST) time-zone WITHOUT Day Light Savings adjustments"
    # See: http://www.histdata.com/f-a-q/data-files-detailed-specification/
    ohlc.index = ohlc.index.tz_localize('US/Eastern')  # data read is in EST format
    ohlc.index = ohlc.index.tz_convert('UTC')  # convert to UTC

    # add metadata to the dataframe by adding custom attibutes
    ohlc.currency = currency
    ohlc.freq = 'minute'
    ohlc.year = year
    ohlc.month = month

    return ohlc


def store_raw_data(ohlc):
    '''
    Receives a ohlc dataframe, converts it in a raw-data document and stores the document in mongodb.
    Document format:
    { currency: 'EURUSD',
      type: 'raw',
      freq: 'minute',
      year: 2017,
      start: 20160101 050100 # datetime
      end: 20161231 170100 # datetime
      date: [ ... ],
      open: [ ... ],
      high: [ ... ],
      low: [ ... ],
      close[ ... ]
    }
    '''
    result = None
    if ohlc.month == 0:  # then it's a full year of data -> store new document per month (orherwise to big for bson)
        for m in range(1, 3):
            last_dom = monthrange(ohlc.year, m)[1]  # last day of the month
            ohlc_month = ohlc[datetime(ohlc.year, m, 1):datetime(ohlc.year, m, last_dom)]
            for pricetype in ['open', 'high', 'low', 'close']:
                # Todo: Now data is stored as {date:[...], close:[...] would it be better if stored as # {close: {date:price}} ?
                update_doc = {'$set': {'start': ohlc.index.min(),  # datetime
                                       'end': ohlc.index.max(),  # datetime
                                       'date': ohlc_month.index.tolist(),
                                       pricetype: ohlc_month[pricetype].values.tolist(),
                                       }}
                # print raw_doc
                client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
                db = client[DB]
                result = db.forex.update_one(filter={'currency': ohlc.currency,
                                                     'type': 'raw',
                                                     'pricetype': pricetype,
                                                     'freq': 'minute',
                                                     'year': ohlc.year,
                                                     'month': m},
                                             update=update_doc,
                                             upsert=True)

    else:  # then it's only a month of data -> update existing year with month document
        pass  # Todo: make update document for month data
        result = None
    return result


if __name__ == '__main__':
    cur = 'EURUSD'
    ohlcx = load_raw_data(currency=cur, year=2016, month=0)
    store_raw_data(ohlcx)
