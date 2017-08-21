from calendar import monthrange
from datetime import datetime
from pymongo import MongoClient
import pandas as pd

from db_settings import MONGO_HOST, MONGO_PORT, DB


# Todo: Check if it's good to initiate db connection for every method. (client = MongoClient(host=MONGO_HOST, port=MONGO_PORT))
# Todo: Make methods more reusable. Use store_month(), store_year(), ... to store raw, resampled and signals


def store_raw_data(currency, year, month, ohlc):
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
    if month == 0:  # then it's a full year of data -> store new document per month (orherwise to big for bson)
        for m in range(1, 13):
            last_dom = monthrange(year, m)[1]  # last day of the month
            ohlc_month = ohlc[datetime(year, m, 1):datetime(year, m, last_dom)]
            for pricetype in ['open', 'high', 'low', 'close']:
                # Todo: Now data is stored as {date:[...], close:[...] would it be better if stored as # {close: {date:price}} ?
                filter_doc = {'currency': currency,
                              'type': 'raw',
                              'pricetype': pricetype,
                              'freq': 'min',
                              'year': year,
                              'month': m
                              }
                update_doc = {'$set': {'begin': ohlc.index.min(),  # datetime
                                       'end': ohlc.index.max(),  # datetime
                                       'date': ohlc_month.index.tolist(),
                                       pricetype: ohlc_month[pricetype].values.tolist(),
                                       }}
                client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
                db = client[DB]
                result = db.forex.update_one(filter=filter_doc,
                                             update=update_doc,
                                             upsert=True)
                print 'Saved {}-{}-{}-{} '.format(currency, year, m, pricetype)

    else:  # then it's only a month of data -> update existing year with month document
        pass  # Todo: make update document for month data
        result = None
    return result


def store_resampled_data(currency, ohlc):
    # Remove NaN
    freq = pd.infer_freq(ohlc.index)
    ohlc.dropna(inplace=True)
    begin = ohlc.index.min()  # pandas.tslib.Timestamp
    end = ohlc.index.max()  # pandas.tslib.Timestamp

    for y in range(begin.year, end.year + 1):  # Store 1 year of data
        for pricetype in ['open', 'high', 'low', 'close']:
            # Todo: Now data is stored as {date:[...], close:[...] would it be better if stored as # {close: {date:price}} ?
            filter_doc = {'currency': currency,
                          'type': 'resampled',
                          'pricetype': pricetype,
                          'freq': freq,
                          'year': y,
                          }

            update_doc = {'$set': {'begin': begin,
                                   'end': end,
                                   'date': ohlc.index.tolist(),
                                   pricetype: ohlc[pricetype].values.tolist(),
                                   }}

            client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
            db = client[DB]
            result = db.forex.update_one(filter=filter_doc,
                                         update=update_doc,
                                         upsert=True)
            print 'Saved {}-{}-{}'.format(currency, y, pricetype)


def get_raw_data(currency, frequency, year, month, pricetype):
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client[DB]
    result = db.forex.find_one({'currency': currency,
                                'type': 'raw',
                                'pricetype': pricetype,
                                'freq': frequency,
                                'year': year,
                                'month': month})
    return result


def get_all_raw_data(currency='EURUSD', frequency='min', begin='1970-01', end='2020-12', pricetype='open'):
    '''
    Get all the 'pricetype' raw documents starting from 'begin' till 'end' for currency
    and return a dataframe
    '''
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client[DB]
    begin_y = int(begin[:4])
    begin_m = int(begin[5:7])
    end_y = int(end[:4])
    end_m = int(end[5:7])
    result = db.forex.find({'currency': currency,
                            'type': 'raw',
                            'pricetype': pricetype,
                            'freq': frequency,
                            'year': {'$gte': begin_y, '$lte': end_y},
                            'month': {'$gte': begin_m, '$lte': end_m},
                            })
    frames = []
    print 'Got {} {}-{}-{} documents'.format(result.count(), currency, frequency, pricetype)
    for doc in result:
        result_df = pd.DataFrame({'date': doc['date'], pricetype: doc[pricetype]})
        result_df.set_index(keys='date', inplace=True)
        frames.append(result_df)
    return pd.concat(frames)


if __name__ == '__main__':
    print get_all_raw_data()
