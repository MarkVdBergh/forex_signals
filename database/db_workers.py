from calendar import monthrange
from datetime import datetime
import pandas as pd
import sys
from pymongo import MongoClient

from db_queries import store_raw_data, get_raw_data, get_all_raw_data, store_resampled_data, get_all_resampled_data
from db_settings import MONGO_HOST, MONGO_PORT, DB, CURRENCIES


def load_raw_data(currency='EURUSD', year=2017, month=0):
    '''
    - Download raw minute data at: http://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes
      and store in ../raw_data/
    - Unzip the downloaded file.
      The name of the directory for full year is HISTDATA_COM_ASCII_CCCCCC_M1YYYY,
      and the name for full year data is DAT_ASCII_CCCCCC_M1_yyyy.csv.
      The name of the directory for a month is HISTDATA_COM_ASCII_CCCCCC_M1YYYYMM,
      and the name of the month data is DAT_ASCII_EURUSD_M1_yyyymm.csv
    - load_raw_data() stares the raw quotes in mongodb
    - Delimiter is ';'
    '''

    if month == 0:  # full year
        f = 'DAT_ASCII_{}_M1_{}.csv'.format(currency, year)
    else:  # month data
        f = 'DAT_ASCII_{}_M1_{}{}.csv'.format(currency, year, str(month).zfill(2))

    path = '../raw_data/download/'  # Todo: move path to settings.py
    subdir = 'HISTDATA_COM_ASCII_{}_M1{}/'.format(currency, year)
    try:
        ohlc = pd.read_csv(filepath_or_buffer=path + subdir + f, delimiter=';', header=None, index_col=0, parse_dates=True)
        ohlc.columns = ['open', 'high', 'low', 'close', 'volume']
        ohlc.index.name = 'date'
        # Convert EST to UTC
        # "Eastern Standard Time (EST) time-zone WITHOUT Day Light Savings adjustments"
        # See: http://www.histdata.com/f-a-q/data-files-detailed-specification/
        ohlc.index = ohlc.index.tz_localize('US/Eastern')  # data read is in EST format
        ohlc.index = ohlc.index.tz_convert('UTC')  # convert to UTC
        # Round to 4 decimals
        ohlc = ohlc.round({'open': 4, 'high': 4, 'low': 4, 'close': 4})
        return ohlc
    except IOError as e:
        print 'Error!', sys.exc_info()[1]
        return None
    except:
        print "Unexpected error:", sys.exc_info()[0]
        raise


def load_store_all_raw_data():
    for cur in CURRENCIES:
        for year in range(2000, 2020):
            ohlc = load_raw_data(currency=cur, year=year, month=0)
            if ohlc is not None:
                store_raw_data(currency=cur, year=year, month=0, ohlc=ohlc)


def resample_store_raw_data(currency=None, frequency='min', scale='D'):
    '''
    If currency == '' then all currencies
    Resamples a raw timeseries to hour, day,... and stores it in the database
        Scale Description
        B	Business day
        D	Calendar day
        W	Weekly
        M	Month end
        Q	Quarter end
        A	Year end
        BA	Business year end
        AS	Year start
        H	Hourly frequency
        T, min	Minutely frequency
        S	Secondly frequency
        L, ms	Millisecond frequency
        U, us	Microsecond frequency
        N, ns	Nanosecond frequency
    '''
    frame = []
    if currency: currencies = [currency]
    else: currencies = CURRENCIES
    for cur in currencies:
        for pt in ['open', 'high', 'low', 'close']:
            raw = get_all_raw_data(currency=cur, frequency=frequency, pricetype=pt)
            if pt == 'open': frame.append(raw.resample(scale).first())
            if pt == 'high': frame.append(raw.resample(scale).max())
            if pt == 'low': frame.append(raw.resample(scale).min())
            if pt == 'close': frame.append(raw.resample(scale).last())
        ohlc = pd.concat(frame, axis=1)
        store_resampled_data(currency=cur, ohlc=ohlc)
        frame = []


def calculate_returns(currency=None, frequency='D', begin='1970', end='2020', pricetype=None, periods=1):
    # Todo: should this method be here? Should I save the returns?
    if currency: currencies = [currency]
    else: currencies = CURRENCIES
    if pricetype: pricetypes = [pricetype]
    else: pricetypes = ['open', 'high', 'low', 'close']

    cframes = []  # list of dataframes with currencies (ohlc)
    pframes = []  # list of dataframes with price column ('open' or 'close' or ...)
    for cur in currencies:
        for pt in pricetypes:
            prices = get_all_resampled_data(currency=cur, frequency='D', begin=begin, end=end, pricetype=pt)
            pframes.append(prices.pct_change(periods=periods))
        cframes.append(pd.concat(pframes, axis=1))
        pframes = []  # list of dataframes with price column ('open' or 'close' or ...)
    df = pd.concat(cframes, keys=currencies,axis=1)
    return df.dropna(how='any')


if __name__ == '__main__':
    # load_store_all_raw_data()
    # resample_store_raw_data()
    # print calculate_returns(currency='EURUSD', begin='2005', end='2007', periods=1)
    print calculate_returns(begin='2007', end='2008', periods=1)
    pass
