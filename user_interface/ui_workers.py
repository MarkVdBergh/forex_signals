from database.db_settings import CURRENCIES
from database.db_workers import load_raw_data
from database.db_queries import store_raw_data, get_raw_data

def load_store_all_currencies():
    for cur in CURRENCIES:
        for year in range(2000, 2018):
            ohlc = load_raw_data(currency=cur, year=year, month=0)
            if ohlc is not None:
                store_raw_data(currency=cur, year=year, month=0, ohlc=ohlc)




if __name__ == '__main__':
    # load_store_all_currencies()
    print get_raw_data('EURUSD',2015,1,'close')
