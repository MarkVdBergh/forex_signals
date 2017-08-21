# mongodb settings
testing = True
TEST_DATABASE = 'test_forex'
PRODUCTION_DATABASE = 'production'
if not testing:
    print '*' * 100
    print ' ' * 20 + 'WARNING !!!!'
    print ' ' * 20 + 'WORKING ON PRODUCTION DATABASE'
    print '*' * 100
    DB = PRODUCTION_DATABASE
else:
    DB = TEST_DATABASE

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

# Localization settings
LOCAL_TIMEZONE = 'Europe/Brussels'
LOCAL_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Currency list
CURRENCIES = ['EURUSD', 'EURCHF', 'EURGBP', 'EURJPY', 'USDCAD']
# CURRENCIES=['EURUSD']