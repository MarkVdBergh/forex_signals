from pymongo import MongoClient
from db_settings import MONGO_HOST, MONGO_PORT, DB

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB]


def setup_database_forex():
    collection = db.forex

    collection.create_index([('currency', 1), ('type', 1), ('pricetype', 1), ('freq', 1), ('year', 1), ('month', 1)], unique=True)
    collection.create_index('type')
    collection.create_index('freq')
    collection.create_index('year')
    collection.create_index('start')
    collection.create_index('end' )


if __name__ == '__main__':
    setup_database_forex()


