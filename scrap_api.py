import datetime

from fastapi import FastAPI
from utils import DATABASE, MONGO_SERVER, character_collection, endgame_metadata, fflogs_vars
from pymongo import MongoClient

app = FastAPI()

# Common variables for the API
mongo_client = MongoClient(MONGO_SERVER)
db = mongo_client[DATABASE]


@app.get("/scraping/lodestone/{n_indexes}")
async def lodestone(n_indexes: int):
    """Returns the amount request of lodestone id to scrap"""
    m_filter = {
        "$and": [
            {"$or": [{"exists": "S"}, {"exists": None}]},
            {
                "$or": [{"scrapped_lodestone_date": None},
                        {"scrapped_lodestone_date": {"$lt": datetime.datetime.now() - datetime.timedelta(days=3)}}
                ]
            }
        ]
    }
    cursor = db[character_collection].find(m_filter, ['_id'], limit=n_indexes)
    indexes = [item['_id'] for item in cursor]
    if not indexes:
        # we had 1000 more indexes to scrap
        max_index = db[character_collection].find_one({}, ['_id'], sort=[("_id", -1)])['_id']
        indexes = [{"_id": i, "scrapped_lodestone_date": None,
                    "scrapped_fflogs_date": None} for i in range(max_index+1, max_index+1001)]
        db[character_collection].insert_many(indexes)
        indexes = [_['_id'] for _ in indexes][:n_indexes]
    print(f'sending: {indexes}')

    return {'lodestone_indexes': indexes}


@app.get("/scraping/fflogs/{n_indexes}")
async def fflogs(n_indexes: int):
    """
        Returns the amount indicated of characters to query the FFLOGS API.
        It can return the fflogs_id or the character name with its server
        if it wasn't matched the lodestone_id to the fflogs_id yet.
    """
    m_filter = {"$and": [
        {"exists": "S"},
        {"$or": [
            {"scrapped_fflogs_date": None},
            {"scrapped_fflogs_date": {"$lt": datetime.datetime.now() - datetime.timedelta(days=3)}}]}
        ]
    }

    cursor = db[character_collection].find(m_filter, ['_id', 'fflogs_id', 'name', 'server', 'region'], limit=n_indexes)

    response = {'fflogs_id': [], 'character_data': []}
    for item in cursor:
        item_keys = item.keys()
        if 'fflogs_id' in item_keys:
            response['fflogs_id'].append(item['fflogs_id'])
        elif 'name' in item_keys and 'server' in item_keys and 'region' in item_keys:
            tmp = {'name': item['name'], 'server': item['server'], 'region': item['region']}
            response['character_data'].append(tmp)
    return response

