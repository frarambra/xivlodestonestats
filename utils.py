from pymongo import MongoClient
from fflogs_utils import create_metadata_collections
import json

with open('config/mongo_conn.json') as f_read:
    conn_vars = json.load(f_read)
    MONGO_SERVER = conn_vars["MONGO_SERVER"]
    DATABASE = conn_vars["DATABASE"]
    character_collection = conn_vars["character_colletion"]
    endgame_metadata = conn_vars["endgame_metadata"]


def create_lodestone_collection(top_index: int = 20_000_001) -> None:
    mongo_client = MongoClient(MONGO_SERVER)
    db = mongo_client[DATABASE]
    indexes = [{"_id": i, "scrapped_lodestone_date": None,
                "scrapped_fflogs_date": None} for i in range(1, top_index)]
    db[character_collection].insert_many(indexes)


def delete_db():
    mongo_client = MongoClient(MONGO_SERVER)
    if DATABASE in [_['name'] for _ in mongo_client.list_databases()]:
        mongo_client.drop_database(DATABASE)


def split(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]


def __main__():
    delete_db()
    print('getting metadata')
    create_metadata_collections()
    print('creating lodestone collection')
    create_lodestone_collection(100000)


if __name__ == '__main__':
    __main__()
