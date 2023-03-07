from pymongo import MongoClient
from fflogs_utils import create_metadata_collections
import os

# MongoDB things
MONGO_URI = os.getenv("MONGO_URI")
DATABASE = os.getenv("MONGO_DB")
CHARACTER_COLLECTION = os.getenv("MONGO_CHARACTER")
ENDGAME_COLLECTION = os.getenv("MONGO_ENDGAME")
ENDGAME_METADATA = os.getenv("MONGO_ENDGAME_METADA")


def create_empty_documents(top_index: int = 20_000_001) -> None:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DATABASE]
    indexes = [{"_id": i, "scrapped_lodestone_date": None,
                "scrapped_fflogs_date": None} for i in range(1, top_index)]
    db[CHARACTER_COLLECTION].insert_many(indexes)


def delete_db():
    mongo_client = MongoClient(MONGO_URI)
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
    create_empty_documents(100000)


if __name__ == '__main__':
    __main__()
