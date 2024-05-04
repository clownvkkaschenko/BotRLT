from motor.motor_asyncio import AsyncIOMotorClient

from config import collection_name, db_name, uri


class MongoDBClient:
    def __init__(
            self, uri=uri,
            db_name=db_name, collection_name=collection_name
    ):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def get_database(self):
        """Возвращает базу данных."""
        return self.db

    def get_collection(self):
        """Возвращает коллекцию."""
        return self.collection


mongo_client = MongoDBClient()
