from mongoengine import connect
from redis_to_mongo_mongo_modules.config_loader import MongoConfig


class MongoHandler:
    _instance = None

    def __new__(cls, config: MongoConfig):
        if cls._instance is None:
            cls._instance = super(MongoHandler, cls).__new__(cls)
            c = config.config
            cls._instance.client = connect(
                host=c["mongo_host"],
                port=c["mongo_port"],
                db=c["mongo_db_name"],
                username=c["mongo_username"],
                password=c["mongo_password"],
                authentication_source="admin",
                UuidRepresentation="standard",
            )
            cls._instance.db = cls._instance.client.get_database(c["mongo_db_name"])
        return cls._instance
