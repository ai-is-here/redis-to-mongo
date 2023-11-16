from mongoengine import connect
from redis_to_mongo.config_loader import Config



class MongoHandler:
    def __init__(self, config: Config) -> None:
        self.client = connect(
            host=f"mongodb://{config.mongo_username}:{config.mongo_password}@{config.mongo_host}:{config.mongo_port}/{config.mongo_db_name}?authSource=admin"
        )
        self.db = self.client.get_database(config.mongo_db_name])
