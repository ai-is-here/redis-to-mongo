from mongoengine import connect
from redis_to_mongo.config_loader import Config


class MongoHandler:
    def __init__(self, config: Config):
        c = config.config
        self.client = connect(
            host=f"{c['mongo_host']}{c['mongo_db_name']}?authSource=admin"
        )
        self.db = self.client.get_database(c["mongo_db_name"])
