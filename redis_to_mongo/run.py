from redis_to_mongo.config_loader import Config
from redis_to_mongo.sync_engine import SyncEngine

if __name__ == "__main__":
    config = Config()
    se = SyncEngine(config)
    se.run()
