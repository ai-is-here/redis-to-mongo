from redis_to_mongo.config_loader import Config
from redis_to_mongo.sync_engine import SyncEngine
import sys
import os
from redis_to_mongo.constants import TEST_CONFIG_ENV, PROD_CONFIG_ENV

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ["prod", "test"]:
        print("Usage: run.py [prod|test]")
        sys.exit(1)

    environment = sys.argv[1]
    if environment == "prod":
        CONFIG_PATH = PROD_CONFIG_ENV
    else:
        CONFIG_PATH = TEST_CONFIG_ENV

    config = Config(CONFIG_PATH)
    se = SyncEngine(config)
    se.run()
