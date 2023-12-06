import signal
import sys

from redis_to_mongo.constants import PROD_CONFIG_ENV, TEST_CONFIG_ENV
from redis_to_mongo.sync_engine import SyncEngine
from redis_to_mongo.logger import logger

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ["prod", "test"]:
        print("Usage: run.py [prod|test]")
        sys.exit(1)

    environment = sys.argv[1]
    if environment == "prod":
        CONFIG_PATH = PROD_CONFIG_ENV
    else:
        CONFIG_PATH = TEST_CONFIG_ENV

    se = SyncEngine(config_path=CONFIG_PATH)

    def signal_handler(signum, frame):
        logger.info("Graceful shutdown initiated.")
        # Perform any necessary cleanup here
        se.shutdown()
        logger.info("SyncEngine has been stopped.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)

    try:
        se.run()
    except (KeyboardInterrupt, SystemExit):
        signal_handler(None, None)
