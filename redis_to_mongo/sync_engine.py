import time
from collections import defaultdict

from redis_to_mongo.config_loader import Config
from redis_to_mongo.logger import logger
from redis_to_mongo.syncers import *
from redis_to_mongo.redis_api import RedisHandler

from redis_to_mongo.mongo_api import MongoHandler


class SyncEngine:
    """
    The SyncEngine class is responsible for synchronizing data between Redis and MongoDB.
    It uses the RedisHandler to interact with Redis and the Mongo API to interact with MongoDB.
    """

    def __init__(self, config: Config):
        self.config = config
        self.redis_handler = RedisHandler(config)
        self.mongo_handler = MongoHandler(config)
        self.init_syncers()

    def init_syncers(self):
        key_types = self.redis_handler.get_all_key_types()
        self.syncers: list[SyncTypeInterface] = []
        syncer_classes = [
            SyncJSONs,
            SyncLists,
            SyncSets,
            SyncStreams,
            SyncStrings,
            SyncZSets,
        ]
        for syncer in syncer_classes:
            s = syncer(self.config, self.redis_handler)
            s.init(key_types)
            self.syncers.append(s)

    def sync(self):
        key_types = self.redis_handler.get_all_key_types()
        implemented_types = set(syncer.TYPE for syncer in self.syncers)
        for key in list(key_types.keys()):
            if key_types[key] not in implemented_types:
                logger.warning(
                    f"Key {key} has unsupported type {key_types[key]} and will be removed from synchronization. Allowed types: {implemented_types=}."
                )
                del key_types[key]
        for syncer in self.syncers:
            syncer.sync(key_types)

    def run(self) -> None:
        """
        Run the SyncEngine indefinitely, synchronizing data between Redis and MongoDB.
        """
        start_time = time.time()
        logger.info("SyncEngine started at: %s", start_time)
        uptime = 0
        while True:
            round_start_time = time.time()
            self.sync()
            # Add logic for processing sets and metadata streams if necessary
            round_elapsed_time = time.time() - round_start_time
            logger.info(f"Round took: {round_elapsed_time} seconds")
            elapsed_time = time.time() - start_time
            uptime += elapsed_time
            logger.info(f"SyncEngine has been running for: {uptime} seconds")
            sleep_time = max(0, self.config.sync_interval_sec - round_elapsed_time)
            logger.info(
                f"Sleeping for: {sleep_time} seconds out of {self.config.sync_interval_sec} max value"
            )
            time.sleep(sleep_time)
            start_time = time.time()
