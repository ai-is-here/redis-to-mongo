from abc import ABC, abstractmethod
from typing import Any
from pymongo import UpdateOne
from redis_to_mongo.redis_api import RedisHandler
from redis_to_mongo.mongo_models import BaseDocument
from redis_to_mongo.logger import logger
from redis_to_mongo.config_loader import Config


# string, list, set, zset, hash and stream
# https://redis.io/commands/type/


class SyncTypeInterface(ABC):
    TYPE = None  # To be defined by each child class
    ODM_CLASS: type[BaseDocument] | None = None

    def __init__(self, config: Config, redis_handler: RedisHandler):
        self.redis_handler = redis_handler
        self.config = config
        self.odm_ids = {}  # keys: ODMs from mongo

    def filter_key_types(self, key_types: dict[str, str]) -> list[str]:
        return [key for key, type in key_types.items() if type == self.TYPE]

    def get_odm_class(self) -> type[BaseDocument]:
        if self.ODM_CLASS is None:
            logger.error("ODM_CLASS is not defined for this sync type.")
            raise ValueError("ODM_CLASS is not defined for this sync type.")
        return self.ODM_CLASS

    def init(self, key_types: dict[str, str]):
        keys = self.filter_key_types(key_types)
        for key in keys:
            odm = self.get_odm_class().objects(key=key).first()
            if odm:
                self.odm_ids[key] = odm.id
        self.sync_structure(keys)

    def sync_structure(self, key_types: list[str]):
        keys = self.filter_key_types(key_types)
        for key in keys:
            if key not in self.odm_ids:
                new_odm = self.get_odm_class()(key=key)
                new_odm.save()
                self.odm_ids[key] = new_odm.id

    def sync(self, key_types: dict[str, str]):
        keys = self.filter_key_types(key_types)
        self.sync_structure(keys)
        updates = self._sync(keys)
        self.bulk_update(updates, False)

    @abstractmethod
    def _sync(self) -> dict[str, dict[str, Any]]:
        pass

    def bulk_update(self, updates: dict[str, dict[str, Any]], ordered) -> None:
        operations = [
            UpdateOne({"_id": str(_id)}, {"$set": update})
            for _id, update in updates.items()
        ]
        self.bulk_write_ops(operations, ordered)

    def bulk_write_ops(self, operations, ordered):
        self.get_odm_class()._get_collection().bulk_write(operations, ordered=ordered)