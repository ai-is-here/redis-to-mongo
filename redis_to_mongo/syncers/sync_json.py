from typing import Any
from redis_to_mongo.mongo_models import JSONODM
from redis_to_mongo.syncers.syncer_base import SyncTypeInterface


class SyncUsualSets(SyncTypeInterface):
    TYPE = "ReJSON-RL"
    ODM_CLASS = JSONODM

    def _sync(self) -> dict[str, dict[str, Any]]:
        updates = {}
        for key, odm_id in self.odm_ids.items():
            redis_json = self.redis_handler.get_json(key)
            # skipping change check
            updates[odm_id] = {"value": redis_json}
        return updates
