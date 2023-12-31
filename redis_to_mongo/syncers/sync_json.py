from typing import Any
from redis_to_mongo.redis_to_mongo_mongo_modules.mongo_models import JSONODM
from redis_to_mongo.syncers.syncer_base import SyncTypeInterface


class SyncJSONs(SyncTypeInterface):
    TYPE = "ReJSON-RL"
    ODM_CLASS = JSONODM

    def _sync(self) -> dict[str, dict[str, Any]]:
        updates = {}
        for key, odm_id in self.odm_ids.items():
            redis_json = self.redis_handler.get_json(key)
            odm = self.get_odm_class().objects(id=odm_id).first()
            if redis_json != odm.value:
                updates[odm_id] = {"value": redis_json}
        return updates
