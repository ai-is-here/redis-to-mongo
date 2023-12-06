from typing import Any
from redis_to_mongo.redis_to_mongo_mongo_modules.mongo_models import ZSetODM
from redis_to_mongo.syncers.syncer_base import SyncTypeInterface


class SyncZSets(SyncTypeInterface):
    TYPE = "zset"
    ODM_CLASS = ZSetODM

    def _sync(self) -> dict[str, dict[str, Any]]:
        updates = {}
        for key, odm_id in self.odm_ids.items():
            redis_ordered_set = self.redis_handler.get_ordered_set(key)
            # guaranteed to be there
            odm = self.get_odm_class().objects(id=odm_id).first()
            if redis_ordered_set != odm.values:
                updates[odm_id] = {"values": redis_ordered_set}
        return updates
