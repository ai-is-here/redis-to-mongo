from typing import Any
from redis_to_mongo.redis_to_mongo_mongo_modules.mongo_models import SetODM
from redis_to_mongo.syncers.syncer_base import SyncTypeInterface


class SyncSets(SyncTypeInterface):
    TYPE = "set"
    ODM_CLASS = SetODM

    def _sync(self) -> dict[str, dict[str, Any]]:
        updates = {}
        for key, odm_id in self.odm_ids.items():
            redis_set = self.redis_handler.get_set(key)
            # guaranteed to be there
            odm = self.get_odm_class().objects(id=odm_id).first()
            if sorted(redis_set) != odm.values:
                updates[odm_id] = {"values": redis_set}
        return updates
