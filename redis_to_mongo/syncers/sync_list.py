from typing import Any
from redis_to_mongo.mongo_models import ListODM
from redis_to_mongo.syncers.syncer_base import SyncTypeInterface


class SyncUsualSets(SyncTypeInterface):
    TYPE = "list"
    ODM_CLASS = ListODM

    def _sync(self) -> dict[str, dict[str, Any]]:
        updates = {}
        for key, odm_id in self.odm_ids.items():
            redis_list = self.redis_handler.get_list(key)
            # guaranteed to be there
            odm = self.get_odm_class().objects(id=odm_id).first()
            if redis_list != odm.values:
                updates[odm_id] = {"values": redis_list}
        return updates
