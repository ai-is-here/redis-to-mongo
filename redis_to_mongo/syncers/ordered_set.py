from typing import Any
from redis_to_mongo.mongo_models import OrderedSet
from redis_to_mongo.syncers.syncer_base import SyncTypeInterface


class SyncOrderedSets(SyncTypeInterface):
    TYPE = "ordered_set"
    ODM_CLASS = OrderedSet

    def _sync(self) -> dict[str, dict[str, Any]]:
        updates = {}
        for key, odm_id in self.odm_ids.items():
            redis_ordered_set = self.redis_handler.get_ordered_set(key)
            # guaranteed to be there
            mongo_ordered_set = self.ODM_CLASS.objects(id=odm_id).first()
            if redis_ordered_set != mongo_ordered_set.values:
                updates[odm_id] = {"values": redis_ordered_set}
        return updates
