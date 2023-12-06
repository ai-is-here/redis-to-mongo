from typing import Any
from redis_to_mongo.redis_to_mongo_mongo_modules.mongo_models import StringODM
from redis_to_mongo.syncers.syncer_base import SyncTypeInterface


class SyncStrings(SyncTypeInterface):
    TYPE = "string"
    ODM_CLASS = StringODM

    def _sync(self) -> dict[str, dict[str, Any]]:
        updates = {}
        for key, odm_id in self.odm_ids.items():
            redis_string = self.redis_handler.get_string(key)
            # guaranteed to be there
            odm = self.get_odm_class().objects(id=odm_id).first()
            if redis_string != odm.value:
                updates[odm_id] = {"value": redis_string}
        return updates
