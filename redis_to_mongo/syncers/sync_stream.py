from typing import Any
from pymongo import InsertOne
from redis_to_mongo.redis_to_mongo_mongo_modules.mongo_models import (
    StreamODM,
    StreamMessageODM,
)
from redis_to_mongo.syncers.syncer_base import SyncTypeInterface


class SyncStreams(SyncTypeInterface):
    TYPE = "stream"
    ODM_CLASS = StreamODM

    def init(self, key_types: dict[str, str]):
        self.last_read_ids = {}
        super().init(key_types)

    def sync_structure(self, key_types: list[str]):
        super().sync_structure(key_types)
        for key, odm_id in self.odm_ids.items():
            # we are not  assigning a default but checking db, because init also uses this method, and no last reads will be here at that point.
            # avoiding fixing inefficiencies due to time and not having a bottleneck yet
            if key not in self.last_read_ids:
                mongo_stream = self.get_odm_class().objects(id=odm_id).first()
                # if stream disappears and then reappears, means it was removed from redis and so
                # we start with it from its new beginnign and so last read id 0-0 and not what we store. doesn't matter
                self.last_read_ids[key] = mongo_stream.last_redis_read_id

    def _sync(self) -> dict[str, dict[str, Any]]:
        updates = {}
        if not self.last_read_ids:
            return {}
        all_messages = self.redis_handler.read_messages(self.last_read_ids)
        # will need to update last read ids only for the streams that got read and we assume other values unchanged
        write_ops = []
        for stream, messages in all_messages.items():
            stream_id = self.odm_ids[stream]
            for message in messages:
                write_ops.append(
                    InsertOne(
                        {"stream": stream_id, "rid": message[0], "content": message[1]}
                    )
                )

            updates[stream_id] = {"last_redis_read_id": self.last_read_ids[stream]}
        # messages are unordered and important field internal time we had in stream or message id i guess
        self.bulk_write_ops(write_ops, ordered=False, odm_override=StreamMessageODM)
        self.changes_processed += len(write_ops)
        return updates
