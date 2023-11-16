from mongoengine import Document, StringField, DictField, DateTimeField, ReferenceField
from mongoengine.queryset.base import DO_NOTHING as MONGO_REF_DELETE_DO_NOTHING
import datetime

WEEK_POSTFIX = datetime.datetime.utcnow().strftime("%Y_week_%W")


class Stream(Document):
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    updated_at = DateTimeField(default=datetime.datetime.utcnow)
    name = StringField(required=True, unique=True)
    metadata_sets = DictField()  # Format: {"set_name": [{...}, {...}]}
    last_redis_read_id = StringField(default="0")

    meta = {
        "collection": f"stream_{WEEK_POSTFIX}",
        "indexes": ["name"],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.utcnow()
        return super(Stream, self).save(*args, **kwargs)

    def update(self, *args, **kwargs):
        self.updated_at = datetime.datetime.utcnow()
        return super(Stream, self).update(*args, **kwargs)


class StreamMessage(Document):
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    stream = ReferenceField(
        Stream, reverse_delete_rule=MONGO_REF_DELETE_DO_NOTHING, required=True
    )
    content = DictField(required=True)  # JSON payload
    type = StringField(default="main", required=True)

    meta = {
        "collection": f"stream_message_{WEEK_POSTFIX}",
        "indexes": ["stream"],
    }
