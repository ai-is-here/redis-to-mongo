from mongoengine import (
    Document,
    StringField,
    DictField,
    DateTimeField,
    ReferenceField,
    ListField,
)
from mongoengine.queryset.base import DO_NOTHING as MONGO_REF_DELETE_DO_NOTHING
import datetime

WEEK_POSTFIX = datetime.datetime.utcnow().strftime("%Y_week_%W")


from mongoengine import Document


class BaseDocument(Document):
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    updated_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"abstract": True}

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.utcnow()
        return super().save(*args, **kwargs)

    def update(self, *args, **kwargs):
        self.updated_at = datetime.datetime.utcnow()
        return super().update(*args, **kwargs)


class OrderedSet(BaseDocument):
    key = StringField(required=True, unique=True)
    values = ListField(DictField(), default=[])
    meta = {
        "collection": f"ordered_set_{WEEK_POSTFIX}",
        "indexes": ["key"],
    }


class Stream(BaseDocument):
    key = StringField(required=True, unique=True)
    last_redis_read_id = StringField(default="0-0")

    meta = {
        "collection": f"stream_{WEEK_POSTFIX}",
        "indexes": ["key"],
    }


class StreamMessage(BaseDocument):
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    stream = ReferenceField(
        Stream, reverse_delete_rule=MONGO_REF_DELETE_DO_NOTHING, required=True
    )
    rid = StringField(required=True)  # id of the message in redis, convinent to sort by
    content = DictField(required=True)  # JSON payload

    meta = {
        "collection": f"stream_message_{WEEK_POSTFIX}",
        "indexes": ["stream", "rid"],
    }
