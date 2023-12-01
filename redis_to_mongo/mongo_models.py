from typing import Any
from mongoengine import (
    Document,
    StringField,
    DictField,
    DateTimeField,
    ReferenceField,
    ListField,
    BooleanField,
)
from mongoengine.queryset.base import DO_NOTHING as MONGO_REF_DELETE_DO_NOTHING
import datetime

DATE_BASED_POSTFIX = "acc" # datetime.datetime.utcnow().strftime("%Y_month_%m")


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

    def to_dict(self):
        return self.to_mongo().to_dict()


class KeyedDocument(BaseDocument):
    RESET_FIELDS = []
    key = StringField(required=True, unique=True)
    active_now = BooleanField(required=True)

    meta = {
        "abstract": True,
        "indexes": ["key", "active_now"],
    }

    def update_active_now_no_save(self, active_now: bool) -> bool:
        if active_now == self.active_now:
            return False
        self.active_now = active_now
        return True

    def reset_fields_to_default_no_save(self):
        for field_name in self.RESET_FIELDS:
            field = self._fields.get(field_name)
            if field is None:
                raise AttributeError(
                    f"Field {field_name} does not exist in the Document."
                )
            if not hasattr(field, "default"):
                raise ValueError(f"No default value for field {field_name}.")
            setattr(self, field_name, field.default())


class JSONODM(KeyedDocument):
    RESET_FIELDS = ["value"]
    value = DictField(default=lambda: None)
    meta = {
        "collection": f"json_{DATE_BASED_POSTFIX}",
    }


class StringODM(KeyedDocument):
    RESET_FIELDS = ["value"]
    value = StringField(default=lambda: None)
    meta = {
        "collection": f"string_{DATE_BASED_POSTFIX}",
    }


class ListODM(KeyedDocument):
    RESET_FIELDS = ["values"]
    values = ListField(StringField(), default=lambda: [])
    meta = {
        "collection": f"list_{DATE_BASED_POSTFIX}",
    }


class ZSetODM(KeyedDocument):
    RESET_FIELDS = ["values"]
    values = ListField(DictField(), default=lambda: [])
    meta = {
        "collection": f"zset_{DATE_BASED_POSTFIX}",
    }


class SetODM(KeyedDocument):
    RESET_FIELDS = ["values"]
    values = ListField(StringField(), default=lambda: [])
    meta = {
        "collection": f"set_{DATE_BASED_POSTFIX}",
    }


class StreamODM(KeyedDocument):
    RESET_FIELDS = ["last_redis_read_id"]
    last_redis_read_id = StringField(default=lambda: "0-0")
    activity_history = ListField(DictField(), default=[])  # type: ignore

    meta = {
        "collection": f"stream_{DATE_BASED_POSTFIX}",
    }

    def update_active_now_no_save(self, active_now: bool) -> bool:
        if not super().update_active_now_no_save(active_now):
            return False
        if self.activity_history is None:
            self.activity_history: list[dict[str, Any]] = []
        self.activity_history.append(
            {"timestamp": datetime.datetime.utcnow(), "active_now": active_now}
        )
        return True


class StreamMessageODM(BaseDocument):
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    stream = ReferenceField(
        StreamODM, reverse_delete_rule=MONGO_REF_DELETE_DO_NOTHING, required=True
    )
    rid = StringField(required=True)  # id of the message in redis, convinent to sort by
    content = DictField(required=True)  # JSON payload

    meta = {
        "collection": f"stream_message_{DATE_BASED_POSTFIX}",
        "indexes": ["stream", "rid"],
    }
