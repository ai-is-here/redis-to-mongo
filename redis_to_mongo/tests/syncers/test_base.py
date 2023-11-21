import pytest

from redis_to_mongo.tests.conftest import redis_populate_all, data_dict, mongo_handler
from redis_to_mongo.syncers import SyncStrings
from redis_to_mongo.mongo_models import StringODM
from pymongo import UpdateOne


def test_filter_key_types():
    sync_strings = SyncStrings(None, None)
    key_types = {
        "key1": "string",
        "key2": "list",
        "key3": "string",
        "key4": "set",
    }
    filtered_keys = sync_strings.filter_key_types(key_types)
    assert filtered_keys == ["key1", "key3"]


def test_get_odm_class():
    sync_strings = SyncStrings(None, None)
    assert sync_strings.get_odm_class() == StringODM


def test_bulk_update(mongo_handler):
    sync_strings = SyncStrings(None, None)
    updates = {
        StringODM(key="key1", value="new_value1").save().id: {"value": "new_value1"},
        StringODM(key="key2", value="new_value2").save().id: {"value": "new_value2"},
    }
    sync_strings.bulk_update(updates, False)
    for _id, update in updates.items():
        updated_odm = StringODM.objects(id=_id).first()
        assert updated_odm.value == update["value"]


def test_bulk_write_ops(mongo_handler):
    sync_strings = SyncStrings(None, None)
    id1 = StringODM(key="key1", value="new_value1").save().id
    id2 = StringODM(key="key2", value="new_value2").save().id
    values = ["new_value1", "new_value2"]
    operations = [
        UpdateOne({"_id": id}, {"$set": {"value": value}})
        for id, value in zip([id1, id2], values)
    ]
    sync_strings.bulk_write_ops(operations, False)
    for _id, value in zip([id1, id2], values):
        updated_odm = StringODM.objects(id=_id).first()
        assert updated_odm.value == value
