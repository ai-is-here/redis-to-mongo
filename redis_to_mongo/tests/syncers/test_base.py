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


def test_no_ops_empty_for_bulk_write(mongo_handler):
    sync_strings = SyncStrings(None, None)
    operations = []
    sync_strings.bulk_write_ops(operations, False)
    assert StringODM.objects.count() == 0


def test_sync_structure(mongo_handler):
    sync_strings = SyncStrings(None, None)
    keys = ["key1", "key2"]
    sync_strings.sync_structure(keys)
    for key in keys:
        assert key in sync_strings.odm_ids
        assert StringODM.objects(key=key).first() is not None


def test_sync_structure_key_removal(mongo_handler):
    sync_strings = SyncStrings(None, None)
    keys = ["key1", "key2"]
    sync_strings.sync_structure(keys)
    keys.remove("key1")
    sync_strings.sync_structure(keys)
    assert "key1" not in sync_strings.odm_ids
    assert StringODM.objects(key="key1").first() is not None
    assert "key2" in sync_strings.odm_ids
    assert StringODM.objects(key="key2").first() is not None


def test_sync_structure_key_removal_and_addition(mongo_handler):
    sync_strings = SyncStrings(None, None)
    keys = ["key1", "key2"]
    sync_strings.sync_structure(keys)
    for key in keys:
        assert key in sync_strings.odm_ids
        assert StringODM.objects(key=key).first() is not None
    keys.remove("key1")
    sync_strings.sync_structure(keys)
    assert "key1" not in sync_strings.odm_ids
    assert StringODM.objects(key="key1").first() is not None
    keys.append("key1")
    sync_strings.sync_structure(keys)
    for key in keys:
        assert key in sync_strings.odm_ids
        assert StringODM.objects(key=key).first() is not None


def test_init_no_new_objects_needed(mongo_handler):
    sync_strings = SyncStrings(None, None)
    key_types = {
        "key1": "string",
        "key2": "string",
    }
    StringODM(key="key1").save()
    StringODM(key="key2").save()
    sync_strings.init(key_types)
    assert "key1" in sync_strings.odm_ids
    assert "key2" in sync_strings.odm_ids


def test_init_new_objects_needed(mongo_handler):
    sync_strings = SyncStrings(None, None)
    key_types = {
        "key1": "string",
        "key2": "string",
    }
    sync_strings.init(key_types)
    assert "key1" in sync_strings.odm_ids
    assert "key2" in sync_strings.odm_ids
    assert StringODM.objects(key="key1").first() is not None
    assert StringODM.objects(key="key2").first() is not None
