from curses import initscr
from multiprocessing import active_children
from os import sync
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
        StringODM(key="key1", value="new_value1", active_now=True)
        .save()
        .id: {"value": "new_value1"},
        StringODM(key="key2", value="new_value2", active_now=True)
        .save()
        .id: {"value": "new_value2"},
    }
    sync_strings.bulk_update(updates, False)
    for _id, update in updates.items():
        updated_odm = StringODM.objects(id=_id).first()
        assert updated_odm.value == update["value"]


def test_bulk_write_ops(mongo_handler):
    sync_strings = SyncStrings(None, None)
    id1 = StringODM(key="key1", value="new_value1", active_now=True).save().id
    id2 = StringODM(key="key2", value="new_value2", active_now=True).save().id
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


def test_sync_string_odm_with_redis_added_start_redis_notactive_mongo_notactive(
    config, redis_handler
):
    """
    init
    key NOT active mongo NOT active but exists
    key added mongo active
    """
    # Manually populate Redis with key-value pairs

    redis_handler.client.set("key2", "value2")

    # Manually populate MongoDB with StringODM objects
    StringODM(key="key1", value=None, active_now=False).save()
    StringODM(key="key2", value="value2", active_now=True).save()

    # Initialize SyncStrings
    sync_strings = SyncStrings(config, redis_handler)
    key_types = redis_handler.get_all_key_types()
    sync_strings.init(key_types)

    # Check initial state of SyncStrings and MongoDB
    assert StringODM.objects(key="key1").first().id not in sync_strings.odm_ids
    assert sync_strings.odm_ids["key2"] == StringODM.objects(key="key2").first().id
    assert StringODM.objects(key="key1").first().value == None
    assert StringODM.objects(key="key2").first().value == "value2"
    # Assert the total count of StringODM objects in MongoDB
    assert StringODM.objects.count() == 2
    # Assert the total count of keys in odm_ids
    assert len(sync_strings.odm_ids) == 1

    redis_handler.client.set("key1", "value3")
    key_types = redis_handler.get_all_key_types()
    sync_strings.sync(key_types)

    # Check MongoDB for changes after adding key1 back
    assert StringODM.objects(key="key1").first().active_now == True
    assert StringODM.objects(key="key1").first().value == "value3"
    assert StringODM.objects(key="key2").first().active_now == True
    assert StringODM.objects(key="key2").first().value == "value2"
    # Assert the total count of StringODM objects in MongoDB
    assert StringODM.objects.count() == 2
    # Assert the total count of keys in odm_ids
    assert len(sync_strings.odm_ids) == 2


def test_sync_string_odm_with_redis_added_start_redis_notactive_mongo_active(
    config, redis_handler
):
    """
    init
    key NOT active mongo active
    mongo becomes NOT active
    key added mongo active
    """
    # Manually populate Redis with key-value pairs

    redis_handler.client.set("key2", "value2")

    # Manually populate MongoDB with StringODM objects
    StringODM(key="key1", value="value1", active_now=True).save()
    StringODM(key="key2", value="value2", active_now=True).save()

    # Initialize SyncStrings
    sync_strings = SyncStrings(config, redis_handler)
    key_types = redis_handler.get_all_key_types()
    sync_strings.init(key_types)

    # Check initial state of SyncStrings and MongoDB
    assert StringODM.objects(key="key1").first().id not in sync_strings.odm_ids
    assert sync_strings.odm_ids["key2"] == StringODM.objects(key="key2").first().id
    assert StringODM.objects(key="key1").first().value == None
    assert StringODM.objects(key="key2").first().value == "value2"
    # Assert the total count of StringODM objects in MongoDB
    assert StringODM.objects.count() == 2
    # Assert the total count of keys in odm_ids
    assert len(sync_strings.odm_ids) == 1

    redis_handler.client.set("key1", "value3")
    key_types = redis_handler.get_all_key_types()
    sync_strings.sync(key_types)

    # Check MongoDB for changes after adding key1 back
    assert StringODM.objects(key="key1").first().active_now == True
    assert StringODM.objects(key="key1").first().value == "value3"
    assert StringODM.objects(key="key2").first().active_now == True
    assert StringODM.objects(key="key2").first().value == "value2"
    # Assert the total count of StringODM objects in MongoDB
    assert StringODM.objects.count() == 2
    # Assert the total count of keys in odm_ids
    assert len(sync_strings.odm_ids) == 2


def test_sync_string_odm_with_redis_deleted_start_redis_active_mongo_active(
    config, redis_handler
):
    """
    init
    key active mongo active
    key empty mongo not active
    key added mongo active
    """
    # Manually populate Redis with key-value pairs
    redis_handler.client.set("key1", "value1")
    redis_handler.client.set("key2", "value2")

    # Manually populate MongoDB with StringODM objects
    StringODM(key="key1", value="value1", active_now=True).save()
    StringODM(key="key2", value="value2", active_now=True).save()

    # Initialize SyncStrings
    sync_strings = SyncStrings(config, redis_handler)
    key_types = redis_handler.get_all_key_types()
    sync_strings.init(key_types)

    # Check initial state of SyncStrings and MongoDB
    assert sync_strings.odm_ids["key1"] == StringODM.objects(key="key1").first().id
    assert sync_strings.odm_ids["key2"] == StringODM.objects(key="key2").first().id
    assert StringODM.objects(key="key1").first().value == "value1"
    assert StringODM.objects(key="key2").first().value == "value2"
    # Assert the total count of StringODM objects in MongoDB
    assert StringODM.objects.count() == 2
    # Assert the total count of keys in odm_ids
    assert len(sync_strings.odm_ids) == 2

    redis_handler.client.delete("key1")
    key_types = redis_handler.get_all_key_types()
    sync_strings.sync(key_types)

    # Check MongoDB for changes after sync
    assert StringODM.objects(key="key1").first().active_now == False
    assert StringODM.objects(key="key1").first().value == None
    assert StringODM.objects(key="key2").first().active_now == True
    assert StringODM.objects(key="key2").first().value == "value2"
    # Assert that only one non-None object is left in odm_ids and MongoDB
    assert len(sync_strings.odm_ids) == 1
    non_none_objects = []
    for id, odm_obj in sync_strings.odm_ids.items():
        if StringODM.objects(id=odm_obj).first().value is not None:
            non_none_objects.append(id)
    assert len(non_none_objects) == 1

    # Add key1 back and check the mongo state
    redis_handler.client.set("key1", "value3")
    key_types = redis_handler.get_all_key_types()
    sync_strings.sync(key_types)

    # Check MongoDB for changes after adding key1 back
    assert StringODM.objects(key="key1").first().active_now == True
    assert StringODM.objects(key="key1").first().value == "value3"
    assert StringODM.objects(key="key2").first().active_now == True
    assert StringODM.objects(key="key2").first().value == "value2"
    # Assert the total count of StringODM objects in MongoDB
    assert StringODM.objects.count() == 2
    # Assert the total count of keys in odm_ids
    assert len(sync_strings.odm_ids) == 2


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
    StringODM(key="key1", active_now=True).save()
    StringODM(key="key2", active_now=True).save()
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
