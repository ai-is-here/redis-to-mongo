import pytest

from redis_to_mongo.tests.conftest import redis_populate_all, data_dict, mongo_handler
from redis_to_mongo.syncers import SyncLists
from redis_to_mongo.mongo_models import ListODM
from pymongo import UpdateOne


@pytest.fixture
def sync_lists_fixture(config, mongo_handler, redis_handler):
    return SyncLists(config, redis_handler)


def test_sync(sync_lists_fixture, redis_populate_list, data_dict):
    sync_lists_fixture.init({"key1": "list"})
    updates = sync_lists_fixture._sync()
    assert updates == {}
    sync_lists_fixture.redis_handler.client.rpush("key1", "new_value")
    sync_lists_fixture.sync({"key1": "list"})
    odm_id = sync_lists_fixture.odm_ids["key1"]
    assert ListODM.objects(id=odm_id).first().values == ["new_value"]


def test_empty_list(sync_lists_fixture, data_dict):
    sync_lists_fixture.init({"key1": "list"})
    sync_lists_fixture.sync({"key1": "list"})
    odm_id = sync_lists_fixture.odm_ids["key1"]
    assert ListODM.objects(id=odm_id).first().values == []
    sync_lists_fixture.redis_handler.client.delete("key1")
    sync_lists_fixture.sync({"key1": "list"})
    assert ListODM.objects(id=odm_id).first().values == []


def test_reorder_list(sync_lists_fixture, data_dict):
    sync_lists_fixture.init({"key1": "list"})
    sync_lists_fixture.sync({"key1": "list"})
    odm_id = sync_lists_fixture.odm_ids["key1"]
    assert ListODM.objects(id=odm_id).first().values == []
    sync_lists_fixture.redis_handler.client.rpush("key1", "new_value")
    sync_lists_fixture.sync({"key1": "list"})
    assert ListODM.objects(id=odm_id).first().values == ["new_value"]
    sync_lists_fixture.redis_handler.client.rpush("key1", "another_value")
    sync_lists_fixture.sync({"key1": "list"})
    assert ListODM.objects(id=odm_id).first().values == ["new_value", "another_value"]
    # Set the whole array at once in Redis
    sync_lists_fixture.redis_handler.client.delete("key1")
    sync_lists_fixture.redis_handler.client.rpush("key1", "another_value", "new_value")
    sync_lists_fixture.sync({"key1": "list"})
    # Check if the list in MongoDB matches the new array
    assert ListODM.objects(id=odm_id).first().values == ["another_value", "new_value"]


def test_remove_item(sync_lists_fixture, data_dict):
    sync_lists_fixture.init({"key1": "list"})
    sync_lists_fixture.redis_handler.client.rpush("key1", "new_value")
    sync_lists_fixture.sync({"key1": "list"})
    odm_id = sync_lists_fixture.odm_ids["key1"]
    assert ListODM.objects(id=odm_id).first().values == ["new_value"]
    sync_lists_fixture.redis_handler.client.lrem("key1", 1, "new_value")
    sync_lists_fixture.sync({"key1": "list"})
    assert ListODM.objects(id=odm_id).first().values == []


def test_change_item(sync_lists_fixture, data_dict):
    sync_lists_fixture.init({"key1": "list"})
    sync_lists_fixture.redis_handler.client.rpush("key1", "new_value")
    sync_lists_fixture.sync({"key1": "list"})
    odm_id = sync_lists_fixture.odm_ids["key1"]
    assert ListODM.objects(id=odm_id).first().values == ["new_value"]
    sync_lists_fixture.redis_handler.client.lset("key1", 0, "changed_value")
    sync_lists_fixture.sync({"key1": "list"})
    assert ListODM.objects(id=odm_id).first().values == ["changed_value"]


def test_change_one_among_many(sync_lists_fixture, data_dict):
    sync_lists_fixture.init({"key1": "list"})
    sync_lists_fixture.redis_handler.client.rpush("key1", "value1", "value2", "value3")
    sync_lists_fixture.sync({"key1": "list"})
    odm_id = sync_lists_fixture.odm_ids["key1"]
    assert ListODM.objects(id=odm_id).first().values == ["value1", "value2", "value3"]
    sync_lists_fixture.redis_handler.client.lset("key1", 1, "changed_value")
    sync_lists_fixture.sync({"key1": "list"})
    assert ListODM.objects(id=odm_id).first().values == [
        "value1",
        "changed_value",
        "value3",
    ]
