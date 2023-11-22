import pytest

from redis_to_mongo.tests.conftest import redis_populate_all, data_dict, mongo_handler
from redis_to_mongo.syncers import SyncSets
from redis_to_mongo.mongo_models import SetODM


@pytest.fixture
def sync_sets_fixture(config, mongo_handler, redis_handler):
    return SyncSets(config, redis_handler)


def test_sync(sync_sets_fixture, redis_populate_set, data_dict):
    sync_sets_fixture.init({"key1": "set"})
    updates = sync_sets_fixture._sync()
    assert updates == {}
    sync_sets_fixture.redis_handler.client.sadd("key1", "new_value1", "new_value2")
    sync_sets_fixture.sync({"key1": "set"})
    odm_id = sync_sets_fixture.odm_ids["key1"]
    assert sorted(SetODM.objects(id=odm_id).first().values) == [
        "new_value1",
        "new_value2",
    ]


def test_empty_set(sync_sets_fixture, data_dict):
    sync_sets_fixture.init({"key1": "set"})
    sync_sets_fixture.sync({"key1": "set"})
    odm_id = sync_sets_fixture.odm_ids["key1"]
    assert SetODM.objects(id=odm_id).first().values == []
    sync_sets_fixture.redis_handler.client.delete("key1")
    sync_sets_fixture.sync({"key1": "set"})
    assert SetODM.objects(id=odm_id).first().values == []


def test_add_item(sync_sets_fixture, data_dict):
    sync_sets_fixture.init({"key1": "set"})
    sync_sets_fixture.sync({"key1": "set"})
    odm_id = sync_sets_fixture.odm_ids["key1"]
    assert SetODM.objects(id=odm_id).first().values == []
    sync_sets_fixture.redis_handler.client.sadd("key1", "new_value1", "new_value2")
    sync_sets_fixture.sync({"key1": "set"})
    assert sorted(SetODM.objects(id=odm_id).first().values) == [
        "new_value1",
        "new_value2",
    ]


def test_remove_item(sync_sets_fixture, data_dict):
    sync_sets_fixture.init({"key1": "set"})
    sync_sets_fixture.redis_handler.client.sadd("key1", "new_value1", "new_value2")
    sync_sets_fixture.sync({"key1": "set"})
    odm_id = sync_sets_fixture.odm_ids["key1"]
    assert sorted(SetODM.objects(id=odm_id).first().values) == [
        "new_value1",
        "new_value2",
    ]
    sync_sets_fixture.redis_handler.client.srem("key1", "new_value1")
    sync_sets_fixture.sync({"key1": "set"})
    assert SetODM.objects(id=odm_id).first().values == ["new_value2"]


def test_remove_all_items_one_by_one(sync_sets_fixture, data_dict):
    sync_sets_fixture.init({"key1": "set"})
    sync_sets_fixture.redis_handler.client.sadd("key1", "new_value1", "new_value2")
    sync_sets_fixture.sync({"key1": "set"})
    odm_id = sync_sets_fixture.odm_ids["key1"]
    assert sorted(SetODM.objects(id=odm_id).first().values) == [
        "new_value1",
        "new_value2",
    ]
    sync_sets_fixture.redis_handler.client.srem("key1", "new_value1")
    sync_sets_fixture.sync({"key1": "set"})
    assert sorted(SetODM.objects(id=odm_id).first().values) == ["new_value2"]
    sync_sets_fixture.redis_handler.client.srem("key1", "new_value2")
    sync_sets_fixture.sync({"key1": "set"})
    assert SetODM.objects(id=odm_id).first().values == []
