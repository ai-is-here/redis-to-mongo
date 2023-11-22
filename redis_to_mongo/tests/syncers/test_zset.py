import pytest

from redis_to_mongo.tests.conftest import redis_populate_all, data_dict, mongo_handler
from redis_to_mongo.syncers import SyncZSets
from redis_to_mongo.mongo_models import ZSetODM


@pytest.fixture
def sync_zsets_fixture(config, mongo_handler, redis_handler):
    return SyncZSets(config, redis_handler)


def test_sync(sync_zsets_fixture, redis_populate_zsets, data_dict):
    sync_zsets_fixture.init({"key1": "zset"})
    updates = sync_zsets_fixture._sync()
    assert updates == {}
    sync_zsets_fixture.redis_handler.client.zadd(
        "key1", {"new_value1": 1, "new_value2": 2}
    )
    sync_zsets_fixture.sync({"key1": "zset"})
    odm_id = sync_zsets_fixture.odm_ids["key1"]
    assert ZSetODM.objects(id=odm_id).first().values == [
        {"key": "new_value1", "score": 1.0},
        {"key": "new_value2", "score": 2.0},
    ]


def test_empty_zset(sync_zsets_fixture, data_dict):
    sync_zsets_fixture.init({"key1": "zset"})
    sync_zsets_fixture.sync({"key1": "zset"})
    odm_id = sync_zsets_fixture.odm_ids["key1"]
    assert ZSetODM.objects(id=odm_id).first().values == []
    sync_zsets_fixture.redis_handler.client.delete("key1")
    sync_zsets_fixture.sync({"key1": "zset"})
    assert ZSetODM.objects(id=odm_id).first().values == []


def test_add_item(sync_zsets_fixture, data_dict):
    sync_zsets_fixture.init({"key1": "zset"})
    sync_zsets_fixture.sync({"key1": "zset"})
    odm_id = sync_zsets_fixture.odm_ids["key1"]
    assert ZSetODM.objects(id=odm_id).first().values == []
    sync_zsets_fixture.redis_handler.client.zadd(
        "key1", {"new_value1": 1, "new_value2": 2}
    )
    sync_zsets_fixture.sync({"key1": "zset"})
    assert ZSetODM.objects(id=odm_id).first().values == [
        {"key": "new_value1", "score": 1.0},
        {"key": "new_value2", "score": 2.0},
    ]
    sync_zsets_fixture.redis_handler.client.zadd("key1", {"new_value3": 1.5})
    sync_zsets_fixture.sync({"key1": "zset"})
    assert ZSetODM.objects(id=odm_id).first().values == [
        {"key": "new_value1", "score": 1.0},
        {"key": "new_value3", "score": 1.5},
        {"key": "new_value2", "score": 2.0},
    ]


def test_remove_item(sync_zsets_fixture, data_dict):
    sync_zsets_fixture.init({"key1": "zset"})
    sync_zsets_fixture.redis_handler.client.zadd(
        "key1", {"new_value1": 1, "new_value2": 2}
    )
    sync_zsets_fixture.sync({"key1": "zset"})
    odm_id = sync_zsets_fixture.odm_ids["key1"]
    assert ZSetODM.objects(id=odm_id).first().values == [
        {"key": "new_value1", "score": 1.0},
        {"key": "new_value2", "score": 2.0},
    ]
    sync_zsets_fixture.redis_handler.client.zrem("key1", "new_value1")
    sync_zsets_fixture.sync({"key1": "zset"})
    assert ZSetODM.objects(id=odm_id).first().values == [
        {"key": "new_value2", "score": 2}
    ]


def test_remove_all_items_one_by_one(sync_zsets_fixture, data_dict):
    sync_zsets_fixture.init({"key1": "zset"})
    sync_zsets_fixture.redis_handler.client.zadd(
        "key1", {"new_value1": 1, "new_value2": 2}
    )
    sync_zsets_fixture.sync({"key1": "zset"})
    odm_id = sync_zsets_fixture.odm_ids["key1"]
    assert ZSetODM.objects(id=odm_id).first().values == [
        {"key": "new_value1", "score": 1.0},
        {"key": "new_value2", "score": 2.0},
    ]
    sync_zsets_fixture.redis_handler.client.zrem("key1", "new_value1")
    sync_zsets_fixture.sync({"key1": "zset"})
    assert ZSetODM.objects(id=odm_id).first().values == [
        {"key": "new_value2", "score": 2}
    ]
    sync_zsets_fixture.redis_handler.client.zrem("key1", "new_value2")
    sync_zsets_fixture.sync({"key1": "zset"})
    assert ZSetODM.objects(id=odm_id).first().values == []
