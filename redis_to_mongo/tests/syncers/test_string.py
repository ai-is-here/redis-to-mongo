import pytest

from redis_to_mongo.tests.conftest import redis_populate_all, data_dict, mongo_handler
from redis_to_mongo.syncers import SyncStrings
from redis_to_mongo.mongo_models import StringODM
from pymongo import UpdateOne


@pytest.fixture
def sync_strings_fixture(config, mongo_handler, redis_handler):
    return SyncStrings(config, redis_handler)


def test_sync(sync_strings_fixture, redis_populate_string, data_dict):
    sync_strings_fixture.init({"key1": "string"})
    updates = sync_strings_fixture._sync()

    assert updates == {}

    sync_strings_fixture.redis_handler.client.set("key1", "new_value")
    updates = sync_strings_fixture._sync()
    odm_id = sync_strings_fixture.odm_ids["key1"]
    assert updates == {odm_id: {"value": "new_value"}}


def test_full_cycle(sync_strings_fixture, data_dict):
    # Step 1: Initialize the sync_strings_fixture with the defined key types
    key_types = {"key1": "string", "key2": "list"}
    sync_strings_fixture.init(key_types)

    # Step 2: Check initial state of the sync_strings_fixture
    assert "key1" in sync_strings_fixture.odm_ids
    assert len(sync_strings_fixture.odm_ids) == 1
    assert StringODM.objects(key="key1").first() is not None
    assert StringODM.objects(key="key1").first().value == None

    # Step 3: Update the value of "key1" in the redis client and sync
    sync_strings_fixture.redis_handler.client.set("key1", "string_test_value0")
    sync_strings_fixture.sync(key_types)
    assert StringODM.objects(key="key1").first().value == "string_test_value0"

    # Step 4: Update the value of "key1" again in the redis client and sync
    sync_strings_fixture.redis_handler.client.set("key1", "new_value")
    sync_strings_fixture.sync(key_types)
    assert StringODM.objects(key="key1").first().value == "new_value"

    # Step 5: Delete "key1" from the redis client, set a new value, and sync without it
    sync_strings_fixture.redis_handler.client.delete("key1")
    sync_strings_fixture.redis_handler.client.set("key1", "new_value_INVISIBLE")
    sync_strings_fixture.sync({"key2": "list"})
    assert StringODM.objects(key="key1").first().value == "new_value"

    # Step 6: Sync again with the original keys and check the value of "key1"
    sync_strings_fixture.sync({"key1": "string", "key2": "list"})
    assert StringODM.objects(key="key1").first().value == "new_value_INVISIBLE"
