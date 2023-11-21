import pytest

from redis_to_mongo.tests.conftest import redis_populate_all, data_dict, mongo_handler
from redis_to_mongo.syncers import SyncJSONs
from redis_to_mongo.mongo_models import JSONODM
from pymongo import UpdateOne


@pytest.fixture
def sync_jsons_fixture(config, mongo_handler, redis_handler):
    return SyncJSONs(config, redis_handler)


def test_sync(sync_jsons_fixture, redis_populate_json, data_dict):
    sync_jsons_fixture.init({"key1": "ReJSON-RL"})
    updates = sync_jsons_fixture._sync()

    assert updates == {}

    sync_jsons_fixture.redis_handler.client.json().set(
        "key1", ".", {"new_key": "new_value"}
    )
    updates = sync_jsons_fixture._sync()
    odm_id = sync_jsons_fixture.odm_ids["key1"]
    assert updates == {odm_id: {"value": {"new_key": "new_value"}}}


def test_full_cycle(sync_jsons_fixture, data_dict):
    # Step 1: Initialize the sync_jsons_fixture with the defined key types
    key_types = {"key1": "ReJSON-RL", "key2": "list"}
    sync_jsons_fixture.init(key_types)

    # Step 2: Check initial state of the sync_jsons_fixture
    assert "key1" in sync_jsons_fixture.odm_ids
    assert len(sync_jsons_fixture.odm_ids) == 1
    assert JSONODM.objects(key="key1").first() is not None
    assert JSONODM.objects(key="key1").first().value == None

    # Step 3: Update the value of "key1" in the redis client and sync
    sync_jsons_fixture.redis_handler.client.json().set(
        "key1", ".", {"json_test_key": "json_test_value0"}
    )
    sync_jsons_fixture.sync(key_types)
    assert JSONODM.objects(key="key1").first().value == {
        "json_test_key": "json_test_value0"
    }

    # Step 4: Update the value of "key1" again in the redis client and sync
    sync_jsons_fixture.redis_handler.client.json().set(
        "key1", ".", {"json_test_key": "new_value"}
    )
    sync_jsons_fixture.sync(key_types)
    assert JSONODM.objects(key="key1").first().value == {"json_test_key": "new_value"}


def test_complex_nested_json_with_deletion(sync_jsons_fixture, data_dict):
    # Step 1: Initialize the sync_jsons_fixture with the defined key types
    key_types = {"key1": "ReJSON-RL", "key2": "list"}
    sync_jsons_fixture.init(key_types)

    # Step 2: Check initial state of the sync_jsons_fixture
    assert "key1" in sync_jsons_fixture.odm_ids
    assert len(sync_jsons_fixture.odm_ids) == 1
    assert JSONODM.objects(key="key1").first() is not None
    assert JSONODM.objects(key="key1").first().value == None

    # Step 3: Update the value of "key1" in the redis client with a complex nested JSON and sync
    complex_json = {
        "json_test_key": "json_test_value0",
        "nested_json": {"nested_key1": "nested_value1", "nested_key2": "nested_value2"},
    }
    sync_jsons_fixture.redis_handler.client.json().set("key1", ".", complex_json)
    sync_jsons_fixture.sync(key_types)
    assert JSONODM.objects(key="key1").first().value == complex_json

    # Step 4: Update the value of "key1" again in the redis client with a new complex nested JSON where new values appear and some other disappear and sync
    updated_complex_json = {
        "json_test_key": "new_value",
        "nested_json": {
            "nested_key1": "new_nested_value1",
            "new_nested_key": "new_nested_value",
        },
    }
    sync_jsons_fixture.redis_handler.client.json().set(
        "key1", ".", updated_complex_json
    )
    sync_jsons_fixture.sync(key_types)
    assert JSONODM.objects(key="key1").first().value == updated_complex_json

    # Step 5: Delete some top-level and nested values in the JSON and sync
    deleted_values_json = {"nested_json": {"nested_key1": "new_nested_value1"}}
    sync_jsons_fixture.redis_handler.client.json().set("key1", ".", deleted_values_json)
    sync_jsons_fixture.sync(key_types)
    assert JSONODM.objects(key="key1").first().value == deleted_values_json
