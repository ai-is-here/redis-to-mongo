import pytest
from redis_to_mongo.tests.conftest import NUMBER_OF_CONTAINERS, NUMBER_OF_ITEMS
from redis_to_mongo.mongo_models import *


def test_redis_all_iterations(
    sync_engine, mongo_handler, data_dict, redis_populate_all
):
    """
    Test the synchronization of all data types from Redis to MongoDB.
    This test populates Redis with all data types, calls the sync method of the SyncEngine,
    and then checks that all corresponding ODMs in MongoDB are in the correct state.
    """
    # Call the sync method to synchronize data from Redis to MongoDB
    sync_engine.sync()

    # Retrieve all ODMs from MongoDB and assert their content
    # Assert that the JSON data has been correctly synchronized
    for json_key in data_dict["jsons"]:
        json_odm = JSONODM.objects(key=json_key).first()
        assert json_odm, f"JSONODM with key {json_key} should exist"
        # Perform additional checks on json_odm.value if necessary
        expected_value = {"json_test_key": f"json_test_value{NUMBER_OF_ITEMS - 1}"}
        assert (
            json_odm.value == expected_value
        ), f"JSONODM value mismatch for key {json_key}"

    # Assert that the String data has been correctly synchronized
    for string_key in data_dict["strings"]:
        string_odm = StringODM.objects(key=string_key).first()
        assert string_odm, f"StringODM with key {string_key} should exist"
        # Perform additional checks on string_odm.value if necessary
        expected_value = f"string_test_value{NUMBER_OF_ITEMS - 1}"
        assert (
            string_odm.value == expected_value
        ), f"StringODM value mismatch for key {string_key}"

    # Assert that the List data has been correctly synchronized
    for list_key in data_dict["lists"]:
        list_odm = ListODM.objects(key=list_key).first()
        assert list_odm, f"ListODM with key {list_key} should exist"
        # Perform additional checks on list_odm.values if necessary
        expected_values = [f"list_test_member{j}" for j in range(NUMBER_OF_ITEMS)]
        assert (
            list_odm.values == expected_values
        ), f"ListODM values mismatch for key {list_key}"

    # Assert that the ZSet data has been correctly synchronized
    for zset_key in data_dict["zsets"]:
        zset_odm = ZSetODM.objects(key=zset_key).first()
        assert zset_odm, f"ZSetODM with key {zset_key} should exist"
        # Perform additional checks on zset_odm.values if necessary
        expected_values = [
            {"key": f"zset_test_member{j}", "score": j} for j in range(NUMBER_OF_ITEMS)
        ]
        assert (
            zset_odm.values == expected_values
        ), f"ZSetODM values mismatch for key {zset_key}"

    # Assert that the Set data has been correctly synchronized
    for set_key in data_dict["sets"]:
        set_odm = SetODM.objects(key=set_key).first()
        assert set_odm, f"SetODM with key {set_key} should exist"
        # Perform additional checks on set_odm.values if necessary
        expected_values = {f"set_test_member{j}" for j in range(NUMBER_OF_ITEMS)}
        assert (
            set(set_odm.values) == expected_values
        ), f"SetODM values mismatch for key {set_key}"

    # Assert that the Stream data has been correctly synchronized
    for stream_key in data_dict["streams"]:
        stream_odm = StreamODM.objects(key=stream_key).first()
        assert stream_odm, f"StreamODM with key {stream_key} should exist"
        # Perform additional checks on stream_odm.last_redis_read_id if necessary
        # Check StreamMessageODM related to the stream
        stream_messages = StreamMessageODM.objects(stream=stream_odm)
        assert (
            stream_messages.count() == NUMBER_OF_ITEMS
        ), f"Incorrect number of messages found for stream {stream_key}"
        # Perform additional checks on the content of stream_messages if necessary
        expected_messages = [
            {"message": f"main_test_message{j}"} for j in range(NUMBER_OF_ITEMS)
        ]
        actual_messages = list(stream_messages.order_by("rid").values_list("content"))
        assert (
            actual_messages == expected_messages
        ), f"StreamMessageODM content mismatch for stream {stream_key}"
