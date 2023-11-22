import pytest

from redis_to_mongo.tests.conftest import redis_handler, NUMBER_OF_ITEMS


def test_all(redis_populate_all):
    pass


def test_read_messages(redis_handler, redis_populate_main_streams, data_dict):
    last_read_ids = {stream: "0" for stream in data_dict["streams"]}
    messages = redis_handler.read_messages(last_read_ids)
    assert len(messages) == len(data_dict["streams"])
    for stream, message_list in messages.items():
        assert len(message_list) == NUMBER_OF_ITEMS
        for message in message_list:
            assert "main_test_message" in message[1]["message"]


def test_read_messages_with_last_read_ids(
    redis_handler, redis_populate_main_streams, data_dict
):
    last_read_ids = {stream: "0" for stream in data_dict["streams"]}
    # Read just one message
    messages = redis_handler.read_messages(last_read_ids, count=1)
    for stream in data_dict["streams"]:
        # Check if last read ids are updated
        assert last_read_ids[stream] != "0"
        # Check if only one message is read
        assert len(messages[stream]) == 1
        # Check the content of the message
        assert "main_test_message0" in messages[stream][0][1]["message"]
    # Read the rest of the messages
    messages = redis_handler.read_messages(last_read_ids)
    for stream, message_list in messages.items():
        # Check if all messages are read
        assert len(message_list) == NUMBER_OF_ITEMS - 1
        for i, message in enumerate(message_list, start=1):
            # Check the content of the messages
            assert f"main_test_message{i}" in message[1]["message"]


def test_get_ordered_set(redis_handler, redis_populate_zsets, data_dict):
    for zset in data_dict["zsets"]:
        set_values = redis_handler.get_ordered_set(zset)
        assert len(set_values) == NUMBER_OF_ITEMS
        for i, member in enumerate(set_values):
            assert f"zset_test_member{i}" == member["key"]
            assert isinstance(member["score"], float)


def test_get_set(redis_handler, redis_populate_set, data_dict):
    for set_key in data_dict["sets"]:
        set_values = redis_handler.get_set(set_key)
        assert len(set_values) == NUMBER_OF_ITEMS
        for member in set_values:
            assert "set_test_member" in member


def test_get_json(redis_handler, redis_populate_json, data_dict):
    for json_key in data_dict["jsons"]:
        json_value = redis_handler.get_json(json_key)
        assert "json_test_key" in json_value


def test_get_list(redis_handler, redis_populate_list, data_dict):
    for list_key in data_dict["lists"]:
        list_values = redis_handler.get_list(list_key)
        assert len(list_values) == NUMBER_OF_ITEMS
        for member in list_values:
            assert "list_test_member" in member


def test_get_string(redis_handler, redis_populate_string, data_dict):
    for string_key in data_dict["strings"]:
        string_value = redis_handler.get_string(string_key)
        assert "string_test_value" in string_value


@pytest.fixture
def all_keys_fixture(data_dict):
    all_keys = []
    for key_type in data_dict:
        all_keys.extend(data_dict[key_type])
    return all_keys


def test_get_all_keys(redis_handler, redis_populate_all, all_keys_fixture):
    all_keys = redis_handler.get_all_keys()
    assert len(all_keys) == len(all_keys_fixture)


def test_get_types(redis_handler, redis_populate_all, all_keys_fixture):
    all_keys = redis_handler.get_all_keys()
    key_types = redis_handler.get_types(all_keys)
    assert len(key_types) == len(all_keys_fixture)


def test_get_all_key_types(redis_handler, redis_populate_all, all_keys_fixture):
    key_types = redis_handler.get_all_key_types()
    assert len(key_types) == len(all_keys_fixture)
