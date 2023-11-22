import pytest
from redis_to_mongo.sync_engine import SyncEngine
from redis_to_mongo.mongo_api import MongoHandler
from redis_to_mongo.redis_api import RedisHandler
from redis_to_mongo.mongo_models import StreamODM, StreamMessageODM
from redis_to_mongo.config_loader import Config
from redis_to_mongo.constants import TEST_CONFIG_ENV

NUMBER_OF_CONTAINERS = 3
NUMBER_OF_ITEMS = 3


@pytest.fixture
def config():
    return Config(TEST_CONFIG_ENV)


@pytest.fixture
def sync_engine(config):
    # Initialize SyncEngine with test environment configuration
    return SyncEngine(config)


@pytest.fixture
def redis_handler(config):
    # Get a Redis instance
    return RedisHandler(config)


@pytest.fixture
def mongo_handler(config):
    # Get a Redis instance
    return MongoHandler(config)


@pytest.fixture(autouse=True)
def clear_dbs(redis_handler, mongo_handler):
    # not for parallel consumption haha and not for prod
    # Clear Redis and MongoDB databases before each test
    redis_handler.client.flushdb()
    mongo_handler.client.drop_database(mongo_handler.db.name)


@pytest.fixture
def data_dict():
    # Define data dict to populate with
    return {
        "streams": [f"public:streams:test{i}" for i in range(NUMBER_OF_CONTAINERS)],
        "sets": [
            f"public:streams:test{i}:info:someset" for i in range(NUMBER_OF_CONTAINERS)
        ],
        "zsets": [
            f"public:streams:test{i}:info:somezset" for i in range(NUMBER_OF_CONTAINERS)
        ],
        "lists": [
            f"public:streams:test{i}:somelist" for i in range(NUMBER_OF_CONTAINERS)
        ],
        "strings": [
            f"public:streams:test{i}:somestring" for i in range(NUMBER_OF_CONTAINERS)
        ],
        "jsons": [
            f"public:streams:test{i}:somejson" for i in range(NUMBER_OF_CONTAINERS)
        ],
    }


@pytest.fixture
def redis_populate_main_streams(redis_handler, data_dict):
    # Populate Redis with main streams
    for i, stream in enumerate(data_dict["streams"]):
        for j in range(NUMBER_OF_ITEMS):
            redis_handler.client.xadd(stream, {"message": f"main_test_message{j}"})


@pytest.fixture
def redis_populate_zsets(redis_handler, data_dict):
    for i, zset in enumerate(data_dict["zsets"]):
        for j in range(NUMBER_OF_ITEMS):
            redis_handler.client.zadd(zset, {f"zset_test_member{j}": j})


@pytest.fixture
def redis_populate_set(redis_handler, data_dict):
    # Populate Redis with unknown data type set
    for i, set_key in enumerate(data_dict["sets"]):
        for j in range(NUMBER_OF_ITEMS):
            redis_handler.client.sadd(set_key, f"set_test_member{j}")


@pytest.fixture
def redis_populate_list(redis_handler, data_dict):
    # Populate Redis with unknown data type list
    for i, list_key in enumerate(data_dict["lists"]):
        for j in range(NUMBER_OF_ITEMS):
            redis_handler.client.lpush(list_key, f"list_test_member{j}")


@pytest.fixture
def redis_populate_string(redis_handler, data_dict):
    # Populate Redis with unknown data type string
    for i, string_key in enumerate(data_dict["strings"]):
        for j in range(NUMBER_OF_ITEMS):
            redis_handler.client.set(string_key, f"string_test_value{j}")


@pytest.fixture
def redis_populate_json(redis_handler, data_dict):
    # Populate Redis with unknown data type json
    for i, json_key in enumerate(data_dict["jsons"]):
        for j in range(NUMBER_OF_ITEMS):
            redis_handler.client.json().set(
                json_key, ".", {"json_test_key": f"json_test_value{j}"}
            )


@pytest.fixture
def redis_populate_all(
    redis_populate_main_streams,
    redis_populate_zsets,
    redis_populate_set,
    redis_populate_list,
    redis_populate_string,
    redis_populate_json,
):
    pass


@pytest.fixture
def mongo_populate_ordered_set(mongo_handler, data_dict):
    # Populate MongoDB with OrderedSet objects
    for i, set_key in enumerate(data_dict["unk"]["sets"]):
        for j in range(NUMBER_OF_ITEMS):
            ordered_set = OrderedSet(key=set_key, values=[{f"zset_test_member{j}": j}])
            ordered_set.save()


@pytest.fixture
def mongo_populate_main_streams(mongo_handler, data_dict):
    # Populate MongoDB with Stream objects
    streams = []
    for i, stream_key in enumerate(data_dict["streams"]):
        stream = Stream(key=stream_key, last_redis_read_id="0")
        stream.save()
        streams.append(stream)
    return streams


@pytest.fixture
def mongo_populate_additional_streams(mongo_handler, data_dict):
    # Populate MongoDB with Stream objects
    streams = []
    for i, stream_key in enumerate(data_dict["additional_streams"]):
        stream = Stream(key=stream_key, last_redis_read_id="0")
        stream.save()
        streams.append(stream)
    return streams


@pytest.fixture
def mongo_populate_stream_message(mongo_handler, data_dict):
    # Populate MongoDB with StreamMessageODM objects
    for i, stream_key in enumerate(data_dict["streams"]):
        stream = Stream.objects.get(key=stream_key)
        for j in range(NUMBER_OF_ITEMS):
            stream_message = StreamMessageODM(
                stream=stream, content={"message": f"main_test_message{j}"}
            )
            stream_message.save()


@pytest.fixture
def mongo_populate_additional_stream_message(mongo_handler, data_dict):
    # Populate MongoDB with additional StreamMessageODM objects
    for i, stream_key in enumerate(data_dict["additional_streams"]):
        stream = Stream.objects.get(key=stream_key)
        for j in range(NUMBER_OF_ITEMS):
            stream_message = StreamMessageODM(
                stream=stream, content={"message": f"additional_test_message{j}"}
            )
            stream_message.save()
