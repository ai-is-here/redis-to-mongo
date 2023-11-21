import pytest
from redis_to_mongo.tests.conftest import redis_populate_main_streams


def test_redis_main_streams(redis_populate_main_streams, redis_handler):
    pass


@pytest.fixture
def test_init_streams(
    mongo_populate_stream, mongo_populate_additional_streams, redis_handler, data_dict
):
    sync_engine = SyncEngine(redis_handler)

    for stream_key in data_dict["streams"] + data_dict["additional_streams"]:
        assert stream_key in sync_engine.streams
        assert sync_engine.streams[stream_key].name == stream_key
        assert sync_engine.streams[stream_key].last_redis_read_id == "0"
