import pytest

from redis_to_mongo.tests.conftest import (
    redis_populate_main_streams,
    redis_handler,
    data_dict,
)
from redis_to_mongo.mongo_models import StreamODM, StreamMessageODM
from redis_to_mongo.syncers.sync_stream import SyncStreams


@pytest.fixture
def sync_streams_fixture(config, mongo_handler, redis_handler):
    return SyncStreams(config, redis_handler)


def test_init(sync_streams_fixture, redis_populate_main_streams, data_dict):
    key_types = {stream: "stream" for stream in data_dict["streams"]}
    sync_streams_fixture.init(key_types)
    for stream in data_dict["streams"]:
        assert stream in sync_streams_fixture.odm_ids
        assert StreamODM.objects(key=stream).first() is not None
        assert sync_streams_fixture.last_read_ids[stream] == "0-0"
        assert StreamODM.objects(key=stream).first().last_redis_read_id == "0-0"


def test_init(sync_streams_fixture, redis_populate_main_streams, data_dict):
    key_types = {stream: "stream" for stream in data_dict["streams"]}

    sync_streams_fixture.init(key_types)
    for stream in data_dict["streams"]:
        assert stream in sync_streams_fixture.odm_ids
        assert StreamODM.objects(key=stream).first() is not None
        assert sync_streams_fixture.last_read_ids[stream] == "0-0"
        assert StreamODM.objects(key=stream).first().last_redis_read_id == "0-0"


def test_reinit_with_modified_last_read(
    sync_streams_fixture, redis_populate_main_streams, data_dict
):
    key_types = {stream: "stream" for stream in data_dict["streams"]}
    sync_streams_fixture.init(key_types)
    # Modify one of the last read ids in mongo
    modified_stream = data_dict["streams"][0]
    modified_last_read_id = "1-1"
    StreamODM.objects(key=modified_stream).update_one(
        set__last_redis_read_id=modified_last_read_id
    )
    # Reinitialize
    sync_streams_fixture.init(key_types)
    # Check that all last read ids are as expected in both sync_streams and mongo
    for stream in data_dict["streams"]:
        expected_last_read_id = (
            modified_last_read_id if stream == modified_stream else "0-0"
        )
        assert sync_streams_fixture.last_read_ids[stream] == expected_last_read_id
        assert (
            StreamODM.objects(key=stream).first().last_redis_read_id
            == expected_last_read_id
        )


def test_no_mongo_objects_sync_with_messages(
    sync_streams_fixture, redis_populate_main_streams, data_dict
):
    # Initialize the sync streams fixture
    key_types = {stream: "stream" for stream in data_dict["streams"]}
    sync_streams_fixture.init(key_types)

    # Check that StreamODM objects are created with the expected initial state
    for stream in data_dict["streams"]:
        stream_odm = StreamODM.objects(key=stream).first()
        assert stream_odm is not None
        assert stream_odm.last_redis_read_id == "0-0"

    # Ensure no StreamMessageODM objects exist yet
    assert StreamMessageODM.objects.count() == 0

    # Perform the sync operation
    sync_streams_fixture.sync(key_types)

    # Verify that last read ids were updated in StreamODM objects
    for stream in data_dict["streams"]:
        stream_odm = StreamODM.objects(key=stream).first()
        last_read_id = sync_streams_fixture.last_read_ids[stream]
        assert stream_odm.last_redis_read_id == last_read_id

    # Verify that all messages from Redis streams are now in StreamMessageODM collection
    # and that the last_redis_read_id corresponds to the last message's ID from Redis
    for stream in data_dict["streams"]:
        redis_messages = sync_streams_fixture.redis_handler.client.xrange(stream)
        stream_odm = StreamODM.objects(key=stream).first()
        mongo_messages = StreamMessageODM.objects(stream=stream_odm)
        # Check if there are any messages, if so, verify the last read ID
        if redis_messages:
            last_redis_message_id = redis_messages[-1][
                0
            ]  # Get the ID of the last message
            assert sync_streams_fixture.last_read_ids[stream] == last_redis_message_id

        assert len(redis_messages) == mongo_messages.count()
        mongo_messages = mongo_messages.order_by("rid")
        for redis_message, mongo_message in zip(redis_messages, mongo_messages):
            assert redis_message[0] == mongo_message.rid
            assert redis_message[1] == mongo_message.content


def test_existing_mongo_objects_sync_with_messages(
    sync_streams_fixture, redis_populate_main_streams, redis_handler, data_dict
):
    # Initialize and perform the first sync operation
    key_types = {stream: "stream" for stream in data_dict["streams"]}
    sync_streams_fixture.init(key_types)
    sync_streams_fixture.sync(key_types)

    # Add new messages to each Redis stream
    for i, stream in enumerate(data_dict["streams"]):
        for j in range(1, i + 2):  # Adding 1 message to first stream, 2 to second, etc.
            redis_handler.client.xadd(stream, {"message": f"new_test_message{j}"})

    # Perform the second sync operation
    sync_streams_fixture.sync(key_types)

    # Verify that MongoDB content matches the Redis streams after the second sync
    for stream in data_dict["streams"]:
        redis_messages = sync_streams_fixture.redis_handler.client.xrange(stream)
        stream_odm = StreamODM.objects(key=stream).first()
        mongo_messages = StreamMessageODM.objects(stream=stream_odm).order_by("rid")

        assert len(redis_messages) == mongo_messages.count()
        for redis_message, mongo_message in zip(redis_messages, mongo_messages):
            assert redis_message[0] == mongo_message.rid
            assert redis_message[1] == mongo_message.content


def test_sync_with_no_prepopulated_mongo(
    sync_streams_fixture, redis_handler, mongo_handler, data_dict
):
    # Ensure MongoDB is empty
    assert StreamODM.objects.count() == 0
    assert StreamMessageODM.objects.count() == 0

    # Initialize MongoDB
    sync_streams_fixture.init(redis_handler.get_all_key_types())

    # Ensure MongoDB is empty
    assert StreamODM.objects.count() == 0
    assert StreamMessageODM.objects.count() == 0
    # Use Redis handler to create a stream with a single message
    test_stream = "test:stream"
    test_message = {"message": "test_message"}
    redis_handler.client.xadd(test_stream, test_message)

    # Sync Redis to MongoDB
    sync_streams_fixture.sync({"test:stream": "stream"})

    # Check MongoDB for the presence of the new stream and message
    stream_odm = StreamODM.objects(key=test_stream).first()
    assert stream_odm is not None
    assert (
        stream_odm.last_redis_read_id
        == redis_handler.client.xinfo_stream(test_stream)["last-generated-id"]
    )
    mongo_messages = StreamMessageODM.objects(stream=stream_odm).order_by("rid")
    assert mongo_messages.count() == 1
    mongo_message = mongo_messages.first()
    assert mongo_message.content == test_message
    assert (
        mongo_message.rid
        == redis_handler.client.xinfo_stream(test_stream)["last-generated-id"]
    )


def test_sync_stream_with_redis_deletion_active_test(
    sync_streams_fixture, redis_handler, mongo_handler, data_dict
):
    # Populate Redis with a stream
    test_stream = "test:stream:deletion"
    test_message = {"message": "test_message_for_deletion"}
    redis_handler.client.xadd(test_stream, test_message)

    # Initialize MongoDB with the stream from Redis
    sync_streams_fixture.init(redis_handler.get_all_key_types())

    # Check MongoDB for the state of StreamODM after initial sync
    initial_stream_odm = StreamODM.objects(key=test_stream).first()
    assert initial_stream_odm is not None
    assert initial_stream_odm.active_now is True
    assert len(initial_stream_odm.activity_history) == 0

    # Delete the stream in Redis
    redis_handler.client.delete(test_stream)

    # Sync after deletion to reflect changes in MongoDB

    key_types = redis_handler.get_all_key_types()

    sync_streams_fixture.sync(key_types)

    # Check MongoDB for the state of StreamODM after deletion
    stream_odm = StreamODM.objects(key=test_stream).first()
    assert stream_odm is not None
    assert stream_odm.active_now is False
    assert len(stream_odm.activity_history) == 1
    assert stream_odm.activity_history[0]["active_now"] is False
