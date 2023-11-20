import pytest
from unittest.mock import patch
from redis_to_mongo.redis_api import RedisHandler


@pytest.fixture
def mock_redis_singleton_get_instance():
    with patch("redis_to_mongo.redis_api.RedisSingleton.get_instance") as mock:
        yield mock


def test_redis_handler_init(mock_redis_singleton_get_instance):
    mock_redis_singleton_get_instance.return_value = "REDIS"
    redis_handler = RedisHandler()
    mock_redis_singleton_get_instance.assert_called_once_with("localhost", 6379, db=0)
    assert redis_handler.client == mock_redis_singleton_get_instance.return_value


@patch("redis.StrictRedis.xread")
def test_read_messages(mock_xread, mock_redis_singleton_get_instance):
    # Arrange
    mock_client = mock_redis_singleton_get_instance.return_value
    mock_client.xread = mock_xread
    redis_handler = RedisHandler()
    last_read_ids = {"stream1": "0", "stream2": "0"}
    count = 10
    mock_xread.return_value = [
        [
            "stream1",
            [(str(i + 1), {"content": f"message{i + 1}"}) for i in range(count)],
        ],
        [
            "stream2",
            [(str(i + 11), {"content": f"message{i + 11}"}) for i in range(count)],
        ],
    ]

    # Act
    result = redis_handler.read_messages(last_read_ids, count)
    print(result)
    # Assert
    assert isinstance(result, dict)
    assert len(result) == 2
    assert len(result["stream1"]) == count
    assert len(result["stream2"]) == count
    for i in range(count):
        assert result["stream1"][i] == (str(i + 1), {"content": f"message{i + 1}"})
        assert result["stream2"][i] == (str(i + 11), {"content": f"message{i + 11}"})
    mock_xread.assert_called_once_with(last_read_ids, count=count, block=1)


@patch("redis.StrictRedis.zrange")
def test_get_set(mock_zrange, mock_redis_singleton_get_instance):
    # Arrange
    mock_client = mock_redis_singleton_get_instance.return_value
    mock_client.zrange = mock_zrange
    redis_handler = RedisHandler()
    set_name = "test_set"
    mock_zrange.return_value = [f"item{i}" for i in range(10)]

    # Act
    result = redis_handler.get_set(set_name)

    # Assert
    assert isinstance(result, list)
    assert len(result) == 10
    for i in range(10):
        assert result[i] == f"item{i}"
    mock_zrange.assert_called_once_with(set_name, 0, -1, withscores=False)


@patch("redis.StrictRedis.scan_iter")
def test_get_sync_keys(mock_scan_iter, mock_redis_singleton_get_instance):
    # Arrange
    mock_client = mock_redis_singleton_get_instance.return_value
    mock_client.scan_iter = mock_scan_iter
    redis_handler = RedisHandler()
    mock_scan_iter.return_value = (
        [f"public:streams:stream{i}" for i in range(5)]
        + [f"public:streams:stream{i}:info:events" for i in range(5)]
        + [f"public:streams:stream{i}:info:online:ZSET" for i in range(5)]
        + [f"public:streams:stream{i}:info:participants:ZSET" for i in range(5)]
    )

    # Act
    result = redis_handler.get_sync_keys()

    # Assert
    assert isinstance(result, dict)
    assert len(result) == 2
    assert len(result["streams"]) == 10
    assert len(result["sets"]) == 10
    for i in range(5):
        assert result["streams"][i] == f"public:streams:stream{i}"
        assert result["streams"][i + 5] == f"public:streams:stream{i}:info:events"
        assert result["sets"][i] == f"public:streams:stream{i}:info:online:ZSET"
        assert (
            result["sets"][i + 5] == f"public:streams:stream{i}:info:participants:ZSET"
        )
    mock_scan_iter.assert_called_once_with(match="*")
