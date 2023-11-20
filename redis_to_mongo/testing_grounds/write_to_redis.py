import time
import redis


def add_messages_to_stream(redis_client, stream_name, num_messages):
    for i in range(1, num_messages + 1):
        message = {"message": f"Message {i}"}
        redis_client.xadd(stream_name, message)


def add_items_to_set(redis_client, set_name, num_items):
    for i in range(1, num_items + 1):
        redis_client.zadd(set_name, {f"Item {i}": int(time.time() * 1000)}, nx=True)
        print(f"Added Item {i} to set {set_name}")


def main_write(redis_client):
    # Stream and set names
    stream_name = "public:streams:test_stream"
    set_name = "public:streams:test_stream:info:online:ZSET"

    # Add messages to stream
    add_messages_to_stream(redis_client, stream_name, 10)

    # Add items to set
    add_items_to_set(redis_client, set_name, 5)

    print("Data added to Redis stream and set successfully.")


def main_read(redis_client):
    # Stream and set names
    stream_name = "public:streams:test_stream"
    set_name = "public:streams:test_stream:info:online:ZSET"
    print(stream_name, set_name)
    # Get all values from the set

    set_values = redis_client.zrange(set_name, 0, -1, withscores=False)
    print(repr(set_values))

    # Read the stream

    stream_messages = redis_client.xread({stream_name: "0"}, count=1000, block=1)
    print(repr(stream_messages))


if __name__ == "__main__":
    # Connect to Redis
    redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    main_write(redis_client)
    main_read(redis_client)
