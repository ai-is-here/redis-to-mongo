import time
from collections import defaultdict

from config_loader import Config
from logger import logger
from mongo_models import Stream, StreamMessage
from redis_api import RedisHandler

from redis_to_mongo.mongo_api import MongoHandler


STREAM_NAME_INFO_PART = ":info:"


class SyncEngine:
    """
    The SyncEngine class is responsible for synchronizing data between Redis and MongoDB.
    It uses the RedisHandler to interact with Redis and the Mongo API to interact with MongoDB.
    """

    def __init__(self, config: Config):
        self.config = config
        self.redis_handler = RedisHandler(
            host=config.redis_host, port=config.redis_port
        )
        self.mongo_handler = MongoHandler(config)
        self.init_streams()

    def init_streams(self) -> None:
        """
        Initialize the streams dictionary with all existing streams in MongoDB.
        """
        self.streams = {}
        for stream in Stream.objects().all():
            self.streams[stream.name] = stream

    def sync_keys_structure(self) -> dict[str, list[str]]:
        """
        Synchronize the structure of keys between Redis and MongoDB.
        """
        sync_keys = self.redis_handler.get_sync_keys()
        for stream in sync_keys["streams"]:
            if stream not in self.streams:
                # Create new MetaStream and update self.streams
                new_stream_odm = Stream(name=stream).save()
                self.streams[stream] = new_stream_odm
        return sync_keys

    def sync(self) -> None:
        """
        Synchronize data between Redis and MongoDB.
        """
        sync_keys = self.sync_keys_structure()
        last_ids = {name: odm.last_redis_read_id for name, odm in self.streams.items()}
        messages = self.redis_handler.read_messages(
            last_ids,
            count=self.config.messages_per_stream,
        )
        for stream_name, message_list in messages.items():
            try:
                if message_list:
                    self.send_stream_messages_to_mongo(stream_name, messages)
            except Exception as e:
                logger.error(f"Error processing stream {stream_name}: {e}")
        self.update_sets_in_mongo(sync_keys["sets"])

    def send_stream_messages_to_mongo(self, stream_name: str, messages: dict) -> None:
        """
        Send stream messages from Redis to MongoDB.
        """
        try:
            stream_type = stream_name.split(STREAM_NAME_INFO_PART)
            if len(stream_type) < 1:
                stream_type = "main"
            else:
                stream_type = stream_type[-1]
            for message in messages:
                new_message = StreamMessage(
                    meta_stream=self.streams[stream_name],
                    content=dict(message),
                    type=stream_type,
                )
                new_message.save()
            self.streams[stream_name].update(
                last_redis_read_id=messages[-1]["id"],
            )
        except Exception as e:
            logger.error(f"Error updating MongoDB for stream {stream_name}: {e}")

    def update_sets_in_mongo(self, sets: list[str]) -> None:
        """
        Update sets in MongoDB with data from Redis.
        """
        streams_with_sets = defaultdict(list)
        for name in sets:
            stream_name = name.split(STREAM_NAME_INFO_PART)[0]
            streams_with_sets[stream_name].append(name)

        for stream, metadata_sets in streams_with_sets.items():
            stream_odm = self.streams[stream]
            try:
                for set_name in metadata_sets:
                    stream_odm.metadata_sets[set_name] = self.redis_handler.get_set(
                        set_name
                    )
                stream_odm.save()
            except Exception as e:
                logger.error(
                    f"Error updating set data in MongoDB for stream {stream_odm.name}: {e}"
                )

    def run(self) -> None:
        """
        Run the SyncEngine indefinitely, synchronizing data between Redis and MongoDB.
        """
        start_time = time.time()
        logger.info("SyncEngine started at: %s", start_time)
        uptime = 0
        while True:
            round_start_time = time.time()
            self.sync()
            # Add logic for processing sets and metadata streams if necessary
            round_elapsed_time = time.time() - round_start_time
            logger.info(f"Round took: {round_elapsed_time} seconds")
            elapsed_time = time.time() - start_time
            uptime += elapsed_time
            logger.info(f"SyncEngine has been running for: {uptime} seconds")
            sleep_time = max(0, self.config.sync_interval_sec - round_elapsed_time)
            logger.info(
                f"Sleeping for: {sleep_time} seconds out of {self.config.sync_interval_sec} max value"
            )
            time.sleep(sleep_time)
            start_time = time.time()
