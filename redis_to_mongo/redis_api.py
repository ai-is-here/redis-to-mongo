import redis
from logger import logger
from typing import Any, cast


class RedisSingleton:
    _instance: redis.Redis | None = None

    @staticmethod
    def get_instance(
        host: str = "localhost", port: int = 6379, db: int = 0
    ) -> redis.Redis:
        if RedisSingleton._instance is None:
            RedisSingleton(host=host, port=port, db=db)
        return RedisSingleton._instance  # type: ignore

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        if RedisSingleton._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            try:
                self.client: redis.Redis = redis.Redis(
                    host=host, port=port, db=db, decode_responses=True
                )
                logger.debug(
                    f"Redis client initialized with host={host}, port={port}, db={db}"
                )
            except Exception as e:
                logger.error(f"Error initializing Redis client: {str(e)}")
                raise e
            RedisSingleton._instance = self.client


class RedisHandler:
    DB_NUMBER = 0
    SET_MARKER = ":SET"

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0) -> None:
        self.client = RedisSingleton.get_instance(host, port, db=self.DB_NUMBER)

    def read_messages(
        self,
        last_read_ids: dict[str, str],
        count: int = 10,
        block: int = 1,
    ) -> dict[str, list[tuple[str, dict[str, str]]]]:
        try:
            messages = self.client.xread(  # type: ignore
                last_read_ids,
                count=count,
                block=block,
            )
            messages = cast(list[Any], messages)
            logger.debug(f"Read {len(messages)} messages from streams {last_read_ids}")
            for stream_data in messages:
                stream_name, message_list = stream_data[0], stream_data[1]  # type: ignore
                latest_message_id = message_list[-1][0]
                last_read_ids[stream_name] = latest_message_id
            return dict(messages)
        except Exception as e:
            logger.error(
                f"Error reading messages from streams {last_read_ids}: {str(e)}"
            )
            raise e

    def get_set(self, set_name: str) -> list[str]:
        """
        Returns all values from the given set.
        """
        try:
            set_values = self.client.zrange(set_name, 0, -1, withscores=False)  # type: ignore
            logger.debug(f"Retrieved all values from set {set_name}: {set_values}")
            return cast(list[str], set_values)
        except Exception as e:
            logger.error(f"Error retrieving values from set {set_name}: {str(e)}")
            raise e

    def get_sync_keys(self) -> dict[str, Any]:
        """
        Efficiently structures Redis keys into streams and sets.
        Sorts keys by length to process main streams first.
        """
        try:
            all_keys = sorted(
                list(self.client.scan_iter(match="*")), key=len
            )  # Sort by string length
            all_keys = cast(list[str], all_keys)
            sets = []
            streams = []

            for key in all_keys:
                if key.endswith(self.SET_MARKER):
                    # It's a set related to the main stream
                    sets.append(key)
                else:
                    # It's a metadata stream related to the main stream
                    streams.append(key)

            logger.debug(
                f"Stream structure retrieved: streams: {streams} and sets: {sets}"
            )
            return {"streams": streams, "sets": sets}
        except Exception as e:
            logger.error(f"Error retrieving stream structure: {str(e)}")
            raise e
