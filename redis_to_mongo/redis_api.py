from typing import Any, cast
import redis
from redis_to_mongo.logger import logger
from redis_to_mongo.config_loader import Config


class RedisHandler:
    DB_NUMBER = 0
    _instance = None

    def __new__(cls, config: Config):
        if cls._instance is None:
            cls._instance = super(RedisHandler, cls).__new__(cls)
            cls._instance.client = cls._instance._initialize_redis_client(config)
        return cls._instance

    def _initialize_redis_client(self, config: Config) -> redis.Redis:
        try:
            client: redis.Redis = redis.Redis(
                host=config.config["redis_host"],
                port=config.config["redis_port"],
                db=self.DB_NUMBER,
                decode_responses=True,
            )
            logger.debug(
                f"Redis client initialized with host={config.config['redis_host']}, port={config.config['redis_port']}, db={self.DB_NUMBER}"
            )
            return client
        except Exception as e:
            logger.error(f"Error initializing Redis client: {str(e)}")
            raise e

    def read_messages(
        self,
        last_read_ids: dict[str, str],
        count: int = 10,
        block: int = 0,
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

    def get_ordered_set(self, set_name: str) -> list[dict[str, Any]]:
        """
        Returns all values from the given ordered set.
        """
        try:
            set_values = self.client.zrange(set_name, 0, -1, withscores=True)  # type: ignore
            logger.debug(f"Retrieved all values from set {set_name}: {set_values}")
            return cast(list[dict[str, Any]], set_values)
        except Exception as e:
            logger.error(f"Error retrieving values from set {set_name}: {str(e)}")
            raise e

    def get_set(self, set_name: str) -> list[str]:
        """
        Returns all values from the given ordered set.
        """
        try:
            set_values = self.client.sget(set_name)  # type: ignore
            logger.debug(f"Retrieved all values from set {set_name}: {set_values}")
            return cast(list[str], list(set_values))
        except Exception as e:
            logger.error(f"Error retrieving values from set {set_name}: {str(e)}")
            raise e

    def get_sync_keys(self) -> list[str]:
        """
        Efficiently structures Redis keys into streams and sets.
        Sorts keys by length to process main streams first.
        """
        try:
            all_keys = sorted(
                list(self.client.scan_iter(match="*")), key=len
            )  # Sort by string length
            return cast(list[str], all_keys)
        except Exception as e:
            logger.error(f"Error retrieving stream structure: {str(e)}")
            raise e

    def get_types(self, keys: list[str]) -> dict[str, str]:
        """
        Returns a dictionary with the Redis type for each key in the provided list.
        """
        try:
            key_types = {key: self.client.type(key) for key in keys}  # type: ignore
            logger.debug(f"Retrieved types for keys: {key_types}")
            return key_types
        except Exception as e:
            logger.error(f"Error retrieving types for keys: {str(e)}")
            raise e
