from typing import Any, cast
import redis
from redis_to_mongo.logger import logger
from redis_to_mongo.config_loader import RedisConfig


class RedisHandler:
    DB_NUMBER = 0
    _instance = None

    def __new__(cls, config: RedisConfig):
        if cls._instance is None:
            cls._instance = super(RedisHandler, cls).__new__(cls)
            cls._instance.client = cls._instance._initialize_redis_client(config)
            cls._instance.config = config
        return cls._instance

    def _initialize_redis_client(self, config: RedisConfig) -> redis.Redis:
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
        block: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, str]]]]:
        try:
            messages = self.client.xread(  # type: ignore
                last_read_ids,
                count=self.config.config["messages_per_stream"],
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
            dict_list = list(map(lambda t: {"key": t[0], "score": t[1]}, set_values))
            return dict_list
        except Exception as e:
            logger.error(f"Error retrieving values from set {set_name}: {str(e)}")
            raise e

    def get_set(self, set_name: str) -> list[str]:
        """
        Returns all values from the given ordered set.
        """
        try:
            set_values = self.client.smembers(set_name)  # type: ignore
            logger.debug(f"Retrieved all values from set {set_name}: {set_values}")
            as_list = list(set_values)
            return cast(list[str], as_list)
        except Exception as e:
            logger.error(f"Error retrieving values from set {set_name}: {str(e)}")
            raise e

    def get_all_key_types(self) -> dict[str, str]:
        all_keys = self.get_all_keys()
        key_types = self.get_types(all_keys)
        return key_types

    def get_json(self, key: str) -> dict[Any, Any]:
        """
        Returns the value of the given key as a JSON object.
        """
        try:
            json_value = self.client.json().get(key)  # type: ignore
            logger.debug(f"Retrieved JSON value for key {key}: {json_value}")
            return json_value
        except Exception as e:
            logger.error(f"Error retrieving JSON value for key {key}: {str(e)}")
            raise e

    def get_list(self, key: str) -> list:
        """
        Returns all values from the list stored at the given key.
        """
        try:
            list_values = self.client.lrange(key, 0, -1)  # type: ignore
            logger.debug(f"Retrieved all values from list {key}: {list_values}")
            return list_values
        except Exception as e:
            logger.error(f"Error retrieving values from list {key}: {str(e)}")
            raise e

    def get_string(self, key: str) -> str:
        """
        Returns the string value of the given key.
        """
        try:
            string_value = self.client.get(key)  # type: ignore
            logger.debug(f"Retrieved string value for key {key}: {string_value}")
            return string_value
        except Exception as e:
            logger.error(f"Error retrieving string value for key {key}: {str(e)}")
            raise e

    def get_all_keys(self) -> list[str]:
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
