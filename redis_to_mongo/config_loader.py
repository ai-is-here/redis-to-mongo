import json
import os
from logger import logger


class Config:
    def __init__(self, config_file: str = "config.json"):
        """
        Initialize the Config class and load the configuration.
        """
        self.config = self.load_config(config_file)

    def load_config(self, config_file: str) -> dict[str, str | None]:
        """
        Load the configuration from the environment variables and the config file.
        """
        config = {
            "mongo_db_name": os.getenv("MONGO_DB_NAME"),
            "mongo_username": os.getenv("MONGO_USERNAME"),
            "mongo_password": os.getenv("MONGO_PASSWORD"),
            "mongo_host": os.getenv("MONGO_HOST"),
            "redis_host": os.getenv("REDIS_HOST"),
            "redis_port": os.getenv("REDIS_PORT"),
            "sync_interval_sec": os.getenv("SYNC_INTERVAL_SEC"),
            "messages_per_stream": os.getenv("MESSAGES_PER_STREAM"),
        }

        try:
            with open(config_file, "r") as f:
                file_config = json.load(f)
        except FileNotFoundError:
            msg = "Config file not found. Please ensure the config file exists."
            logger.error(msg)
            raise FileNotFoundError(msg)

        for key in config:
            if config[key] is None:
                config[key] = file_config.get(key)

        return config
