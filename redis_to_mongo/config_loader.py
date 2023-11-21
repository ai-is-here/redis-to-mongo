import json
import os
from redis_to_mongo.logger import logger


from dotenv import dotenv_values


class Config:
    def __init__(self, config_file: str = ".env"):
        """
        Initialize the Config class and load the configuration.
        """
        self.config = self.load_config(config_file)

    def load_config(self, config_file: str) -> dict[str, str | None]:
        """
        Load the configuration from the environment variables and the config file.
        """
        file_config = dotenv_values(config_file)
        config_vars = {
            "mongo_db_name": "MONGO_DB_NAME",
            "mongo_username": "MONGO_DB_USER",
            "mongo_password": "MONGO_DB_PASSWORD",
            "mongo_host": "MONGO_HOST",
            "redis_host": "REDIS_HOST",
            "redis_port": "REDIS_PORT",
            "sync_interval_sec": "SYNC_INTERVAL_SEC",
            "messages_per_stream": "MESSAGES_PER_STREAM",
        }

        # Load configuration from file and environment variables
        config = {}
        for key, value in config_vars.items():
            config_value = file_config.get(value, os.getenv(value))
            config[key] = config_value

            # Check if configuration value is None and raise error if it is
            if config_value is None:
                msg = f"Configuration for {key} not found. Please ensure it is set in the environment or the .env file."
                logger.error(msg)
                raise ValueError(msg)

        return config
