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
            "mongo_db_name": ("MONGO_DB_NAME", str),
            "mongo_username": ("MONGO_DB_USER", str),
            "mongo_password": ("MONGO_DB_PASSWORD", str),
            "mongo_host": ("MONGO_HOST", str),
            "redis_host": ("REDIS_HOST", str),
            "redis_port": ("REDIS_PORT", int),
            "sync_interval_sec": ("SYNC_INTERVAL_SEC", int),
            "messages_per_stream": ("MESSAGES_PER_STREAM", int),
        }

        # Load configuration from file and environment variables
        config = {}
        for key, (env_var, expected_type) in config_vars.items():
            config_value = file_config.get(env_var, os.getenv(env_var))
            if config_value is not None:
                try:
                    config[key] = expected_type(config_value)
                except ValueError as e:
                    msg = f"Invalid type for {key}: {config_value}. Expected {expected_type.__name__}."
                    logger.error(msg)
                    raise ValueError(msg) from e
            else:
                msg = f"Configuration for {key} not found. Please ensure it is set in the environment or the .env file."
                logger.error(msg)
                raise ValueError(msg)

        return config
