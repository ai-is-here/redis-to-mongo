import os
from dotenv import dotenv_values


class BaseConfig:
    def __init__(self, config_file: str = ".env"):
        self.config = self.load_config(config_file)

    def load_config(self, config_file: str):
        return dotenv_values(config_file)

    def type_check_and_map(self, config_vars):
        config = {}
        for key, (env_var, expected_type) in config_vars.items():
            config_value = self.config.get(env_var, os.getenv(env_var))
            if config_value is not None:
                try:
                    config[key] = expected_type(config_value)
                except ValueError as e:
                    msg = f"Invalid type for {key}: {config_value}. Expected {expected_type.__name__}."
                    raise ValueError(msg) from e
            else:
                msg = f"Configuration for {key} not found. Please ensure it is set in the environment or the .env file."
                raise ValueError(msg)
        return config


class MongoConfig(BaseConfig):
    def __init__(self, config_file: str = ".env"):
        super().__init__(config_file)
        self.config_vars = {
            "mongo_db_name": ("MONGO_DB_NAME", str),
            "mongo_username": ("MONGO_DB_USER", str),
            "mongo_password": ("MONGO_DB_PASSWORD", str),
            "mongo_host": ("MONGO_HOST", str),
            "mongo_port": ("MONGO_PORT", int),
        }
        self.config = self.type_check_and_map(self.config_vars)
