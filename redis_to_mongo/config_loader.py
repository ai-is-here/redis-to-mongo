from redis_to_mongo.redis_to_mongo_mongo_modules.config_loader import BaseConfig


class RedisConfig(BaseConfig):
    def __init__(self, config_file: str = ".env"):
        super().__init__(config_file)
        self.config_vars = {
            "redis_host": ("REDIS_HOST", str),
            "redis_port": ("REDIS_PORT", int),
            "messages_per_stream": ("MESSAGES_PER_STREAM", int),
        }
        self.config = self.type_check_and_map(self.config_vars)


class SyncerConfig(BaseConfig):
    def __init__(self, config_file: str = ".env"):
        super().__init__(config_file)
        self.config_vars = {
            "sync_interval_sec": ("SYNC_INTERVAL_SEC", int),
        }
        self.config = self.type_check_and_map(self.config_vars)
