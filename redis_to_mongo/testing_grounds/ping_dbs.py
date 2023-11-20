from redis_to_mongo.redis_api import RedisHandler
from redis_to_mongo.mongo_api import MongoHandler
from redis_to_mongo.config_loader import Config


if __name__ == "__main__":
    config = Config()
    redis = RedisHandler()
    mongo = MongoHandler(config)

    try:
        redis.client.ping()
        print("Redis client connected successfully.")
    except Exception as e:
        print(f"Error connecting to Redis client: {str(e)}")
    try:
        mongo.client.server_info()
        print("MongoDB client connected successfully.")
    except Exception as e:
        print(f"Error connecting to MongoDB client: {str(e)}")
