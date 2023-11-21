import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR) + "/"
TEST_CONFIG_ENV = PROJECT_ROOT + ".env.test"
PROD_CONFIG_ENV = PROJECT_ROOT + ".env.prod"
