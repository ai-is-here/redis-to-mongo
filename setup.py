from setuptools import setup, find_packages

setup(
    name="redis_to_mongo",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "mongoengine",
        "redis",
    ],
)
