import pytest
from redis_to_mongo.syncers import *


def test_sync_jsons_with_empty_key_types(config, redis_handler):
    key_types = {}
    syncer = SyncJSONs(config, redis_handler)
    syncer.init(key_types)
    syncer.sync(key_types)
    assert syncer.changes_processed == 0


def test_sync_lists_with_empty_key_types(config, redis_handler):
    key_types = {}
    syncer = SyncLists(config, redis_handler)
    syncer.init(key_types)
    syncer.sync(key_types)
    assert syncer.changes_processed == 0


def test_sync_sets_with_empty_key_types(config, redis_handler):
    key_types = {}
    syncer = SyncSets(config, redis_handler)
    syncer.init(key_types)
    syncer.sync(key_types)
    assert syncer.changes_processed == 0


def test_sync_streams_with_empty_key_types(config, redis_handler):
    key_types = {}
    syncer = SyncStreams(config, redis_handler)
    syncer.init(key_types)
    syncer.sync(key_types)
    assert syncer.changes_processed == 0


def test_sync_strings_with_empty_key_types(config, redis_handler):
    key_types = {}
    syncer = SyncStrings(config, redis_handler)
    syncer.init(key_types)
    syncer.sync(key_types)
    assert syncer.changes_processed == 0


def test_sync_zsets_with_empty_key_types(config, redis_handler):
    key_types = {}
    syncer = SyncZSets(config, redis_handler)
    syncer.init(key_types)
    syncer.sync(key_types)
    assert syncer.changes_processed == 0
