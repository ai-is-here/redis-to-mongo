import pytest
from redis_to_mongo.mongo_models import StringODM, StreamODM


@pytest.fixture
def string_odm_instance():
    # Create a StringODM instance for testing
    instance = StringODM(key="test_key", value="test_value", active_now=True)
    instance.save()
    return instance


def test_update_active_now_no_save(string_odm_instance):
    # Test the update_active_now_no_save method
    string_odm_instance.update_active_now_no_save(False)
    assert string_odm_instance.active_now is False
    # Ensure the change is not saved to the database
    assert StringODM.objects(key="test_key").first().active_now is True


def test_reset_fields_to_default_no_save(string_odm_instance):
    # Test the reset_fields_to_default_no_save method
    string_odm_instance.value = "new_value"
    string_odm_instance.reset_fields_to_default_no_save()
    assert string_odm_instance.value is None
    # Ensure the change is not saved to the database
    assert StringODM.objects(key="test_key").first().value == "test_value"


def test_stream_odm_activity_history_initial_true():
    # Create StreamODM with active_now True
    stream_odm = StreamODM(
        key="test_stream_true", last_redis_read_id="0-0", active_now=True
    )
    stream_odm.save()
    assert stream_odm.active_now is True
    assert stream_odm.last_redis_read_id == "0-0"
    assert len(stream_odm.activity_history) == 0

    # Set active_now to False
    stream_odm.update_active_now_no_save(False)
    assert stream_odm.active_now is False
    assert len(stream_odm.activity_history) == 1
    assert stream_odm.activity_history[-1]["active_now"] is False

    # Set active_now to True
    stream_odm.update_active_now_no_save(True)
    assert stream_odm.active_now is True
    assert len(stream_odm.activity_history) == 2
    assert stream_odm.activity_history[-1]["active_now"] is True

    # Set active_now to False again
    stream_odm.update_active_now_no_save(False)
    assert stream_odm.active_now is False
    assert len(stream_odm.activity_history) == 3
    assert stream_odm.activity_history[-1]["active_now"] is False


def test_stream_odm_activity_history_initial_false():
    # Create StreamODM with active_now False
    stream_odm_false = StreamODM(
        key="test_stream_false", last_redis_read_id="0-0", active_now=False
    )
    stream_odm_false.save()
    assert stream_odm_false.active_now is False
    assert len(stream_odm_false.activity_history) == 0

    # Set active_now to True
    stream_odm_false.update_active_now_no_save(True)
    assert stream_odm_false.active_now is True
    assert len(stream_odm_false.activity_history) == 1
    assert stream_odm_false.activity_history[-1]["active_now"] is True

    # Set active_now to False
    stream_odm_false.update_active_now_no_save(False)
    assert stream_odm_false.active_now is False
    assert len(stream_odm_false.activity_history) == 2
    assert stream_odm_false.activity_history[-1]["active_now"] is False


def test_stream_odm_activity_history_multiple_updates():
    # Create StreamODM with active_now True
    stream_odm_multi = StreamODM(
        key="test_stream_multi", last_redis_read_id="0-0", active_now=True
    )
    stream_odm_multi.save()
    assert stream_odm_multi.active_now is True
    assert len(stream_odm_multi.activity_history) == 0

    # Set active_now to True (no change)
    stream_odm_multi.update_active_now_no_save(True)
    assert stream_odm_multi.active_now is True
    assert (
        len(stream_odm_multi.activity_history) == 0
    )  # No change should not append to history

    # Set active_now to False
    stream_odm_multi.update_active_now_no_save(False)
    assert stream_odm_multi.active_now is False
    assert len(stream_odm_multi.activity_history) == 1
    assert stream_odm_multi.activity_history[-1]["active_now"] is False

    # Set active_now to False again (no change)
    stream_odm_multi.update_active_now_no_save(False)
    assert stream_odm_multi.active_now is False
    assert (
        len(stream_odm_multi.activity_history) == 1
    )  # No change should not append to history

    # Set active_now to True
    stream_odm_multi.update_active_now_no_save(True)
    assert stream_odm_multi.active_now is True
    assert len(stream_odm_multi.activity_history) == 2
    assert stream_odm_multi.activity_history[-1]["active_now"] is True
