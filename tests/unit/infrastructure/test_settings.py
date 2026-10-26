import pytest

from order_events.infrastructure.settings import ConfigurationError, Settings


def test_defaults_describe_the_local_compose_setup():
    settings = Settings.from_env({})
    assert settings.bootstrap_servers == "localhost:9092"
    assert (settings.topic, settings.group_id, settings.partitions) == ("order-events", "order-history", 6)
    assert settings.log_level == "INFO"


def test_every_variable_can_be_overridden():
    settings = Settings.from_env(
        {
            "BOOTSTRAP_SERVERS": "a:1,b:2",
            "TOPIC": "t",
            "GROUP_ID": "g",
            "PARTITIONS": "3",
            "LOG_LEVEL": "debug",
            "KAFKA_REQUEST_TIMEOUT_MS": "5000",
            "KAFKA_RETRY_BACKOFF_MS": "50",
        }
    )
    assert (settings.bootstrap_servers, settings.topic, settings.group_id, settings.partitions) == (
        "a:1,b:2",
        "t",
        "g",
        3,
    )
    assert (settings.log_level, settings.request_timeout_ms, settings.retry_backoff_ms) == ("DEBUG", 5000, 50)


@pytest.mark.parametrize(
    "environ",
    [
        {"BOOTSTRAP_SERVERS": "no-port"},
        {"BOOTSTRAP_SERVERS": ""},
        {"TOPIC": "  "},
        {"PARTITIONS": "0"},
        {"PARTITIONS": "six"},
        {"LOG_LEVEL": "LOUD"},
        {"KAFKA_REQUEST_TIMEOUT_MS": "-1"},
    ],
)
def test_an_unusable_value_is_rejected_with_the_variable_name(environ):
    with pytest.raises(ConfigurationError, match=next(iter(environ))):
        Settings.from_env(environ)


def test_settings_read_the_process_environment_by_default(monkeypatch):
    monkeypatch.setenv("TOPIC", "from-the-process")
    assert Settings.from_env().topic == "from-the-process"
