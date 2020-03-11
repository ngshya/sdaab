import os 

from sdaab.utils.get_config import get_config


def test_utils_get_config():

    config = get_config()
    assert config["ENV"] == "TESTING"

    del os.environ["ENV_RUN"]
    config = get_config()
    assert os.environ["ENV_RUN"] == "DEVELOPMENT"
    assert config["ENV"] == "DEVELOPMENT"
    assert config["PING"] == "PONG"

    config = get_config(path_env_yaml="config/testing.yml")
    assert config["ENV"] == "TESTING"
    assert config["PING"] == "PONG"

    config = get_config(path_env_yaml="pincopallo")
    assert config["ENV"] == "DEFAULT"
    assert config["PING"] == "PONG"

    os.environ["ENV_RUN"] = "TESTING"
