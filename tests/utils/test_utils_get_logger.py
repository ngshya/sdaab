from sdaab.utils.get_logger import get_logger


def test_utils_get_logger():
    logger = get_logger(str_logger_name="logger")
    assert logger.name == "logger"
    logger = get_logger(str_logger_name="logger_name", yaml_logging="ABC")
    assert logger.name == "logger_name"
