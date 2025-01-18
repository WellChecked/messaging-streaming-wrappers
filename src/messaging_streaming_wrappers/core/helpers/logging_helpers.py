import logging
import sys
import os

from logging.handlers import TimedRotatingFileHandler

LOG_FORMAT = logging.Formatter(
    os.environ.get('LOG_RECORD_FORMAT', '[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] — %(message)s'),
    os.environ.get('LOG_DATE_FORMAT', '%Y-%m-%d %H:%M:%S')
)
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')
LOG_FILE = os.environ.get('LOG_FILE', None)
logging.basicConfig(level=LOG_LEVEL)


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(LOG_FORMAT)
    return console_handler


def get_file_handler():
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
    file_handler.setFormatter(LOG_FORMAT)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)

    logger.setLevel(LOG_LEVEL)  # better to have too much log than not enough
    logger.addHandler(get_console_handler())
    if LOG_FILE:
        logger.addHandler(get_file_handler())

    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False
    return logger
