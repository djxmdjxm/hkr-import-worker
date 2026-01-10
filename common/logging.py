# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.


import logging
from logging import getLogger


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    blue = "\x1b[34;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    str_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + str_format + reset,
        logging.INFO: blue + str_format + reset,
        logging.WARNING: yellow + str_format + reset,
        logging.ERROR: red + str_format + reset,
        logging.CRITICAL: bold_red + str_format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter  =logging.Formatter(log_fmt)

        return formatter.format(record)


stream_handler = logging.StreamHandler()
stream_handler.setFormatter(CustomFormatter())

logging.basicConfig(level=logging.DEBUG, handlers=[stream_handler])
