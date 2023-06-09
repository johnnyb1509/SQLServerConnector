
from loguru import logger
import sys
import os

def defineLoguru(level = "INFO"):
    logger.remove(0)
    fmt = "{time:YYYY-MM-DD <> HH:mm:ss.SSS} |-| Exec {function}(): {line} |-| {level} |-| {message}"
    logger.add(sys.stderr, level=level, format=fmt, catch = True)
    return logger