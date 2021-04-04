import logging
import sys

from pathlib import Path
# Verify if system is macOS
foldername = Path("./logs")
foldername.mkdir(exist_ok=True)

FILENAME = "log.log"
PATH = foldername / FILENAME
FORMAT = "%(asctime)s [%(name)-12s] [%(levelname)-5.5s]  %(message)s"
DEFAULT_LEVEL = logging.INFO 

logFormatter = logging.Formatter(FORMAT)
logging.basicConfig(stream=sys.stderr, format=FORMAT)


def get_logger(name, path=PATH, level=DEFAULT_LEVEL):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    filename = path
    fileHandler = logging.FileHandler(filename, mode='a')
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    return logger
