from utils.logger import get_logger
logger = get_logger(__name__)


class EndpointException(Exception):
    def __init__(self, endpoint,  original, message, payload={}):
        self.original = original

        logger.error(endpoint)
        logger.error(payload)
        logger.error(message)

        super(EndpointException, self).__init__(message)
