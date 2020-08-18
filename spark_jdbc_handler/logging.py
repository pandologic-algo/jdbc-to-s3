import logging


LOGGING_LEVEL = logging.INFO


def create_default_logger():
    logger = logging.getLogger(__name__.split('.')[0])

    logger.setLevel(LOGGING_LEVEL)

    # handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.getLevelName(LOGGING_LEVEL))

    # handler formater
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
    handler.setFormatter(formatter)

    # add handler
    logger.addHandler(handler)

    return logger