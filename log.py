import logging

def setup_custom_logger(name, level='INFO'):
    formatter = logging.Formatter(fmt='%(levelname)-7s - %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.getLevelName(level))
    logger.addHandler(handler)
    return logger
