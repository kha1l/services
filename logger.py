import logging


def get_logger(logger_name: str, file_mode: str = 'a'):
    loader_logger = logging.getLogger(logger_name)
    if not loader_logger.hasHandlers():
        loader_logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(f"../logs/{logger_name}.log", mode=file_mode)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(
            '%(filename)s:%(lineno)d #%(levelname)-8s '
            '[%(asctime)s] - %(name)s - %(message)s'
        ))
        loader_logger.addHandler(file_handler)
    return loader_logger
