"""Общий файловый логгер проекта."""

from __future__ import annotations

import logging
from pathlib import Path


def get_logger(logger_name: str, file_mode: str = "a"):
    """Возвращает файловый логгер и переиспользует уже созданный handler."""
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)

    loader_logger = logging.getLogger(logger_name)
    if not loader_logger.handlers:
        # Все job пишут в отдельные файлы в `logs/`, чтобы было проще разбирать падения по сервисам.
        loader_logger.setLevel(logging.INFO)
        loader_logger.propagate = False

        file_handler = logging.FileHandler(logs_dir / f"{logger_name}.log", mode=file_mode)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(
            logging.Formatter(
                "%(filename)s:%(lineno)d #%(levelname)-8s "
                "[%(asctime)s] - %(name)s - %(message)s"
            )
        )
        loader_logger.addHandler(file_handler)
    return loader_logger
