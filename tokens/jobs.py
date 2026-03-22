"""Dagster job для обновления токенов."""

from dagster import job

from .ops import tokens_op


@job
def tokens_job():
    """Запускает обновление токенов."""
    tokens_op()
