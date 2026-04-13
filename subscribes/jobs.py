"""Dagster job для обновления подписок."""

from dagster import job

from .ops import subscribes_op


@job(description="Сервисное обновление подписок, заведений и получателей отчетов.")
def subscribes_job():
    """Запускает обновление подписок и заведений."""
    subscribes_op()
