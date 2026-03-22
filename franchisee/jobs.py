"""Dagster job для обновления франшиз."""

from dagster import job

from .ops import franchisee_op


@job
def franchisee_job():
    """Запускает обновление справочника франшиз."""
    franchisee_op()
