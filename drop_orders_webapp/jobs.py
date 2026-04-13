"""Dagster job для очистки временных WebApp-отчетов."""

from dagster import job

from .ops import webapp_report_cleanup_op


@job(description="Очищает временные результаты WebApp-отчетов Aida из БД.")
def webapp_report_cleanup_job():
    """Удаляет устаревшие записи из aida.webapp_report_results."""
    webapp_report_cleanup_op()
