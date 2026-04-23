"""Расписание очистки временных WebApp-отчетов."""

from dagster import ScheduleDefinition

from config import schedule_config

from .jobs import webapp_report_cleanup_job


_cfg = schedule_config("webapp_report_cleanup", default_cron="*/15 * * * *")

webapp_report_cleanup_schedule = None
if _cfg.enabled:
    webapp_report_cleanup_schedule = ScheduleDefinition(
        job=webapp_report_cleanup_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )
