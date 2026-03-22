"""Расписание для job обновления франшиз."""

from dagster import ScheduleDefinition

from config import schedule_config

from .jobs import franchisee_job


_cfg = schedule_config("franchisee", default_cron="0 5 * * *")

franchisee_schedule = None
if _cfg.enabled:
    franchisee_schedule = ScheduleDefinition(
        job=franchisee_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )
