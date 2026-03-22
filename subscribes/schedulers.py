"""Расписание для job обновления подписок."""

from dagster import ScheduleDefinition

from config import schedule_config

from .jobs import subscribes_job


_cfg = schedule_config("subscribes", default_cron="0 4 * * *")

subscribes_schedule = None
if _cfg.enabled:
    subscribes_schedule = ScheduleDefinition(
        job=subscribes_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )
