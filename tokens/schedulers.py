"""Расписание для job обновления токенов."""

from dagster import ScheduleDefinition

from config import schedule_config

from .jobs import tokens_job


_cfg = schedule_config("tokens", default_cron="0 */6 * * *")

tokens_schedule = None
if _cfg.enabled:
    tokens_schedule = ScheduleDefinition(
        job=tokens_job,
        cron_schedule=_cfg.cron,
        execution_timezone=_cfg.timezone,
    )
