"""Корневой Dagster Definitions для `servises`."""

from dagster import Definitions

from franchisee.jobs import franchisee_job
from franchisee.schedulers import franchisee_schedule
from shared.definition_resources import COMMON_RESOURCE_DEFS
from subscribes.jobs import subscribes_job
from subscribes.schedulers import subscribes_schedule
from tokens.jobs import tokens_job
from tokens.schedulers import tokens_schedule


defs = Definitions(
    jobs=[
        franchisee_job,
        subscribes_job,
        tokens_job,
    ],
    schedules=[
        schedule
        for schedule in (
            franchisee_schedule,
            subscribes_schedule,
            tokens_schedule,
        )
        if schedule is not None
    ],
    resources=COMMON_RESOURCE_DEFS,
)
