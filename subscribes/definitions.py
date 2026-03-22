"""Модульные Definitions для обновления подписок."""

from dagster import Definitions

from shared.definition_resources import COMMON_RESOURCE_DEFS

from .jobs import subscribes_job


defs = Definitions(jobs=[subscribes_job], resources=COMMON_RESOURCE_DEFS)
definitions = defs
