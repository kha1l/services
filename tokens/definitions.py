"""Модульные Definitions для обновления токенов."""

from dagster import Definitions

from shared.definition_resources import COMMON_RESOURCE_DEFS

from .jobs import tokens_job


defs = Definitions(jobs=[tokens_job], resources=COMMON_RESOURCE_DEFS)
definitions = defs
