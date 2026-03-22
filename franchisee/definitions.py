"""Модульные Definitions для обновления франшиз."""

from dagster import Definitions

from shared.definition_resources import COMMON_RESOURCE_DEFS

from .jobs import franchisee_job


defs = Definitions(jobs=[franchisee_job], resources=COMMON_RESOURCE_DEFS)
definitions = defs
