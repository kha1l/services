"""Dagster-ресурс для работы с БД."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Any

from dagster import BoolSource, Field, InitResourceContext, StringSource, resource

from config import database_settings
from database.engine import build_engine, build_sessionmaker


@dataclass(frozen=True)
class DatabaseResource:
    """Обертка над настройками БД и фабриками сессий."""

    url: str
    echo: bool
    pool_pre_ping: bool

    @cached_property
    def engine(self):
        return build_engine(self.url, echo=self.echo, pool_pre_ping=self.pool_pre_ping)

    @cached_property
    def sessionmaker(self):
        return build_sessionmaker(self.engine)

    def get_sessionmaker(self):
        return self.sessionmaker


@resource(
    config_schema={
        "url": Field(StringSource, is_required=False),
        "echo": Field(BoolSource, is_required=False),
        "pool_pre_ping": Field(BoolSource, is_required=False),
    }
)
def db_resource(context: InitResourceContext) -> DatabaseResource:
    """Возвращает ресурс БД с возможностью override через config."""
    cfg = database_settings()
    overrides: dict[str, Any] = context.resource_config or {}
    return DatabaseResource(
        url=overrides.get("url", cfg.url),
        echo=overrides.get("echo", cfg.echo),
        pool_pre_ping=overrides.get("pool_pre_ping", cfg.pool_pre_ping),
    )
