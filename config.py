"""Загрузка конфигурации `servises` из переменных окружения."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from environs import Env


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


@dataclass(frozen=True)
class Database:
    name: str
    user: str
    password: str
    host: str
    port: int
    driver: str


@dataclass(frozen=True)
class Config:
    dbase: Database


@dataclass(frozen=True)
class DatabaseSettings:
    """Нормализованные настройки БД для SQLAlchemy."""

    url: str
    echo: bool
    pool_pre_ping: bool


@dataclass(frozen=True)
class ScheduleConfig:
    """Конфигурация расписания для Dagster job."""

    name: str
    cron: str
    timezone: str
    enabled: bool


@dataclass(frozen=True)
class DodoApiSettings:
    """Конфигурация HTTP-клиента для Dodo API."""

    timeout_seconds: int
    retry_limit: int
    retry_delay_seconds: int
    user_agent: str
    trust_env: bool


@dataclass(frozen=True)
class RabbitMQSettings:
    """Настройки публикации в RabbitMQ."""

    url: str
    queue: str
    exchange: str
    routing_key: str
    exchange_type: str
    durable: bool
    enabled: bool


def load_config(path: str | None = None) -> Config:
    """Собирает обратноссовместимый Config из env."""
    env = Env()
    env.read_env(path)
    return Config(
        dbase=Database(
            name=env.str("DBASE_NAME"),
            user=env.str("DBASE_USER"),
            password=env.str("DBASE_PASSWORD"),
            host=env.str("DBASE_HOST"),
            port=env.int("DBASE_PORT", 5432),
            driver=env.str("DBASE_DRIVER", "postgresql"),
        )
    )


def _build_database_url_from_parts() -> str:
    cfg = load_config()
    port_part = f":{cfg.dbase.port}" if cfg.dbase.port else ""
    return (
        f"{cfg.dbase.driver}://{cfg.dbase.user}:{cfg.dbase.password}"
        f"@{cfg.dbase.host}{port_part}/{cfg.dbase.name}"
    )


def database_settings() -> DatabaseSettings:
    """Возвращает настройки подключения к БД."""
    url = _env("DATABASE_URL")
    if not url:
        url = _build_database_url_from_parts()

    return DatabaseSettings(
        url=url,
        echo=_env_bool("DATABASE_ECHO", False),
        pool_pre_ping=_env_bool("DATABASE_POOL_PRE_PING", True),
    )


def schedule_config(
    name: str,
    *,
    default_cron: str,
    default_timezone: str = "UTC",
    default_enabled: bool = True,
) -> ScheduleConfig:
    """Читает конфигурацию расписания Dagster job из env."""
    key = name.upper()
    return ScheduleConfig(
        name=name,
        cron=_env(f"SCHEDULE_{key}_CRON", default_cron) or default_cron,
        timezone=_env(f"SCHEDULE_{key}_TZ", default_timezone) or default_timezone,
        enabled=_env_bool(f"SCHEDULE_{key}_ENABLED", default_enabled),
    )


def dodo_api_settings() -> DodoApiSettings:
    """Читает настройки общего клиента Dodo API из env."""
    return DodoApiSettings(
        timeout_seconds=_env_int("DODO_API_TIMEOUT_SECONDS", 30),
        retry_limit=_env_int("DODO_API_RETRY_LIMIT", 5),
        retry_delay_seconds=_env_int("DODO_API_RETRY_DELAY_SECONDS", 60),
        user_agent=_env("DODO_API_USER_AGENT", "DodoVkus") or "DodoVkus",
        trust_env=_env_bool("DODO_API_TRUST_ENV", True),
    )


def rabbitmq_settings() -> RabbitMQSettings:
    """Читает настройки RabbitMQ из env."""
    return RabbitMQSettings(
        url=_env("RABBIT_URL", "amqp://guest:guest@localhost:5672/") or "amqp://guest:guest@localhost:5672/",
        queue=_env("RABBIT_QUEUE", "aida") or "aida",
        exchange=_env("RABBIT_EXCHANGE", "") or "",
        routing_key=_env("RABBIT_ROUTING_KEY", "") or "",
        exchange_type=_env("RABBIT_EXCHANGE_TYPE", "direct") or "direct",
        durable=_env_bool("RABBIT_DURABLE", True),
        enabled=_env_bool("RABBIT_ENABLED", True),
    )
