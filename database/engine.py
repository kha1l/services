"""SQLAlchemy engine и фабрика async sessions."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import database_settings


def _normalize_async_url(url: str) -> str:
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def build_engine(url: str, *, echo: bool = False, pool_pre_ping: bool = True):
    """Создает async SQLAlchemy engine для указанного URL."""
    return create_async_engine(
        _normalize_async_url(url),
        echo=echo,
        pool_pre_ping=pool_pre_ping,
        future=True,
    )


def build_sessionmaker(engine):
    """Создает async sessionmaker поверх переданного engine."""
    return async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


_cfg = database_settings()
engine = build_engine(_cfg.url, echo=_cfg.echo, pool_pre_ping=_cfg.pool_pre_ping)
session_maker = build_sessionmaker(engine)
