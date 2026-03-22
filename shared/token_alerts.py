"""Общие helper'ы для антиспама и очистки stale токенов."""

from __future__ import annotations

from datetime import datetime, timedelta


NOTIFY_AFTER = timedelta(hours=24)
DELETE_AFTER = timedelta(hours=48)


def alert_reference_time(token_last_update: datetime | None, first_seen_at: datetime) -> datetime:
    """Берет время последнего успешного обновления, а если его нет — первое наблюдение ошибки."""
    return token_last_update or first_seen_at


def should_notify(reference_time: datetime, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    return now - reference_time >= NOTIFY_AFTER


def should_delete(reference_time: datetime, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    return now - reference_time >= DELETE_AFTER
