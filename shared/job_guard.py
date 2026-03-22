"""Helpers для устойчивого выполнения job без падения всего run."""

from __future__ import annotations


async def safe_rollback(session, logger, *, context: str) -> None:
    """Пытается откатить транзакцию и логирует проблему, если rollback тоже не удался."""
    try:
        await session.rollback()
    except Exception as exc:  # noqa: BLE001
        logger.error("%s: rollback failed: %s", context, exc)
