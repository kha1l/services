"""Helpers для устойчивого выполнения job без падения всего run."""

from __future__ import annotations

from security_logging import safe_identifier


async def safe_rollback(session, logger, *, context: str) -> None:
    """Пытается откатить транзакцию и логирует проблему, если rollback тоже не удался."""
    try:
        await session.rollback()
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "rollback failed: context_hash=%s error_type=%s",
            safe_identifier(context),
            type(exc).__name__,
        )
