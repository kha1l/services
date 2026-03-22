"""Dagster op для обновления токенов."""

import contextlib

from dagster import op

from logger import get_logger

from .tokens_update import TokenRefresher


logger = get_logger(__name__)


async def run_tokens_refresh(session_factory, dodo_api, rabbitmq, log=None) -> None:
    active_log = log or logger
    active_log.info("START TOKENS")
    try:
        async with session_factory() as session:
            refresher = TokenRefresher(dodo_api=dodo_api, rabbitmq=rabbitmq)
            try:
                await refresher.refresh_all_tokens(session)
            except Exception as exc:  # noqa: BLE001
                active_log.error("TOKENS JOB: unhandled error inside refresh loop: %s", exc)
                with contextlib.suppress(Exception):
                    await session.rollback()
        active_log.info("END TOKENS")
    except Exception as exc:  # noqa: BLE001
        active_log.error("TOKENS JOB: failed before completion: %s", exc)
    finally:
        try:
            await rabbitmq.close_async()
        except Exception as exc:  # noqa: BLE001
            active_log.error("TOKENS JOB: failed to close RabbitMQ transport: %s", exc)


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
async def tokens_op(context) -> None:
    """Запускает обновление refresh/access токенов."""
    await run_tokens_refresh(
        context.resources.db.get_sessionmaker(),
        context.resources.dodo_api,
        context.resources.rabbitmq,
        context.log,
    )
