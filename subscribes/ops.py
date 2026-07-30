"""Dagster op для обновления подписок и заведений."""

import contextlib

from dagster import op

from logger import get_logger
from security_logging import safe_context_logger

from .subs_update import SubscribeRefresher


logger = get_logger(__name__)


async def run_subscribes_refresh(session_factory, dodo_api, rabbitmq, log=None) -> None:
    active_log = log or logger
    active_log.info("START SUBSCRIBES")
    try:
        async with session_factory() as session:
            refresher = SubscribeRefresher(dodo_api=dodo_api, rabbitmq=rabbitmq)
            try:
                await refresher.refresh_subscribe(session)
            except Exception as exc:  # noqa: BLE001
                active_log.error(
                    "SUBSCRIBES JOB: unhandled error inside refresh loop: error_type=%s",
                    type(exc).__name__,
                )
                with contextlib.suppress(Exception):
                    await session.rollback()
        active_log.info("END SUBSCRIBES")
    except Exception as exc:  # noqa: BLE001
        active_log.error(
            "SUBSCRIBES JOB: failed before completion: error_type=%s",
            type(exc).__name__,
        )
    finally:
        try:
            await rabbitmq.close_async()
        except Exception as exc:  # noqa: BLE001
            active_log.error(
                "SUBSCRIBES JOB: failed to close RabbitMQ transport: error_type=%s",
                type(exc).__name__,
            )


@op(required_resource_keys={"db", "dodo_api", "rabbitmq"})
async def subscribes_op(context) -> None:
    """Запускает обновление подписок и заведений."""
    log = safe_context_logger(context)
    await run_subscribes_refresh(
        context.resources.db.get_sessionmaker(),
        context.resources.dodo_api,
        context.resources.rabbitmq,
        log,
    )
