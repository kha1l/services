"""Dagster op для обновления франшиз."""

import contextlib

from dagster import op

from logger import get_logger

from .franchisee_update import FranchiseeRefresher


logger = get_logger(__name__)


async def run_franchisee_refresh(session_factory, dodo_api, log=None) -> None:
    active_log = log or logger
    active_log.info("START FRANCHISEE")
    try:
        async with session_factory() as session:
            refresher = FranchiseeRefresher(dodo_api=dodo_api)
            try:
                await refresher.refresh_franchisee(session)
            except Exception as exc:  # noqa: BLE001
                active_log.error("FRANCHISEE JOB: unhandled error inside refresh loop: %s", exc)
                with contextlib.suppress(Exception):
                    await session.rollback()
        active_log.info("END FRANCHISEE")
    except Exception as exc:  # noqa: BLE001
        active_log.error("FRANCHISEE JOB: failed before completion: %s", exc)


@op(required_resource_keys={"db", "dodo_api"})
async def franchisee_op(context) -> None:
    """Запускает обновление справочника франшиз."""
    await run_franchisee_refresh(
        context.resources.db.get_sessionmaker(),
        context.resources.dodo_api,
        context.log,
    )
