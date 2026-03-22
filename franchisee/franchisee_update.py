"""Синхронизация справочника франчайзи и точек из Dodo API."""

import asyncio

from logger import get_logger
from database.requests import select_uuids, select_all_tokens, insert_unit_franchisee
from resources.dodo_api import DodoApiClient, DodoApiError
from shared.job_guard import safe_rollback

logger = get_logger(__name__)


class FranchiseeRefresher:
    """Обходит все токены и пополняет локальный справочник франчайзи."""

    def __init__(self, dodo_api: DodoApiClient, delay: float = 1.0):
        self.dodo_api = dodo_api
        self.delay = delay

    async def refresh_franchisee(self, session):
        """Грузит список франчайзи из Dodo и добавляет только новые UUID."""
        try:
            uuids = set(await select_uuids(session))
            tokens = await select_all_tokens(session)
        except Exception as exc:  # noqa: BLE001
            logger.error("FRANCHISEE JOB: failed to load initial data: %s", exc)
            await safe_rollback(session, logger, context="FRANCHISEE JOB load initial data")
            return

        for token in tokens:
            try:
                account = token.account
                domain = account.properties.split("/")[0]
                try:
                    franc_data = await self.dodo_api.get_json(
                        f"https://api.dodois.{domain}/franchisee/units",
                        bearer_token=token.access,
                    )
                except DodoApiError as exc:
                    logger.error(f"ERROR LOAD FRANCHISEE {token.id}: {exc}")
                    continue
                if franc_data:
                    # Оставляем только новые UUID, чтобы job была идемпотентной.
                    for unit in franc_data.get("units", []):
                        unit_id = unit["id"]
                        if unit_id not in uuids:
                            try:
                                await insert_unit_franchisee(session, unit)
                                uuids.add(unit_id)
                                logger.info(f'Inserted unit {unit_id}')
                            except Exception as e:
                                logger.error(f"Failed to insert unit {unit_id}: {e}")
                                await safe_rollback(
                                    session,
                                    logger,
                                    context=f"FRANCHISEE JOB insert unit_id={unit_id}",
                                )

                            await asyncio.sleep(self.delay)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "FRANCHISEE JOB: unexpected error for token_id=%s account_id=%s: %s",
                    getattr(token, "id", "unknown"),
                    getattr(token, "account_id", "unknown"),
                    exc,
                )
                await safe_rollback(
                    session,
                    logger,
                    context=f"FRANCHISEE JOB token_id={getattr(token, 'id', 'unknown')}",
                )
                continue
