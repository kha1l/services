"""Синхронизация справочника франчайзи и точек из Dodo API."""

import asyncio

from logger import get_logger
from database.requests import select_uuids, select_all_tokens, insert_unit_franchisee
from resources.dodo_api import DodoApiClient, DodoApiError
from security_logging import safe_identifier
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
            logger.error(
                "FRANCHISEE JOB: failed to load initial data: error_type=%s",
                type(exc).__name__,
            )
            await safe_rollback(session, logger, context="FRANCHISEE JOB load initial data")
            return

        for token in tokens:
            try:
                credential_id = token.id
                account = token.account
                domain = account.properties.split("/")[0]
                try:
                    franc_data = await self.dodo_api.get_json(
                        f"https://api.dodois.{domain}/franchisee/units",
                        bearer_token=token.access,
                    )
                except DodoApiError as exc:
                    logger.error(
                        "Failed to load franchise data for credential_hash=%s: error_type=%s",
                        safe_identifier(credential_id),
                        type(exc).__name__,
                    )
                    continue
                if franc_data:
                    # Оставляем только новые UUID, чтобы job была идемпотентной.
                    for unit in franc_data.get("units", []):
                        unit_id = unit["id"]
                        if unit_id not in uuids:
                            try:
                                await insert_unit_franchisee(session, unit)
                                uuids.add(unit_id)
                                logger.info(
                                    "Inserted unit_hash=%s",
                                    safe_identifier(unit_id),
                                )
                            except Exception as exc:
                                logger.error(
                                    "Failed to insert unit_hash=%s: error_type=%s",
                                    safe_identifier(unit_id),
                                    type(exc).__name__,
                                )
                                await safe_rollback(
                                    session,
                                    logger,
                                    context=f"FRANCHISEE JOB insert unit_id={unit_id}",
                                )

                            await asyncio.sleep(self.delay)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "FRANCHISEE JOB: unexpected error for token_hash=%s account_hash=%s: error_type=%s",
                    safe_identifier(getattr(token, "id", None)),
                    safe_identifier(getattr(token, "account_id", None)),
                    type(exc).__name__,
                )
                await safe_rollback(
                    session,
                    logger,
                    context=f"FRANCHISEE JOB token_id={getattr(token, 'id', 'unknown')}",
                )
                continue
