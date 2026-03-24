"""Синхронизация premium-подписок и списка заведений из Dodo API."""

import asyncio
from datetime import datetime

from database.models import Token
from database.requests import (
    delete_subscribes_and_orphan_units,
    insert_subs,
    insert_unit,
    resolve_service_alert,
    select_account_recipients,
    select_all_tokens,
    select_subscribe_snapshot_for_token,
    select_subscribe_uuids_for_token,
    select_units,
    touch_service_alert,
)
from logger import get_logger
from messaging import RabbitMQPublisher, build_payload
from resources.dodo_api import DodoApiClient
from shared.job_guard import safe_rollback

logger = get_logger(__name__)


class SubscribeRefresher:
    """Обновляет локальные premium-подписки и уведомляет только об их изменении."""

    def __init__(
        self,
        dodo_api: DodoApiClient,
        rabbitmq: RabbitMQPublisher,
        delay: float = 1.0,
    ):
        self.dodo_api = dodo_api
        self.rabbitmq = rabbitmq
        self.delay = delay

    @staticmethod
    def _parse_expires(value: str | None):
        """Преобразует `expiresAt` из Dodo API в `datetime`."""
        if not value:
            return None
        try:
            normalized = value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except Exception as exc:
            raise ValueError(f"invalid expiresAt: {value}") from exc

    @staticmethod
    def _parse_account_properties(properties: str) -> tuple[str, str, str]:
        """Нормализует `account.properties`, в том числе строки с хвостовым `/`."""
        normalized = str(properties or "").strip().strip("/")
        parts = [part.strip() for part in normalized.split("/") if part.strip()]
        if len(parts) < 3:
            raise ValueError(f"invalid account.properties: {properties}")
        return parts[0], parts[1], parts[2]

    async def _publish_message(self, session, token: Token, text: str) -> None:
        """Отправляет сервисное сообщение всем пользователям аккаунта."""
        recipients = await select_account_recipients(session, token.account_id)
        if not recipients:
            logger.warning(f"No recipients for subscribe token {token.id}")
            return

        for recipient in recipients:
            try:
                await self.rabbitmq.publish_async(
                    build_payload(
                        chat_id=recipient.chat_id,
                        post=None,
                        text=text,
                    ),
                    platform=recipient.platform,
                )
            except Exception as exc:
                logger.error(
                    f"Failed to publish subscribe message for chat {recipient.chat_id}"
                    f" platform={recipient.platform}: {exc}"
                )

    async def _handle_failure(self, session, token: Token, error_text: str) -> None:
        """Фиксирует алерт сбоя подписок без деактивации аккаунта и удаления токена."""
        await touch_service_alert(
            session,
            service="subscribes",
            token_id=token.id,
            kind="refresh_failed",
            error_text=error_text,
        )

    async def _publish_premium_changes(self, session, token: Token, changed_lines: list[str]) -> None:
        """Публикует одно агрегированное сообщение об изменении premium-подписок."""
        if not changed_lines:
            return

        account = token.account
        account_name = account.name if account else f"account_id={token.account_id}"
        text = (
            "[servises/subscribes]\n"
            f"Изменилась premium-подписка для аккаунта {account_name}.\n"
            + "\n".join(changed_lines)
        )
        await self._publish_message(session, token, text)

    @staticmethod
    def _build_premium_change_lines(
        existing_snapshot: dict[str, tuple[str | None, str]],
        desired_aliases: dict[str, str],
        desired_names: dict[str, str],
        free_uuids: set[str],
    ) -> list[str]:
        """Собирает понятный diff между прошлым и текущим состоянием premium-подписок."""
        changed_lines: list[str] = []
        all_uuids = set(existing_snapshot) | set(desired_aliases)

        for unit_uuid in sorted(all_uuids, key=lambda item: (desired_names.get(item) or existing_snapshot.get(item, (None, item))[1]).lower()):
            previous_alias, previous_name = existing_snapshot.get(unit_uuid, (None, unit_uuid))
            new_alias = desired_aliases.get(unit_uuid)
            unit_name = desired_names.get(unit_uuid) or previous_name or unit_uuid

            if previous_alias == new_alias:
                continue

            if previous_alias is None and new_alias is not None:
                continue

            if previous_alias is not None and new_alias is None:
                if unit_uuid in free_uuids:
                    changed_lines.append(f"- {unit_name}: {previous_alias} -> free")
                else:
                    changed_lines.append(f"- {unit_name}: {previous_alias} -> отключен")
                continue

            if previous_alias is not None and new_alias is not None:
                changed_lines.append(f"- {unit_name}: {previous_alias} -> {new_alias}")

        return changed_lines

    async def _refresh_subs(self, token: Token, domain: str):
        """Запрашивает у Dodo список активных подписок по токену."""
        url = f"https://api.dodois.{domain}/marketplace/subscriptions"
        return await self.dodo_api.get_json(url, bearer_token=token.access)

    async def _refresh_units(self, token: Token, properties: str, units):
        """Запрашивает карточки заведений для списка UUID из подписки."""
        concept_dict = {
            "dodopizza": "63d4829611ea45c8ae71394860a2481c",
            "drinkit": "c0b18e725258427a8bffea4f73957b0e"
        }
        domain, concept_name, country = self._parse_account_properties(properties)
        url = f"https://api.dodois.{domain}/{concept_name}/{country}/units/stores"
        concept = concept_dict.get(concept_name)
        units_str = ','.join(units)
        params = {
            "countryId": country,
            "businessId": concept,
            "units": units_str
        }
        return await self.dodo_api.get_json(
            url,
            bearer_token=token.access,
            params=params,
        )

    async def refresh_subscribe(self, session):
        """Полностью синхронизирует premium-подписки по всем токенам из БД."""
        try:
            uuids = set(await select_units(session))
            tokens = await select_all_tokens(session)
        except Exception as exc:  # noqa: BLE001
            logger.error("SUBSCRIBES JOB: failed to load initial data: %s", exc)
            await safe_rollback(session, logger, context="SUBSCRIBES JOB load initial data")
            return

        for token in tokens:
            try:
                account = token.account
                domain, _concept_name, _country = self._parse_account_properties(account.properties)
                existing_snapshot = await select_subscribe_snapshot_for_token(session, token.id)
                try:
                    resp = await self._refresh_subs(token, domain)
                except Exception as e:
                    logger.error(f"ERROR UPDATE TOKEN {token.id}: {e}")
                    await self._handle_failure(session, token, str(e))
                    continue

                subs = resp.get("activeUserSubscriptions", [])
                if not subs:
                    logger.warning(f"No active subscribes for {token.id}")
                    # Если Dodo больше не возвращает premium-подписки, удаляем локальный хвост этого токена.
                    existing_uuids = await select_subscribe_uuids_for_token(session, token.id)
                    deleted_unit_uuids = await delete_subscribes_and_orphan_units(session, token.id, existing_uuids)
                    uuids.difference_update(deleted_unit_uuids)
                    changed_lines = self._build_premium_change_lines(
                        existing_snapshot=existing_snapshot,
                        desired_aliases={},
                        desired_names={},
                        free_uuids=set(),
                    )
                    await self._publish_premium_changes(session, token, changed_lines)
                    await resolve_service_alert(session, "subscribes", token.id, "refresh_failed")
                    continue

                token_failed = False
                desired_uuids: set[str] = set()
                desired_aliases: dict[str, str] = {}
                desired_names: dict[str, str] = {}
                free_uuids: set[str] = set()
                for sub in subs:
                    units = sub.get("units") or []
                    if not units:
                        continue

                    alias = sub.get("tariffAlias")
                    normalized_alias = str(alias or "").strip().lower()
                    if normalized_alias == "free":
                        # `free` не храним как premium-подписку, но учитываем для diff-сообщений.
                        free_uuids.update(str(unit_uuid) for unit_uuid in units if unit_uuid is not None)
                        continue

                    normalized_units = [str(unit_uuid) for unit_uuid in units if unit_uuid is not None]
                    desired_uuids.update(normalized_units)
                    for unit_uuid in normalized_units:
                        desired_aliases[unit_uuid] = alias
                    expires_str = sub.get("expiresAt")
                    try:
                        units_response = await self._refresh_units(token, account.properties, units)
                    except Exception as e:
                        logger.error(f"ERROR UPDATE UNITS {token.id}: {e}")
                        token_failed = True
                        await self._handle_failure(session, token, str(e))
                        continue

                    try:
                        expires = self._parse_expires(expires_str)
                    except Exception as e:
                        logger.error(f"not convert expiresAt {token.id}: {e}")
                        expires = None

                    units_info = units_response.get("stores", [])
                    try:
                        for unit in units_info:
                            unit_id = str(unit.get("id"))
                            desired_names[unit_id] = str(unit.get("name") or unit_id)
                            if unit_id not in uuids:
                                await insert_unit(session, unit)
                                uuids.add(unit_id)
                            await insert_subs(session, unit, alias, expires, token.id)
                    except Exception as e:
                        logger.error(f"incorrect units for {token.id}: {e}")
                        token_failed = True
                        await self._handle_failure(session, token, str(e))
                if not token_failed:
                    existing_uuids = await select_subscribe_uuids_for_token(session, token.id)
                    stale_uuids = existing_uuids - desired_uuids
                    if stale_uuids:
                        deleted_unit_uuids = await delete_subscribes_and_orphan_units(session, token.id, stale_uuids)
                        uuids.difference_update(deleted_unit_uuids)
                    # Сообщение собираем только после успешной синхронизации токена.
                    changed_lines = self._build_premium_change_lines(
                        existing_snapshot=existing_snapshot,
                        desired_aliases=desired_aliases,
                        desired_names=desired_names,
                        free_uuids=free_uuids,
                    )
                    await self._publish_premium_changes(session, token, changed_lines)
                    await resolve_service_alert(session, "subscribes", token.id, "refresh_failed")
                await asyncio.sleep(self.delay)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "SUBSCRIBES JOB: unexpected error for token_id=%s account_id=%s: %s",
                    getattr(token, "id", "unknown"),
                    getattr(token, "account_id", "unknown"),
                    exc,
                )
                await safe_rollback(
                    session,
                    logger,
                    context=f"SUBSCRIBES JOB token_id={getattr(token, 'id', 'unknown')}",
                )
                continue
