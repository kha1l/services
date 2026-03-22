"""Обновление OAuth-токенов Dodo и lifecycle-логика по stale токенам."""

import asyncio
import aiohttp
from datetime import datetime

from database.models import Token, Application
from logger import get_logger
from database.requests import (
    delete_token_with_relations,
    mark_service_alert_notified,
    resolve_service_alert,
    set_account_active,
    select_account_chat_ids,
    select_all_tokens,
    touch_service_alert,
    update_token,
)
from messaging import build_payload, RabbitMQPublisher
from resources.dodo_api import DodoApiClient
from shared.job_guard import safe_rollback
from shared.token_alerts import alert_reference_time, should_delete, should_notify

logger = get_logger(__name__)


class TokenRefresher:
    """Обходит все токены, обновляет их и управляет алертами/деактивацией аккаунтов."""

    def __init__(
        self,
        dodo_api: DodoApiClient,
        rabbitmq: RabbitMQPublisher,
        delay: float = 1.0,
    ):
        self.dodo_api = dodo_api
        self.rabbitmq = rabbitmq
        self.delay = delay

    async def _publish_message(self, session, token: Token, text: str) -> None:
        """Отправляет уведомление всем Telegram-получателям аккаунта."""
        chat_ids = await select_account_chat_ids(session, token.account_id)
        if not chat_ids:
            logger.warning(f"No Telegram recipients for token {token.id}")
            return

        for chat_id in chat_ids:
            try:
                await self.rabbitmq.publish_async(
                    build_payload(
                        chat_id=chat_id,
                        post=None,
                        text=text,
                    )
                )
            except Exception as exc:
                logger.error(f"Failed to publish token message for chat {chat_id}: {exc}")

    async def _handle_failure(self, session, token: Token, error_text: str) -> None:
        """Обновляет алерт токена, при необходимости деактивирует аккаунт и удаляет stale токен."""
        account = token.account
        account_name = account.name if account else f"account_id={token.account_id}"

        alert = await touch_service_alert(
            session,
            service="tokens",
            token_id=token.id,
            kind="refresh_failed",
            error_text=error_text,
        )
        reference_time = alert_reference_time(token.last_update, alert.first_seen_at)

        if should_notify(reference_time, datetime.now()):
            await set_account_active(session, token.account_id, False)

        if should_delete(reference_time, datetime.now()):
            text = (
                "[servises/tokens]\n"
                f"Токен для аккаунта {account_name} не обновлялся более 48 часов и был удален.\n"
                f"token_id={token.id}\n"
                "Связанные подписки этого токена также удалены."
            )
            await self._publish_message(session, token, text)
            await delete_token_with_relations(session, token)
            logger.warning("Deleted stale token: token_id=%s account_id=%s", token.id, token.account_id)
            return

        if should_notify(reference_time, datetime.now()) and alert.notified_at is None:
            text = (
                "[servises/tokens]\n"
                f"Не удалось обновить токен для аккаунта {account_name}.\n"
                f"token_id={token.id}\n"
                f"Ошибка: {error_text}"
            )
            await self._publish_message(session, token, text)
            await mark_service_alert_notified(session, alert)

    async def _refresh_single(self, token: Token, app: Application, domain: str):
        """Выполняет refresh access/refresh токена через OAuth endpoint Dodo."""
        url = f"https://auth.dodois.{domain}/connect/token"

        redirect = app.redirect_io if domain == "io" else app.redirect_com

        data = {
            "grant_type": "refresh_token",
            "redirect_uri": redirect,
            "code_verifier": app.verifier,
            "refresh_token": token.refresh,
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        auth = aiohttp.BasicAuth(app.client_id, app.secret)
        return await self.dodo_api.post_json(
            url,
            data=data,
            headers=headers,
            auth=auth,
        )

    async def refresh_all_tokens(self, session):
        """Последовательно обновляет все токены из БД и не роняет job из-за одной ошибки."""
        try:
            tokens = await select_all_tokens(session)
        except Exception as exc:  # noqa: BLE001
            logger.error("TOKENS JOB: failed to load tokens: %s", exc)
            await safe_rollback(session, logger, context="TOKENS JOB load tokens")
            return

        for token in tokens:
            try:
                app = token.application
                account = token.account
                domain = account.properties.split("/")[0]

                try:
                    resp = await self._refresh_single(token, app, domain)
                except Exception as e:
                    logger.error(f"ERROR UPDATE TOKEN {token.id}: {e}")
                    await self._handle_failure(session, token, str(e))
                    continue

                # Токен и last_update обновляются одним commit внутри DB helper.
                await update_token(
                    session,
                    token,
                    resp.get("access_token"),
                    resp.get("refresh_token")
                )

                logger.info(
                    "Token updated successfully: token_id=%s account_id=%s",
                    token.id,
                    token.account_id,
                )
                await resolve_service_alert(session, "tokens", token.id, "refresh_failed")
                await asyncio.sleep(self.delay)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "TOKENS JOB: unexpected error for token_id=%s account_id=%s: %s",
                    getattr(token, "id", "unknown"),
                    getattr(token, "account_id", "unknown"),
                    exc,
                )
                await safe_rollback(
                    session,
                    logger,
                    context=f"TOKENS JOB token_id={getattr(token, 'id', 'unknown')}",
                )
                continue
