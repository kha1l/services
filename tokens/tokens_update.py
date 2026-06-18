"""Обновление OAuth-токенов Dodo и lifecycle-логика по stale токенам."""

import asyncio
import os
import re
import aiohttp
from datetime import datetime

from database.models import Token, Application
from logger import get_logger
from database.requests import (
    delete_token_with_relations,
    mark_service_alert_notified,
    resolve_service_alert,
    set_account_active,
    select_account_recipients,
    select_all_tokens,
    touch_service_alert,
    update_token,
)
from messaging import build_payload, RabbitMQPublisher
from resources.dodo_api import DodoApiClient
from shared.job_guard import safe_rollback
from shared.token_alerts import alert_reference_time, should_delete, should_notify

logger = get_logger(__name__)

APPLICATION_ENV_ALIASES = {
    "aida": ("AIDA",),
    "aidametrics": ("AIDAMETRICS", "METRICS"),
    "core": ("CORE",),
    "kado": ("KADO",),
}


def _normalize_env_key(value: object) -> str:
    """Приводит имя приложения к безопасному env-суффиксу."""
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_")
    return normalized.upper()


def _env_first(names: list[str]) -> str | None:
    """Возвращает первое непустое значение из env."""
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return None


def _application_secret_env_names(app: Application, field: str) -> list[str]:
    """Строит список поддерживаемых env-имен для секрета приложения."""
    field_key = field.upper()
    app_name = str(getattr(app, "name", "") or "").strip().lower()
    app_key = _normalize_env_key(app_name)
    app_id = getattr(app, "id", None)

    names: list[str] = []
    if app_key:
        names.extend(
            [
                f"DODO_APP_{app_key}_{field_key}",
                f"DODO_APPLICATION_{app_key}_{field_key}",
                f"{field_key}_{app_key}",
            ]
        )
    for alias in APPLICATION_ENV_ALIASES.get(app_name, ()):
        names.extend(
            [
                f"DODO_APP_{alias}_{field_key}",
                f"DODO_APPLICATION_{alias}_{field_key}",
                f"{field_key}_{alias}",
            ]
        )
    if app_id is not None:
        names.extend(
            [
                f"DODO_APP_{app_id}_{field_key}",
                f"DODO_APPLICATION_{app_id}_{field_key}",
            ]
        )
    return list(dict.fromkeys(names))


def get_application_secret_value(app: Application, field: str) -> str:
    """Берет OAuth secret/verifier приложения из env/Kubernetes Secret, а не из БД."""
    names = _application_secret_env_names(app, field)
    value = _env_first(names)
    if value is None:
        app_name = getattr(app, "name", None) or getattr(app, "id", "unknown")
        raise RuntimeError(
            f"Missing OAuth env value for application {app_name!r}: set one of {', '.join(names)}"
        )
    return value


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
        """Отправляет уведомление всем получателям аккаунта."""
        recipients = await select_account_recipients(session, token.account_id)
        if not recipients:
            logger.warning(f"No recipients for token {token.id}")
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
                    f"Failed to publish token message for chat {recipient.chat_id}"
                    f" platform={recipient.platform}: {exc}"
                )

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
            "code_verifier": get_application_secret_value(app, "verifier"),
            "refresh_token": token.refresh,
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        auth = aiohttp.BasicAuth(app.client_id, get_application_secret_value(app, "secret"))
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
