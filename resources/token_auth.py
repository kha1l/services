"""Клиент dodovkus/auth для безопасной работы с Dodo-токенами."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

import aiohttp
from sqlalchemy.orm.attributes import set_committed_value

from config import dodovkus_token_auth_settings
from database.models import Token


class TokenAuthError(RuntimeError):
    """Ошибка сервиса шифрования/дешифрования токенов."""


@dataclass(frozen=True)
class DodovkusTokenAuthClient:
    """HTTP-клиент для endpoints `/auth/decrypt-tokens/` и `/auth/updated-tokens/`."""

    default_service_key: str
    decrypt_url: str
    update_url: str
    timeout_seconds: int
    trust_env: bool

    async def decrypt_tokens(self, tokens: Iterable[Token]) -> None:
        """Дешифрует токены и кладет plaintext только в ORM-объекты в памяти."""
        grouped = self._group_tokens_by_service_key(tokens)
        for service_key, token_group in grouped.items():
            payload = {"tokens": [self._token_payload(token, include_refresh=True) for token in token_group]}
            data = await self._post_json(self.decrypt_url, service_key=service_key, payload=payload)
            decrypted_by_id = {
                int(item["id"]): item
                for item in data.get("tokens", [])
                if isinstance(item, dict) and item.get("id") is not None
            }

            for token in token_group:
                decrypted = decrypted_by_id.get(token.id)
                if decrypted is None:
                    raise TokenAuthError(f"decrypt-tokens response does not contain token_id={token.id}")
                self._set_token_values(
                    token,
                    access=decrypted.get("access"),
                    refresh=decrypted.get("refresh"),
                )

    async def persist_updated_token(self, token: Token, *, access: str, refresh: str) -> None:
        """Передает обновленный plaintext-токен в dodovkus, где он будет сохранен зашифрованным."""
        service_key = self._service_key_for_token(token)
        payload = {
            "tokens": [
                {
                    "id": token.id,
                    "access": access,
                    "refresh": refresh,
                }
            ]
        }
        await self._post_json(self.update_url, service_key=service_key, payload=payload)
        self._set_token_values(token, access=access, refresh=refresh)

    async def _post_json(self, url: str, *, service_key: str, payload: dict) -> dict:
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Service-Key": service_key,
        }
        async with aiohttp.ClientSession(timeout=timeout, trust_env=self.trust_env) as session:
            try:
                async with session.post(url, json=payload, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        if not isinstance(data, dict):
                            raise TokenAuthError(f"{url} returned non-object JSON")
                        return data

                    response_text = await response.text()
                    raise TokenAuthError(f"{url} failed: {response.status} - {response_text}")
            except (aiohttp.ClientError, TimeoutError) as exc:
                raise TokenAuthError(f"{url} failed: {exc}") from exc

    def _group_tokens_by_service_key(self, tokens: Iterable[Token]) -> dict[str, list[Token]]:
        grouped: dict[str, list[Token]] = defaultdict(list)
        for token in tokens:
            if not token.access:
                continue
            grouped[self._service_key_for_token(token)].append(token)
        return dict(grouped)

    def _service_key_for_token(self, token: Token) -> str:
        application = getattr(token, "application", None)
        service_key = getattr(application, "client_id", None)
        if not service_key:
            service_key = self.default_service_key
        return service_key

    @staticmethod
    def _token_payload(token: Token, *, include_refresh: bool) -> dict:
        payload = {
            "id": token.id,
            "access": token.access,
        }
        if include_refresh:
            payload["refresh"] = token.refresh
        return payload

    @staticmethod
    def _set_token_values(token: Token, *, access: str | None, refresh: str | None) -> None:
        # set_committed_value не помечает ORM-объект dirty, поэтому plaintext не утечет
        # обратно в БД при следующих session.commit().
        set_committed_value(token, "access", access)
        set_committed_value(token, "refresh", refresh)


def token_auth_client() -> DodovkusTokenAuthClient:
    """Создает клиент с настройками из env."""
    cfg = dodovkus_token_auth_settings()
    return DodovkusTokenAuthClient(
        default_service_key=cfg.default_service_key,
        decrypt_url=cfg.decrypt_url,
        update_url=cfg.update_url,
        timeout_seconds=cfg.timeout_seconds,
        trust_env=cfg.trust_env,
    )
