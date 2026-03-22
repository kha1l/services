"""Общий Dagster-ресурс для работы с Dodo API."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import aiohttp
from dagster import BoolSource, Field, InitResourceContext, IntSource, StringSource, resource

from config import dodo_api_settings


class DodoApiError(RuntimeError):
    """Ошибка запроса к Dodo API."""


@dataclass(frozen=True)
class DodoApiClient:
    """Тонкий async-клиент Dodo API с общими retry-настройками."""

    timeout_seconds: int
    retry_limit: int
    retry_delay_seconds: int
    user_agent: str
    trust_env: bool

    async def get_json(
        self,
        url: str,
        *,
        bearer_token: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        return await self._request_json(
            "GET",
            url,
            bearer_token=bearer_token,
            params=params,
            headers=headers,
        )

    async def post_json(
        self,
        url: str,
        *,
        bearer_token: str | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        auth: aiohttp.BasicAuth | None = None,
    ) -> Any:
        return await self._request_json(
            "POST",
            url,
            bearer_token=bearer_token,
            data=data,
            headers=headers,
            auth=auth,
        )

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        bearer_token: str | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        auth: aiohttp.BasicAuth | None = None,
    ) -> Any:
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        request_headers = self._build_headers(bearer_token=bearer_token, headers=headers)

        async with aiohttp.ClientSession(timeout=timeout, trust_env=self.trust_env) as session:
            for attempt in range(self.retry_limit):
                try:
                    async with session.request(
                        method,
                        url,
                        params=params,
                        data=data,
                        headers=request_headers,
                        auth=auth,
                    ) as response:
                        if response.status == 200:
                            return await response.json()

                        response_text = await response.text()
                        if self._should_retry(response.status) and attempt + 1 < self.retry_limit:
                            await asyncio.sleep(self.retry_delay_seconds)
                            continue

                        raise DodoApiError(
                            f"{method} {url} failed: {response.status} - {response_text}"
                        )
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    if attempt + 1 < self.retry_limit:
                        await asyncio.sleep(self.retry_delay_seconds)
                        continue
                    raise DodoApiError(f"{method} {url} failed: {exc}") from exc

        raise DodoApiError(f"{method} {url} failed after {self.retry_limit} attempts")

    def _build_headers(
        self,
        *,
        bearer_token: str | None,
        headers: dict[str, str] | None,
    ) -> dict[str, str]:
        result = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": self.user_agent,
        }
        if headers:
            result.update(headers)
        if bearer_token:
            result["Authorization"] = f"Bearer {bearer_token}"
        return result

    @staticmethod
    def _should_retry(status: int) -> bool:
        return status in {429, 443} or status >= 500


@resource(
    config_schema={
        "timeout_seconds": Field(IntSource, is_required=False),
        "retry_limit": Field(IntSource, is_required=False),
        "retry_delay_seconds": Field(IntSource, is_required=False),
        "user_agent": Field(StringSource, is_required=False),
        "trust_env": Field(BoolSource, is_required=False),
    }
)
def dodo_api_resource(context: InitResourceContext) -> DodoApiClient:
    """Создает общий клиент Dodo API для всех jobs."""
    cfg = dodo_api_settings()
    overrides = context.resource_config or {}
    return DodoApiClient(
        timeout_seconds=overrides.get("timeout_seconds", cfg.timeout_seconds),
        retry_limit=overrides.get("retry_limit", cfg.retry_limit),
        retry_delay_seconds=overrides.get("retry_delay_seconds", cfg.retry_delay_seconds),
        user_agent=overrides.get("user_agent", cfg.user_agent),
        trust_env=overrides.get("trust_env", cfg.trust_env),
    )
