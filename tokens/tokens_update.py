import asyncio
import aiohttp
from database.models import Token, Application
from logger import get_logger
from database.requests import update_token, select_all_tokens

logger = get_logger(__name__)

class TokenRefresher:
    def __init__(
        self,
        delay: float = 1.0,
        retry_limit: int = 5,
        retry_delay: float = 60.0,
    ):
        self.delay = delay
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    async def _refresh_single(self, token: Token, app: Application, domain: str):
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
            "User-Agent": "DodoVkus",
        }

        auth = aiohttp.BasicAuth(app.client_id, app.secret)

        async with aiohttp.ClientSession(trust_env=True) as session:
            for attempt in range(self.retry_limit):
                async with session.post(url, headers=headers, data=data, auth=auth) as r:
                    status = r.status

                    if status == 200:
                        return await r.json()

                    if status in (429, 443) or status >= 500:
                        await asyncio.sleep(self.retry_delay)
                        continue

                    raise Exception(f"Refresh failed: {status} - {await r.text()}")

            raise Exception(f"Failed after {self.retry_limit} attempts")

    async def refresh_all_tokens(self, session):
        tokens = await select_all_tokens(session)

        for token in tokens:
            app = token.application
            account = token.account

            domain = account.properties.split("/")[0]

            try:
                resp = await self._refresh_single(token, app, domain)
            except Exception as e:
                logger.error(f"ERROR UPDATE TOKEN {token.id}: {e}")
                continue

            await update_token(
                session,
                token,
                resp.get("access_token"),
                resp.get("refresh_token")
            )

            logger.info(f"{token.id}\n{resp.get("access_token")}\n{resp.get("refresh_token")}")

            await asyncio.sleep(self.delay)
