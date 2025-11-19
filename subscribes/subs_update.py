from database.requests import select_all_tokens, select_units, insert_unit, insert_subs
import aiohttp
import asyncio
from database.models import Token
from logger import get_logger

logger = get_logger(__name__)

class SubscribeRefresher:
    def __init__(
            self,
            delay: float = 1.0,
            retry_limit: int = 5,
            retry_delay: float = 60.0,
    ):
        self.delay = delay
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "DodoVkus"
        }

    async def _refresh_subs(self, token: Token, domain: str):
        url = f"https://api.dodois.{domain}/marketplace/subscriptions"
        headers = {**self.headers, "Authorization": f"Bearer {token.access}"}
        async with aiohttp.ClientSession(trust_env=True) as session:
            for attempt in range(self.retry_limit):
                async with session.get(url, headers=headers) as r:
                    status = r.status
                    if status == 200:
                        return await r.json()

                    if status in (429, 443) or status >= 500:
                        await asyncio.sleep(self.retry_delay)
                        continue
                    raise Exception(f"Refresh failed: {status} - {await r.text()}")
            raise Exception(f"Failed after {self.retry_limit} attempts")

    async def _refresh_units(self, token: Token, properties: str, units):
        concept_dict = {
            "dodopizza": "63d4829611ea45c8ae71394860a2481c",
            "drinkit": "c0b18e725258427a8bffea4f73957b0e"
        }
        url = f"https://api.dodois.{properties}/units/stores"
        headers = {**self.headers, "Authorization": f"Bearer {token.access}"}
        country = properties.split('/')[-1]
        concept = concept_dict.get(properties.split('/')[1])
        units_str = ','.join(units)
        params = {
            "countryId": country,
            "businessId": concept,
            "units": units_str
        }
        async with aiohttp.ClientSession(trust_env=True) as session:
            for attempt in range(self.retry_limit):
                async with session.get(url, headers=headers, params=params) as r:
                    status = r.status
                    if status == 200:
                        return await r.json()

                    if status in (429, 443) or status >= 500:
                        await asyncio.sleep(self.retry_delay)
                        continue
                    raise Exception(f"Refresh failed: {status} - {await r.text()}")
            raise Exception(f"Failed after {self.retry_limit} attempts")


    async def refresh_subscribe(self, session):
        uuids = await select_units(session)
        tokens = await select_all_tokens(session)

        for token in tokens:
            account = tokens.account
            domain = account.properties.split("/")[0]
            try:
                resp = await self._refresh_subs(token, domain)
            except Exception as e:
                logger.error(f"ERROR UPDATE TOKEN {token.id}: {e}")
                continue

            subs = resp.get("activeUserSubscriptions", [])
            try:
                sub = subs[0]
                units = sub.get("units")
                alias = sub.get("tariffAlias")
                expires = sub.get("expiresAt")
            except IndexError as e:
                logger.error(f"No active subscribes for {token.id}: {e}")
                continue
            units_response = await self._refresh_units(token, account.properties, units)
            units_info = units_response.get("stores", [])
            try:
                for unit in units_info:
                    if str(unit.get("id")) not in uuids:
                        await insert_unit(session, unit)
                    await insert_subs(session, unit, alias, expires, token.id)
            except Exception as e:
                logger.error(f"incorrect units for {token.id}: {e}")
            await asyncio.sleep(self.delay)
