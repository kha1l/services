import aiohttp
import asyncio
from logger import get_logger
from database.requests import select_uuids, select_all_tokens, insert_unit_franchisee

logger = get_logger(__name__)

async def get_franchisee(access: str, domain: str):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "DodoVkus",
        "Authorization": f"Bearer {access}"
    }
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.get(f'https://api.dodois.{domain}/franchisee/units', headers=headers) as response:
            if response.status != 200:
                return None
            return await response.json()


class FranchiseeRefresher:
    def __init__(self, delay: float = 1.0):
        self.delay = delay

    async def refresh_franchisee(self, session):
        uuids = await select_uuids(session)
        tokens = await select_all_tokens(session)
        for token in tokens:
            account = token.account
            domain = account.properties.split("/")[0]
            franc_data = await get_franchisee(token.access, domain)
            if franc_data:
                for unit in franc_data.get("units", []):
                    if unit["id"] not in uuids:
                        try:
                            await insert_unit_franchisee(session, unit)
                            logger.info(f'Inserted unit {unit["id"]}')
                        except Exception as e:
                            logger.error(f"Failed to insert unit {unit['id']}: {e}")

                        await asyncio.sleep(self.delay)
                break
