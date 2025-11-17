import aiohttp
import asyncio
from logger import get_logger
from database.requests import select_uuids, select_tokens, insert_unit

logger = get_logger(__name__)

async def get_franchisee(access: str):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "DodoVkus",
        "Authorization": f"Bearer {access}"
    }
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.get('https://api.dodois.io/franchisee/units', headers=headers) as response:
            if response.status != 200:
                return None
            return await response.json()


class FranchiseeRefresher:
    def __init__(self, delay: float = 1.0):
        self.delay = delay

    async def refresh_franchisee(self, session):
        uuids = await select_uuids(session)
        tokens = await select_tokens(session)

        for token in tokens:
            franc_data = await get_franchisee(token.access)
            if franc_data:
                for unit in franc_data.get("units", []):
                    if unit["id"] not in uuids:
                        try:
                            await insert_unit(session, unit)
                            logger.info(f'Inserted unit {unit["id"]}')
                        except Exception as e:
                            logger.error(f"Failed to insert unit {unit['id']}: {e}")

                        await asyncio.sleep(self.delay)
                break
