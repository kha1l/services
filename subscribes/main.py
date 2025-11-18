import asyncio
from logger import get_logger
from datetime import datetime
from database.engine import session_maker
from subs_update import SubscribeRefresher

logger = get_logger(__name__)

async def main() -> None:
    logger.info(f"START SUBS&UNITS: {datetime.now()}")
    async with session_maker() as session:
        refresher = SubscribeRefresher()
        await refresher.refresh_subscribe(session)
    logger.info(f"END SUBS&UNITS: {datetime.now()}")

if __name__ == '__main__':
    asyncio.run(main())