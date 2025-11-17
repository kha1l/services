import asyncio
from logger import get_logger
from datetime import datetime
from database.engine import session_maker
from franchisee_update import FranchiseeRefresher

logger = get_logger(__name__)

async def main() -> None:
    logger.info(f"START FRANCHISEE: {datetime.now()}")
    async with session_maker() as session:
        refresher = FranchiseeRefresher()
        await refresher.refresh_franchisee(session)
    logger.info(f"END FRANCHISEE: {datetime.now()}")

if __name__ == '__main__':
    asyncio.run(main())
