import asyncio
from logger import get_logger
from datetime import datetime
from database.engine import session_maker
from tokens_update import TokenRefresher

logger = get_logger(__name__)

async def main() -> None:
    logger.info(f"START TOKENS: {datetime.now()}")
    async with session_maker() as session:
        refresher = TokenRefresher()
        await refresher.refresh_all_tokens(session)
    logger.info(f"END TOKENS: {datetime.now()}")

if __name__ == '__main__':
    asyncio.run(main())