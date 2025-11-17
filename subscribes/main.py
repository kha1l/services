import asyncio
from logger import get_logger
from datetime import datetime

logger = get_logger(__name__)

async def main() -> None:
    logger.info(f"START SUBS&UNITS: {datetime.now()}")

    logger.info(f"END SUBS&UNITS: {datetime.now()}")

if __name__ == '__main__':
    asyncio.run(main())