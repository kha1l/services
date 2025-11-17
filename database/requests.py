from sqlalchemy import select
from database.models import Token, Application, Account, Franchisee
from datetime import datetime
from iso3166 import countries


async def select_all_tokens(session):
    stmt = (
        select(Token)
        .join(Application, Application.id == Token.application_id)
        .join(Account, Account.id == Token.account_id)
    )

    result = await session.scalars(stmt)
    tokens = result.all()

    for t in tokens:
        await session.refresh(t, ["application", "account"])

    return tokens


async def update_token(session, token: Token, access: str, refresh: str):
    token.access = access
    token.refresh = refresh
    token.last_update = datetime.now()

    await session.commit()


async def select_uuids(session):
    stmt = select(Franchisee.uuid)
    result = await session.execute(stmt)
    return [row[0] for row in result.all()]


async def insert_unit(session, unit_data: dict):
    country = countries.get(unit_data.get("countryCode")).alpha2.lower()
    new_unit = Franchisee(
        name=unit_data.get("name"),
        group_uuid=unit_data.get("franchiseeId"),
        uuid=unit_data.get("id"),
        country=country
    )
    session.add(new_unit)
    await session.commit()


async def select_tokens(session):
    stmt = select(Token)
    result = await session.execute(stmt)
    return result.all()
