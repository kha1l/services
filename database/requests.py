"""Набор SQLAlchemy-запросов для job `servises`."""

from datetime import datetime

from sqlalchemy import delete, exists, select, text

from database.models import (
    Token,
    Application,
    Account,
    Franchisee,
    Unit,
    Subscribe,
    User,
    UserAccount,
    ServiceAlert,
)
from iso3166 import countries


async def select_all_tokens(session):
    """Возвращает все токены вместе с приложением и аккаунтом."""
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
    """Обновляет access/refresh токен и помечает аккаунт активным."""
    token.access = access
    token.refresh = refresh
    token.last_update = datetime.now()
    if token.account is not None:
        token.account.active = True

    await session.commit()


async def select_uuids(session):
    """Возвращает все UUID франчайзи, уже сохраненные в БД."""
    stmt = select(Franchisee.uuid)
    result = await session.execute(stmt)
    return [row[0] for row in result.all()]


async def insert_unit_franchisee(session, unit_data: dict):
    """Добавляет новую запись франчайзи, если ее раньше не было в локальном справочнике."""
    country = countries.get(unit_data.get("countryCode")).alpha2.lower()
    new_unit = Franchisee(
        name=unit_data.get("name"),
        group_uuid=unit_data.get("franchiseeId"),
        uuid=unit_data.get("id"),
        country=country
    )
    session.add(new_unit)
    await session.commit()


async def select_units(session):
    """Возвращает список UUID заведений из локальной БД."""
    stmt = select(Unit.uuid)
    result = await session.execute(stmt)
    return [row[0] for row in result.all()]


async def insert_unit(session, unit_data: dict):
    """Добавляет новое заведение в локальный справочник."""
    new_unit = Unit(
        name=unit_data.get("name"),
        uuid=unit_data.get("id"),
        country=unit_data.get("countryId").lower(),
        latitude=unit_data.get("location")["latitude"],
        longitude=unit_data.get("location")["longitude"],
        timezone=unit_data.get("dateTimeInfo")["timeZoneShift"]
    )
    session.add(new_unit)
    await session.commit()


async def insert_subs(session, unit: dict, alias, expires, token):
    """Создает или обновляет premium-подписку токена на заведение."""
    uuid = unit.get("id")
    select_stmt = select(Subscribe).where(
        Subscribe.uuid == uuid,
        Subscribe.token_id == token,
    )
    result = await session.execute(select_stmt)
    existing_subs = result.scalars().first()
    if existing_subs:
        existing_subs.expires = expires
        existing_subs.alias = alias
    else:
        new_subs = Subscribe(
            uuid=uuid,
            token_id=token,
            expires=expires,
            alias=alias
        )
        session.add(new_subs)
    await session.commit()


async def select_subscribe_uuids_for_token(session, token_id: int) -> set[str]:
    """Возвращает все UUID заведений, на которые сейчас подписан токен."""
    stmt = select(Subscribe.uuid).where(Subscribe.token_id == token_id)
    result = await session.execute(stmt)
    return {str(row[0]) for row in result.all() if row[0] is not None}


async def select_subscribe_snapshot_for_token(session, token_id: int) -> dict[str, tuple[str | None, str]]:
    """Возвращает снимок текущих premium-подписок токена для сравнения до/после sync."""
    stmt = (
        select(Subscribe.uuid, Subscribe.alias, Unit.name)
        .outerjoin(Unit, Unit.uuid == Subscribe.uuid)
        .where(Subscribe.token_id == token_id)
    )
    result = await session.execute(stmt)
    return {
        str(uuid): (alias, unit_name or str(uuid))
        for uuid, alias, unit_name in result.all()
        if uuid is not None
    }


async def delete_subscribes_and_orphan_units(session, token_id: int, unit_uuids: set[str]) -> set[str]:
    """Удаляет подписки токена и очищает точки, которые больше никем не используются."""
    if not unit_uuids:
        return set()

    await session.execute(
        delete(Subscribe).where(
            Subscribe.token_id == token_id,
            Subscribe.uuid.in_(unit_uuids),
        )
    )

    orphaned_unit_uuids: list[str] = []
    for unit_uuid in unit_uuids:
        has_other_subscribes_stmt = select(
            exists().where(Subscribe.uuid == unit_uuid)
        )
        has_other_subscribes = await session.scalar(has_other_subscribes_stmt)
        if not has_other_subscribes:
            orphaned_unit_uuids.append(unit_uuid)

    if orphaned_unit_uuids:
        await session.execute(
            delete(Unit).where(Unit.uuid.in_(orphaned_unit_uuids))
        )

    await session.commit()
    return set(orphaned_unit_uuids)


async def select_account_chat_ids(session, account_id: int) -> list[int]:
    """Возвращает Telegram chat_id всех пользователей, связанных с аккаунтом."""
    stmt = (
        select(User.platform_id)
        .join(UserAccount, UserAccount.user_id == User.id)
        .where(
            UserAccount.account_id == account_id,
            User.platform == "telegram",
            User.platform_id.is_not(None),
        )
        .distinct()
    )
    result = await session.execute(stmt)
    return [int(row[0]) for row in result.all() if row[0] is not None]


async def ensure_service_alerts_table(session) -> None:
    """Создает таблицу service_alerts, если она еще не появилась в схеме."""
    await session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS special.service_alerts (
                id SERIAL PRIMARY KEY,
                service VARCHAR(50) NOT NULL,
                token_id INTEGER NOT NULL REFERENCES special.tokens(id) ON DELETE CASCADE,
                kind VARCHAR(50) NOT NULL,
                first_seen_at TIMESTAMP NOT NULL,
                last_seen_at TIMESTAMP NOT NULL,
                notified_at TIMESTAMP NULL,
                resolved_at TIMESTAMP NULL,
                last_error TEXT NULL,
                CONSTRAINT uq_service_alert_service_token_kind UNIQUE (service, token_id, kind)
            )
            """
        )
    )
    await session.commit()


async def touch_service_alert(session, service: str, token_id: int, kind: str, error_text: str) -> ServiceAlert:
    """Открывает или обновляет алерт сервиса по токену."""
    await ensure_service_alerts_table(session)
    stmt = (
        select(ServiceAlert)
        .where(
            ServiceAlert.service == service,
            ServiceAlert.token_id == token_id,
            ServiceAlert.kind == kind,
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    alert = result.scalars().first()
    now = datetime.now()

    if alert is None:
        alert = ServiceAlert(
            service=service,
            token_id=token_id,
            kind=kind,
            first_seen_at=now,
            last_seen_at=now,
            last_error=error_text,
        )
        session.add(alert)
    else:
        if alert.resolved_at is not None:
            alert.first_seen_at = now
            alert.notified_at = None
            alert.resolved_at = None
        alert.last_seen_at = now
        alert.last_error = error_text

    await session.commit()
    await session.refresh(alert)
    return alert


async def mark_service_alert_notified(session, alert: ServiceAlert) -> None:
    """Помечает, что уведомление по алерту уже было отправлено."""
    alert.notified_at = datetime.now()
    await session.commit()


async def resolve_service_alert(session, service: str, token_id: int, kind: str) -> None:
    """Закрывает активный алерт после успешной обработки токена."""
    await ensure_service_alerts_table(session)
    stmt = (
        select(ServiceAlert)
        .where(
            ServiceAlert.service == service,
            ServiceAlert.token_id == token_id,
            ServiceAlert.kind == kind,
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    alert = result.scalars().first()
    if alert is None or alert.resolved_at is not None:
        return

    alert.resolved_at = datetime.now()
    await session.commit()


async def delete_token_with_relations(session, token: Token) -> None:
    """Удаляет токен и связанные с ним записи по каскаду."""
    await session.delete(token)
    await session.commit()


async def set_account_active(session, account_id: int, active: bool) -> None:
    """Меняет флаг активности аккаунта, если он действительно изменился."""
    stmt = select(Account).where(Account.id == account_id).limit(1)
    result = await session.execute(stmt)
    account = result.scalars().first()
    if account is None:
        return

    if account.active is active:
        return

    account.active = active
    await session.commit()
