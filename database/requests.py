"""Набор SQLAlchemy-запросов для job `servises`."""

from datetime import datetime
from dataclasses import dataclass

from sqlalchemy import delete, exists, func, or_, select, text

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
    ReportOrder,
)
from resources.token_auth import token_auth_client
from security_logging import safe_error_type
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

    await token_auth_client().decrypt_tokens(tokens)
    return tokens


async def select_marketplace_tokens(session):
    """Возвращает токены приложений, которые должны проверять marketplace-подписки."""
    stmt = (
        select(Token)
        .join(Application, Application.id == Token.application_id)
        .join(Account, Account.id == Token.account_id)
        .where(Application.on_marketplace.is_(True))
    )

    result = await session.scalars(stmt)
    tokens = result.all()

    for t in tokens:
        await session.refresh(t, ["application", "account"])

    await token_auth_client().decrypt_tokens(tokens)
    return tokens


async def update_token(session, token: Token, access: str, refresh: str):
    """Обновляет access/refresh через dodovkus/auth и помечает аккаунт активным."""
    await token_auth_client().persist_updated_token(token, access=access, refresh=refresh)
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
    location = unit_data.get("location") or {}
    new_unit = Unit(
        name=unit_data.get("name"),
        uuid=unit_data.get("id"),
        country=unit_data.get("countryId").lower(),
        latitude=str(location.get("latitude")) if location.get("latitude") is not None else None,
        longitude=str(location.get("longitude")) if location.get("longitude") is not None else None,
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


async def prune_report_order_units_for_account(
    session,
    account_id: int | None,
    candidate_unit_uuids: set[str],
) -> tuple[int, int, int]:
    """Удаляет из настроек отчётов только что утраченные аккаунтом точки.

    Возвращает количество обновлённых и удалённых настроек, а также число
    удалённых ссылок на заведения. Порядок доступных UUID сохраняется.
    """
    if account_id is None or not candidate_unit_uuids:
        return 0, 0, 0

    still_accessible_stmt = (
        select(Subscribe.uuid)
        .join(Token, Token.id == Subscribe.token_id)
        .where(
            Token.account_id == account_id,
            Subscribe.uuid.in_(candidate_unit_uuids),
            or_(Subscribe.expires.is_(None), Subscribe.expires >= func.now()),
        )
        .distinct()
    )
    still_accessible_uuids = {
        str(unit_uuid)
        for unit_uuid in (await session.execute(still_accessible_stmt)).scalars().all()
        if unit_uuid
    }
    inaccessible_uuids = candidate_unit_uuids - still_accessible_uuids
    if not inaccessible_uuids:
        return 0, 0, 0

    orders_stmt = select(ReportOrder).where(ReportOrder.account_id == account_id)
    orders = (await session.execute(orders_stmt)).scalars().all()

    updated_orders = 0
    deleted_orders = 0
    removed_references = 0
    for order in orders:
        original_uuids = [str(unit_uuid) for unit_uuid in (order.uuids or []) if unit_uuid]
        retained_uuids = list(
            dict.fromkeys(
                unit_uuid
                for unit_uuid in original_uuids
                if unit_uuid not in inaccessible_uuids
            )
        )
        removed_references += len(original_uuids) - len(retained_uuids)
        if retained_uuids == original_uuids:
            continue
        if retained_uuids:
            order.uuids = retained_uuids
            updated_orders += 1
        else:
            await session.delete(order)
            deleted_orders += 1

    await session.commit()
    return updated_orders, deleted_orders, removed_references


async def select_account_chat_ids(session, account_id: int) -> list[int]:
    """Возвращает chat_id всех пользователей, связанных с аккаунтом."""
    rows = await select_account_recipients(session, account_id)
    return [row.chat_id for row in rows]


@dataclass(frozen=True)
class AccountRecipient:
    """Получатель сервисного уведомления по аккаунту."""

    chat_id: int
    platform: str


async def select_account_recipients(session, account_id: int) -> list[AccountRecipient]:
    """Возвращает chat_id + platform всех пользователей, связанных с аккаунтом."""
    stmt = (
        select(User.platform_id, User.platform)
        .join(UserAccount, UserAccount.user_id == User.id)
        .where(
            UserAccount.account_id == account_id,
            User.platform_id.is_not(None),
            User.platform.in_(("telegram", "max")),
        )
        .distinct()
    )
    result = await session.execute(stmt)
    recipients: list[AccountRecipient] = []
    for platform_id, platform in result.all():
        if platform_id is None:
            continue
        normalized_platform = str(platform or "telegram").strip().lower() or "telegram"
        recipients.append(
            AccountRecipient(
                chat_id=int(platform_id),
                platform=normalized_platform,
            )
        )
    return recipients


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


async def touch_service_alert(session, service: str, token_id: int, kind: str, error_type: str) -> ServiceAlert:
    """Открывает или обновляет алерт сервиса по токену."""
    normalized_error_type = safe_error_type(error_type)
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
            last_error=normalized_error_type,
        )
        session.add(alert)
    else:
        if alert.resolved_at is not None:
            alert.first_seen_at = now
            alert.notified_at = None
            alert.resolved_at = None
        alert.last_seen_at = now
        alert.last_error = normalized_error_type

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
