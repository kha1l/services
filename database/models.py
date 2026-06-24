"""SQLAlchemy-модели схемы `special`, которые используют job проекта."""

from sqlalchemy import (
    Column, String, Integer, BigInteger, Boolean, TIMESTAMP, ForeignKey, Text, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Franchisee(Base):
    """Справочник франчайзи и сетевых групп."""

    __tablename__ = "franchisee"
    __table_args__ = {"schema": "special"}

    uuid = Column(String, primary_key=True)
    name = Column(String(200))
    group_uuid = Column(String(200))
    country = Column(String(10))

    units = relationship("Unit", back_populates="franchisee")


class Unit(Base):
    """Заведение/точка, привязанная к франчайзи."""

    __tablename__ = "units"
    __table_args__ = {"schema": "special"}

    uuid = Column(String, ForeignKey("special.franchisee.uuid"), primary_key=True)
    name = Column(String(200))
    timezone = Column(Integer)
    latitude = Column(String(50))
    longitude = Column(String(50))
    country = Column(String(10))

    franchisee = relationship("Franchisee", back_populates="units")
    subscribes = relationship("Subscribe", back_populates="unit")


class User(Base):
    """Пользователь, которого потом можно уведомлять в Telegram."""

    __tablename__ = "users"
    __table_args__ = {"schema": "special"}

    id = Column(Integer, primary_key=True)
    platform_id = Column(BigInteger)
    username = Column(String)
    name = Column(String)
    platform = Column(String(20))

    accounts = relationship("UserAccount", back_populates="user", cascade="all, delete")


class Account(Base):
    """Dodo-аккаунт пользователя с признаком активности токена."""

    __tablename__ = "accounts"
    __table_args__ = {"schema": "special"}

    id = Column(Integer, primary_key=True)
    sub = Column(String(100))
    name = Column(String(100))
    properties = Column(String(100))
    active = Column(Boolean)
    email = Column(String(150))

    users = relationship("UserAccount", back_populates="account", cascade="all, delete")
    tokens = relationship("Token", back_populates="account")


class UserAccount(Base):
    """Связь many-to-many между пользователями и Dodo-аккаунтами."""

    __tablename__ = "user_account"
    __table_args__ = {"schema": "special"}

    user_id = Column(Integer, ForeignKey("special.users.id", ondelete="CASCADE"), primary_key=True)
    account_id = Column(Integer, ForeignKey("special.accounts.id", ondelete="CASCADE"), primary_key=True)

    user = relationship("User", back_populates="accounts")
    account = relationship("Account", back_populates="users")


class Application(Base):
    """OAuth-приложение Dodo, через которое обновляются токены."""

    __tablename__ = "applications"
    __table_args__ = {"schema": "special"}

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    client_id = Column(String(200))
    secret = Column(String(200))
    redirect_io = Column(String(200))
    redirect_com = Column(String(200))
    challenge = Column(String(200))
    verifier = Column(String(200))
    scope = Column(String)
    on_marketplace = Column(Boolean)

    tokens = relationship("Token", back_populates="application")


class Token(Base):
    """Access/refresh токен для конкретного аккаунта и OAuth-приложения."""

    __tablename__ = "tokens"
    __table_args__ = {"schema": "special"}

    id = Column(Integer, primary_key=True)
    access = Column(String)
    refresh = Column(String)
    last_update = Column(TIMESTAMP)

    account_id = Column(Integer, ForeignKey("special.accounts.id"))
    application_id = Column(Integer, ForeignKey("special.applications.id"))

    account = relationship("Account", back_populates="tokens")
    application = relationship("Application", back_populates="tokens")
    subscribes = relationship("Subscribe", back_populates="token", cascade="all, delete")
    service_alerts = relationship("ServiceAlert", back_populates="token", cascade="all, delete")


class Subscribe(Base):
    """Premium-подписка токена на конкретную точку."""

    __tablename__ = "subscribes"
    __table_args__ = {"schema": "special"}

    uuid = Column(String, ForeignKey("special.units.uuid", ondelete="CASCADE"), primary_key=True)
    token_id = Column(Integer, ForeignKey("special.tokens.id", ondelete="CASCADE"), primary_key=True)
    expires = Column(TIMESTAMP)
    alias = Column(String(50))

    unit = relationship("Unit", back_populates="subscribes")
    token = relationship("Token", back_populates="subscribes")


class ServiceAlert(Base):
    """Состояние сервисного алерта, чтобы не спамить одними и теми же ошибками."""

    __tablename__ = "service_alerts"
    __table_args__ = (
        UniqueConstraint("service", "token_id", "kind", name="uq_service_alert_service_token_kind"),
        {"schema": "special"},
    )

    id = Column(Integer, primary_key=True)
    service = Column(String(50), nullable=False)
    token_id = Column(Integer, ForeignKey("special.tokens.id", ondelete="CASCADE"), nullable=False)
    kind = Column(String(50), nullable=False)
    first_seen_at = Column(TIMESTAMP, nullable=False)
    last_seen_at = Column(TIMESTAMP, nullable=False)
    notified_at = Column(TIMESTAMP)
    resolved_at = Column(TIMESTAMP)
    last_error = Column(Text)

    token = relationship("Token", back_populates="service_alerts")
