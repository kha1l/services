from sqlalchemy import (
    Column, String, Integer, BigInteger, Boolean, TIMESTAMP, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Franchisee(Base):
    __tablename__ = "franchisee"
    __table_args__ = {"schema": "special"}

    uuid = Column(String, primary_key=True)
    name = Column(String(200))
    group_uuid = Column(String(200))
    country = Column(String(10))

    units = relationship("Unit", back_populates="franchisee")

class Unit(Base):
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
    __tablename__ = "users"
    __table_args__ = {"schema": "special"}

    id = Column(Integer, primary_key=True)
    platform_id = Column(BigInteger)
    username = Column(String)
    name = Column(String)
    platform = Column(String(20))

    accounts = relationship("UserAccount", back_populates="user", cascade="all, delete")

class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = {"schema": "special"}

    id = Column(Integer, primary_key=True)
    sub = Column(String(100))
    name = Column(String(100))
    properties = Column(String(100))
    active = Column(Boolean)

    users = relationship("UserAccount", back_populates="account", cascade="all, delete")
    tokens = relationship("Token", back_populates="account")

class UserAccount(Base):
    __tablename__ = "user_account"
    __table_args__ = {"schema": "special"}

    user_id = Column(Integer, ForeignKey("special.users.id", ondelete="CASCADE"), primary_key=True)
    account_id = Column(Integer, ForeignKey("special.accounts.id", ondelete="CASCADE"), primary_key=True)

    user = relationship("User", back_populates="accounts")
    account = relationship("Account", back_populates="users")

class Application(Base):
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

    tokens = relationship("Token", back_populates="application")

class Token(Base):
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

class Subscribe(Base):
    __tablename__ = "subscribes"
    __table_args__ = {"schema": "special"}

    uuid = Column(String, ForeignKey("special.units.uuid", ondelete="CASCADE"), primary_key=True)
    token_id = Column(Integer, ForeignKey("special.tokens.id", ondelete="CASCADE"), primary_key=True)
    expires = Column(TIMESTAMP)
    alias = Column(String(50))

    unit = relationship("Unit", back_populates="subscribes")
    token = relationship("Token", back_populates="subscribes")
