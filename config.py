from environs import Env
from dataclasses import dataclass
import os

@dataclass
class Database:
    name: str
    user: str
    password: str
    host: str

@dataclass
class Config:
    dbase: Database

def load_config(path=None) -> Config:
    env = Env()
    env.read_env(path)
    return Config(
        dbase=Database(
            name=os.getenv('DBASE_NAME'),
            user=os.getenv('DBASE_USER'),
            password=os.getenv('DBASE_PASSWORD'),
            host=os.getenv('DBASE_HOST'))
    )
