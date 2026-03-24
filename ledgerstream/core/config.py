from dotenv import load_dotenv
from dataclasses import dataclass
import os

@dataclass
class Loadenv:
    KAFKA_BOOTSTRAP_SERVERS : str
    KAFKA_TOPIC : str
    SCHEMA_REGISTRY_URL : str

    POSTGRES_USER : str
    POSTGRES_PASSWORD : str
    POSTGRES_HOST : str
    POSTGRES_PORT : int
    POSTGRES_DB : str

    API_URL : str

    JWT_SECRET_KEY : str
    JWT_ALGORITHM : str
    JWT_EXPIRE_MINUTES : int

    @classmethod
    def from_env(cls) -> None:
        load_dotenv()
        return cls (
                KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                KAFKA_TOPIC = os.getenv('KAFKA_TOPIC'),
                SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL'),
                
                POSTGRES_USER = os.getenv('POSTGRES_USER'),
                POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD'),
                POSTGRES_HOST = os.getenv('POSTGRES_HOST'),
                POSTGRES_PORT = int(os.getenv('POSTGRES_PORT')),
                POSTGRES_DB = os.getenv('POSTGRES_DB'),
                API_URL = os.getenv('API_URL'),

                JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY'),
                JWT_ALGORITHM = os.getenv('JWT_ALGORITHM'),
                JWT_EXPIRE_MINUTES = int(os.getenv('JWT_EXPIRE_MINUTES'))

        )
        