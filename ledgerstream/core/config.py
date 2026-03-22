from dotenv import load_dotenv
from dataclasses import dataclass
import os

@dataclass
class Loadenv:
    KAFKA_BOOTSTRAP_SERVERS : str
    KAFKA_TOPIC : str
    SCHEMA_REGISTRY_URL : str

    @classmethod
    def from_env(cls) -> None:
        load_dotenv()
        return cls (
                KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                KAFKA_TOPIC = os.getenv('KAFKA_TOPIC'),
                SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
        )
        