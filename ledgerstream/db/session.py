from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from core.config import Loadenv

env = Loadenv.from_env()

DATABASE_URL = (
    f"postgresql+psycopg2://{env.POSTGRES_USER}:{env.POSTGRES_PASSWORD}"
    f"@{env.POSTGRES_HOST}:{env.POSTGRES_PORT}/{env.POSTGRES_DB}"
)

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
