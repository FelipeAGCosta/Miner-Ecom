import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "")

def make_engine():
    if not all([DB_HOST, DB_PORT, DB_USER, DB_NAME]) or DB_PASS is None:
        raise RuntimeError(
            "Variáveis de DB ausentes no .env (DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME)."
        )
    url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)
