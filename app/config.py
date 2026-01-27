import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()  # loads .env for local dev

@dataclass(frozen=True)
class Settings:
    env: str = os.getenv("ENV", "local")
    service_name: str = os.getenv("SERVICE_NAME", "humtech-worker")
    worker_id: str = os.getenv("WORKER_ID", "worker-1")
    database_url: str = os.getenv("DATABASE_URL", "")

settings = Settings()
