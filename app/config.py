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

    # Outreach pipeline
    apollo_api_key: str = os.getenv("APOLLO_API_KEY", "")
    apify_api_key: str = os.getenv("APIFY_API_KEY", "")
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
    instantly_api_key: str = os.getenv("INSTANTLY_API_KEY", "")
    instantly_campaign_id: str = os.getenv("INSTANTLY_CAMPAIGN_ID", "")

settings = Settings()
