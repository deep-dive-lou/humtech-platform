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

    # Document portal
    portal_tenant_slug: str = os.getenv("PORTAL_TENANT_SLUG", "humtech")
    portal_base_url: str = os.getenv("PORTAL_BASE_URL", "http://127.0.0.1:8000")
    spaces_region: str = os.getenv("SPACES_REGION", "")
    spaces_bucket: str = os.getenv("SPACES_BUCKET", "")
    spaces_endpoint: str = os.getenv("SPACES_ENDPOINT", "")
    spaces_key: str = os.getenv("SPACES_KEY", "")
    spaces_secret: str = os.getenv("SPACES_SECRET", "")
    portal_jwt_secret: str = os.getenv("PORTAL_JWT_SECRET", "dev-secret-change-in-prod")

settings = Settings()
