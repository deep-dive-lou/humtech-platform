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
    anthropic_api_key_outreach: str = os.getenv("ANTHROPIC_API_KEY_OUTREACH", "")
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

    # Optimisation engine
    optimiser_jwt_secret: str = os.getenv("OPTIMISER_JWT_SECRET", "dev-optimiser-secret-change-in-prod")

    # Analytics command centre
    analytics_jwt_secret: str = os.getenv("ANALYTICS_JWT_SECRET", "dev-analytics-secret-change-in-prod")
    analytics_password_hash: str = os.getenv("ANALYTICS_PASSWORD_HASH", "")

    # AWS SES (email sending)
    aws_ses_region: str = os.getenv("AWS_SES_REGION", "eu-west-2")
    aws_ses_access_key_id: str = os.getenv("AWS_SES_ACCESS_KEY_ID", "")
    aws_ses_secret_access_key: str = os.getenv("AWS_SES_SECRET_ACCESS_KEY", "")

    # Monitoring
    slack_webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", "")
    monitor_interval_seconds: int = int(os.getenv("MONITOR_INTERVAL_SECONDS", "300"))

settings = Settings()
