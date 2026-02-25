import os
import boto3
from botocore.exceptions import ClientError

SPACES_REGION = os.getenv("SPACES_REGION")
SPACES_BUCKET = os.getenv("SPACES_BUCKET")
SPACES_ENDPOINT = os.getenv("SPACES_ENDPOINT")
SPACES_KEY = os.getenv("SPACES_KEY")
SPACES_SECRET = os.getenv("SPACES_SECRET")

_s3 = None


def _get_s3():
    global _s3
    if _s3 is None:
        missing = [
            k for k, v in {
                "SPACES_REGION": SPACES_REGION,
                "SPACES_BUCKET": SPACES_BUCKET,
                "SPACES_ENDPOINT": SPACES_ENDPOINT,
                "SPACES_KEY": SPACES_KEY,
                "SPACES_SECRET": SPACES_SECRET,
            }.items() if not v
        ]
        if missing:
            raise RuntimeError(f"Missing Spaces env vars: {', '.join(missing)}")
        session = boto3.session.Session()
        _s3 = session.client(
            "s3",
            region_name=SPACES_REGION,
            endpoint_url=SPACES_ENDPOINT,
            aws_access_key_id=SPACES_KEY,
            aws_secret_access_key=SPACES_SECRET,
        )
    return _s3


def presign_put(key: str, content_type: str, expires_seconds: int = 600) -> str:
    s3 = _get_s3()
    return s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": SPACES_BUCKET,
            "Key": key,
            "ContentType": content_type,
            "ACL": "private",
        },
        ExpiresIn=expires_seconds,
    )


def presign_get(key: str, expires_seconds: int = 300) -> str:
    s3 = _get_s3()
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": SPACES_BUCKET, "Key": key},
        ExpiresIn=expires_seconds,
    )


def head_object(key: str) -> dict | None:
    """HEAD object in Spaces. Returns {"size_bytes": int, "content_type": str} or None if missing."""
    s3 = _get_s3()
    try:
        resp = s3.head_object(Bucket=SPACES_BUCKET, Key=key)
        return {
            "size_bytes": resp["ContentLength"],
            "content_type": resp.get("ContentType"),
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return None
        raise
