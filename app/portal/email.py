"""
Amazon SES email sending for the document portal.

Singleton pattern matches storage.py (Spaces client).
"""
import logging
import os

import boto3
from botocore.exceptions import ClientError
from jinja2 import Template

from app.config import settings

log = logging.getLogger(__name__)

_ses = None


def _get_ses():
    """Lazy-init boto3 SES client."""
    global _ses
    if _ses is None:
        region = settings.aws_ses_region
        key_id = settings.aws_ses_access_key_id
        secret = settings.aws_ses_secret_access_key
        missing = [
            k for k, v in {
                "AWS_SES_REGION": region,
                "AWS_SES_ACCESS_KEY_ID": key_id,
                "AWS_SES_SECRET_ACCESS_KEY": secret,
            }.items() if not v
        ]
        if missing:
            raise RuntimeError(f"Missing SES env vars: {', '.join(missing)}")
        session = boto3.session.Session()
        _ses = session.client(
            "ses",
            region_name=region,
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
        )
    return _ses


# ---------------------------------------------------------------------------
# Default email copy
# ---------------------------------------------------------------------------

DEFAULT_SUBJECT = "Documents requested — {{ brand_name }}"

DEFAULT_BODY = (
    "We need a few documents from you. "
    "Please click the link below to view what's needed and upload securely."
)


# ---------------------------------------------------------------------------
# HTML email template (inline CSS — email clients ignore <style> blocks)
# ---------------------------------------------------------------------------

EMAIL_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f5;padding:40px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.08);">

  <!-- Header -->
  <tr>
    <td style="background:{{ brand_color }};padding:28px 32px;">
      {% if logo_url %}
      <img src="{{ logo_url }}" alt="{{ brand_name }}" style="max-height:40px;display:block;" />
      {% else %}
      <span style="color:#ffffff;font-size:20px;font-weight:600;">{{ brand_name }}</span>
      {% endif %}
    </td>
  </tr>

  <!-- Body -->
  <tr>
    <td style="padding:32px;">
      <p style="margin:0 0 16px;font-size:16px;color:#1a1a1a;">Hi {{ client_name }},</p>
      <p style="margin:0 0 24px;font-size:15px;color:#4a4a4a;line-height:1.6;">{{ body_text }}</p>

      {% if due_date %}
      <p style="margin:0 0 24px;font-size:14px;color:#71717a;">
        <strong>Due by:</strong> {{ due_date }}
      </p>
      {% endif %}

      {% if magic_link %}
      <!-- CTA Button -->
      <table cellpadding="0" cellspacing="0" style="margin:0 auto 24px;">
      <tr>
        <td style="background:{{ brand_color }};border-radius:8px;">
          <a href="{{ magic_link }}"
             style="display:inline-block;padding:14px 32px;color:#ffffff;font-size:15px;font-weight:600;text-decoration:none;">
            View &amp; Upload Documents
          </a>
        </td>
      </tr>
      </table>

      <p style="margin:0;font-size:13px;color:#a1a1aa;line-height:1.5;">
        Or copy this link:<br/>
        <a href="{{ magic_link }}" style="color:{{ brand_color }};word-break:break-all;">{{ magic_link }}</a>
      </p>
      {% endif %}
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="padding:20px 32px;border-top:1px solid #f0f0f0;">
      <p style="margin:0;font-size:12px;color:#a1a1aa;">
        Sent by {{ brand_name }}{% if brand_name != 'HumTech' %} via HumTech{% endif %}
      </p>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>
"""

_email_template = Template(EMAIL_HTML)
_subject_template = Template(DEFAULT_SUBJECT)


def render_email(
    *,
    client_name: str,
    magic_link: str,
    due_date: str | None,
    body_text: str,
    brand_name: str,
    brand_color: str,
    logo_url: str | None,
) -> str:
    """Render the magic-link email HTML."""
    return _email_template.render(
        client_name=client_name,
        magic_link=magic_link,
        due_date=due_date,
        body_text=body_text,
        brand_name=brand_name,
        brand_color=brand_color or "#111827",
        logo_url=logo_url,
    )


def render_subject(
    *,
    custom_subject: str | None,
    brand_name: str,
    client_name: str,
) -> str:
    """Render the email subject line."""
    if custom_subject:
        return Template(custom_subject).render(
            brand_name=brand_name, client_name=client_name,
        )
    return _subject_template.render(brand_name=brand_name, client_name=client_name)


def send_email(
    *,
    to_email: str,
    from_email: str,
    subject: str,
    html_body: str,
) -> str:
    """Send an email via SES. Returns the SES MessageId."""
    ses = _get_ses()
    try:
        resp = ses.send_email(
            Source=from_email,
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                },
            },
        )
        message_id = resp["MessageId"]
        log.info("SES email sent to=%s message_id=%s", to_email, message_id)
        return message_id
    except ClientError as e:
        log.error("SES send failed to=%s error=%s", to_email, e)
        raise


# ---------------------------------------------------------------------------
# SES domain verification helpers (for settings page)
# ---------------------------------------------------------------------------

def verify_domain(domain: str) -> list[dict]:
    """
    Start domain verification + DKIM setup in SES.
    Returns list of DKIM CNAME records to add to DNS.
    """
    ses = _get_ses()
    # Start domain identity verification
    ses.verify_domain_identity(Domain=domain)
    # Get DKIM tokens
    dkim_resp = ses.verify_domain_dkim(Domain=domain)
    tokens = dkim_resp["DkimTokens"]
    return [
        {
            "name": f"{token}._domainkey.{domain}",
            "type": "CNAME",
            "value": f"{token}.dkim.amazonses.com",
        }
        for token in tokens
    ]


def check_domain_verification(domain: str) -> bool:
    """Check if a domain is verified in SES."""
    ses = _get_ses()
    resp = ses.get_identity_verification_attributes(Identities=[domain])
    attrs = resp.get("VerificationAttributes", {}).get(domain, {})
    return attrs.get("VerificationStatus") == "Success"
