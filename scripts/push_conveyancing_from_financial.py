"""
Take the LIVE financial-services-march26 sequence from Instantly,
adapt industry-specific references for conveyancing, and push to
the conveyancing-q1-2026 campaign.

Preserves all HTML structure, spacing, and hyperlinks.
"""

import copy
import json
import httpx

API_KEY = "ZDQ2ZDFlMGUtYWU1My00N2JhLWE2ODgtMDA5NmUxM2I1MjUxOmZvakl3QWZoUE1BbA=="
BASE_URL = "https://api.instantly.ai/api/v2"

SOURCE_CAMPAIGN_ID = "76fc48b8-aa92-426e-8291-212fa3b4ee39"  # financial-services-march26
TARGET_CAMPAIGN_ID = "8c3f515b-2058-45bc-af6f-9d03f47d0833"  # conveyancing-q1-2026

HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def adapt_for_conveyancing(text: str) -> str:
    """Apply industry-specific replacements for conveyancing sector."""
    replacements = [
        # Email 3 — sector reference + question
        ("financial services firms like", "property and conveyancing firms like"),
        ("clients call asking for financial advice, what % transact?",
         "potential clients enquire about conveyancing services, what % convert to instructed matters?"),
    ]

    for old, new in replacements:
        text = text.replace(old, new)

    return text


def main():
    # 1. Fetch the live financial-services sequence
    print("Fetching financial-services-march26 sequence...")
    resp = httpx.get(
        f"{BASE_URL}/campaigns/{SOURCE_CAMPAIGN_ID}",
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    source = resp.json()

    source_steps = source["sequences"][0]["steps"]
    print(f"  Found {len(source_steps)} email steps")

    # 2. Deep copy and adapt each step
    adapted_steps = copy.deepcopy(source_steps)
    changes_made = []

    for i, step in enumerate(adapted_steps):
        for variant in step.get("variants", []):
            original_subject = variant["subject"]
            original_body = variant["body"]

            variant["subject"] = adapt_for_conveyancing(variant["subject"])
            variant["body"] = adapt_for_conveyancing(variant["body"])

            if variant["subject"] != original_subject or variant["body"] != original_body:
                changes_made.append(f"  Email {i+1}: modified")
            else:
                changes_made.append(f"  Email {i+1}: unchanged (generic)")

    print("\nChanges per email:")
    for c in changes_made:
        print(c)

    # 3. Build payload and push
    payload = {
        "sequences": [{"steps": adapted_steps}],
    }

    # Save payload for reference
    with open("scripts/instantly_payload_conveyancing_v2.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\nPayload saved to scripts/instantly_payload_conveyancing_v2.json")

    print(f"\nPushing to conveyancing campaign {TARGET_CAMPAIGN_ID}...")
    resp = httpx.patch(
        f"{BASE_URL}/campaigns/{TARGET_CAMPAIGN_ID}",
        json=payload,
        headers=HEADERS,
        timeout=30,
    )

    if resp.status_code >= 400:
        print(f"ERROR {resp.status_code}: {resp.text}")
        raise SystemExit(1)

    result = resp.json()
    steps_count = len(result.get("sequences", [{}])[0].get("steps", []))
    print(f"SUCCESS - Campaign updated with {steps_count} email steps")
    print(f"Campaign: {result.get('name')}")


if __name__ == "__main__":
    main()