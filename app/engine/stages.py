"""
Canonical pipeline stages and ordering.

Stages are string constants, not a Postgres ENUM.
Adding a new stage requires only a code change, not a migration.

Not every tenant uses every stage — stage_mappings.stage_order per tenant
determines the actual funnel for that client.
"""
from __future__ import annotations

# Canonical stages in pipeline order
LEAD_CREATED = "lead_created"
NO_COMMS = "no_comms"
PROCESSING = "processing"
LEAD_QUALIFIED = "lead_qualified"
APPOINTMENT_BOOKED = "appointment_booked"
APPOINTMENT_COMPLETED = "appointment_completed"
PROPOSAL_SENT = "proposal_sent"
LEAD_WON = "lead_won"
REVENUE_COLLECTED = "revenue_collected"

# Terminal (from any stage)
LEAD_LOST = "lead_lost"
REJECTED = "rejected"

# Ordered pipeline (excludes terminals — they exit from any stage)
PIPELINE_ORDER: list[str] = [
    LEAD_CREATED,
    NO_COMMS,
    PROCESSING,
    LEAD_QUALIFIED,
    APPOINTMENT_BOOKED,
    APPOINTMENT_COMPLETED,
    PROPOSAL_SENT,
    LEAD_WON,
    REVENUE_COLLECTED,
]

# stage -> position (1-indexed to match stage_mappings.stage_order)
STAGE_INDEX: dict[str, int] = {s: i + 1 for i, s in enumerate(PIPELINE_ORDER)}
STAGE_INDEX[REJECTED] = 98
STAGE_INDEX[LEAD_LOST] = 99

# Terminal stages (exits, not funnel progression)
TERMINAL_STAGES: frozenset[str] = frozenset(["won", "lost", REJECTED])

# All valid canonical stages
ALL_STAGES: frozenset[str] = frozenset(PIPELINE_ORDER) | {LEAD_LOST, REJECTED}

# Valid event types (from schema doc)
EVENT_TYPES: frozenset[str] = frozenset([
    "lead_created",
    "stage_changed",
    "appointment_booked",
    "appointment_completed",
    "appointment_no_show",
    "proposal_sent",
    "lead_won",
    "lead_lost",
    "cash_collected",
    "value_changed",
    "first_contact",
])
