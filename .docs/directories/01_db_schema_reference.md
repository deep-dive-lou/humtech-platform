# DB Schema Reference (Source of Truth)

Claude must not invent table/column names.
If a column is not listed here, it does not exist.

## Included tables
- core.tenants
- core.tenant_credentials
- bot.inbound_events
- bot.job_queue
- bot.contacts
- bot.conversations
- bot.messages

Schema: core
Table: core.tenants

Represents a single client (tenant) using the system.

Used for:

Adapter routing (calendar, messaging)

Tenant-specific configuration

Enable/disable gating

Columns
Column	Type	Notes
tenant_id	uuid	Primary key
tenant_slug	text	Unique, human-readable identifier
name	text	Display name
is_enabled	boolean	Hard on/off switch
messaging_adapter	text	e.g. ghl
calendar_adapter	text	e.g. ghl
settings	jsonb	Arbitrary tenant config (calendar_id, timezone, tokens)
created_at	timestamptz	
updated_at	timestamptz	
Constraints & Indexes

PRIMARY KEY (tenant_id)

UNIQUE (tenant_slug)

Index on is_enabled

Table: core.tenant_credentials

Stores encrypted credentials per tenant per provider (e.g., GHL tokens).

Credentials are encrypted at rest using Fernet (AES-128-CBC + HMAC).
Encryption key stored in TENANT_ENCRYPTION_KEY env var.

Columns
Column	Type	Notes
credential_id	uuid	Primary key
tenant_id	uuid	FK → core.tenants
provider	text	Adapter identifier (e.g. ghl, openai)
credentials	bytea	Encrypted JSON blob
key_version	int	For future key rotation (default: 1)
created_at	timestamptz
updated_at	timestamptz

Constraints & Indexes

PRIMARY KEY (credential_id)

UNIQUE (tenant_id, provider)

FK tenant_id → core.tenants(tenant_id) ON DELETE CASCADE

Schema: bot
Table: bot.inbound_events

Raw, idempotent inbound events from external systems (SMS, webhooks, forms).

This table is append-only.

Columns
Column	Type	Notes
inbound_event_id	uuid	Primary key
tenant_id	uuid	FK → core.tenants
provider	text	e.g. ghl
event_type	text	e.g. inbound_message
provider_event_id	text	Optional
provider_msg_id	text	Used for idempotency
channel	text	sms, whatsapp, etc
channel_address	text	Phone number / handle
received_at	timestamptz	Default now()
dedupe_key	text	Unique per tenant
payload	jsonb	Raw inbound payload
trace_id	uuid	Glass-box tracing ID (auto-generated, reused on retries)
Constraints & Indexes

PRIMARY KEY (inbound_event_id)

UNIQUE (tenant_id, dedupe_key)

Index on received_at

Index on trace_id

Table: bot.job_queue

Durable job queue for background processing.

Used for:

Inbound message processing

Retry/backoff logic

Worker concurrency

Columns
Column	Type	Notes
job_id	uuid	Primary key
tenant_id	uuid	FK → core.tenants
job_type	text	e.g. process_inbound_message
inbound_event_id	uuid	FK → bot.inbound_events
status	text	queued, processing, done, failed
attempts	integer	Retry counter
run_after	timestamptz	Backoff scheduling
locked_at	timestamptz	Worker lock
locked_by	text	Worker identifier
last_error	text	Failure reason
created_at	timestamptz
updated_at	timestamptz
trace_id	uuid	Copied from inbound_event (glass-box tracing)
Constraints & Indexes

PRIMARY KEY (job_id)

UNIQUE (job_type, inbound_event_id)

Composite index for job picking: (status, run_after, created_at)

Index on tenant_id

Index on trace_id

Table: bot.contacts

Represents an external person (lead/customer).

Uniqueness is enforced per tenant + channel + address.

Columns
Column	Type	Notes
contact_id	uuid	Primary key
tenant_id	uuid	FK → core.tenants
channel	text	sms, etc
channel_address	text	Phone / identifier
display_name	text	Optional
metadata	jsonb	Arbitrary contact info
created_at	timestamptz	
updated_at	timestamptz	
Constraints & Indexes

PRIMARY KEY (contact_id)

UNIQUE (tenant_id, channel, channel_address)

Index on tenant_id

Table: bot.conversations

Represents an active or historical conversation with a contact.

Exactly one open conversation per contact per tenant.

Columns
Column	Type	Notes
conversation_id	uuid	Primary key
tenant_id	uuid	FK → core.tenants
contact_id	uuid	FK → bot.contacts
status	text	open, closed
last_step	text	State machine marker
last_intent	text	Last detected intent
context	jsonb	Conversation state (offers, bookings, flags)
last_inbound_at	timestamptz	
last_outbound_at	timestamptz	
created_at	timestamptz	
updated_at	timestamptz	
Constraints & Indexes

PRIMARY KEY (conversation_id)

UNIQUE (tenant_id, contact_id, status)

Composite index on (tenant_id, contact_id, status)

Table: bot.messages

Stores all inbound and outbound messages.

This is the canonical conversation transcript.

Columns
Column	Type	Notes
message_id	uuid	Primary key
tenant_id	uuid	FK → core.tenants
conversation_id	uuid	FK → bot.conversations
contact_id	uuid	FK → bot.contacts
direction	text	inbound / outbound
provider	text	e.g. ghl
provider_msg_id	text	External ID
channel	text	sms, etc
text	text	Message body
payload	jsonb	Metadata, delivery status
created_at	timestamptz
trace_id	uuid	Glass-box tracing ID (propagated from inbound_event)
Constraints & Indexes

PRIMARY KEY (message_id)

Unique inbound idempotency index:
(tenant_id, provider, provider_msg_id, direction) where direction = 'inbound'

Index on (conversation_id, created_at)

Index on (provider, provider_msg_id)

Index on trace_id