# Process Inbound Message (v1)

Steps:
1) claim job
2) load inbound_event
3) upsert contact (tenant_id + channel + channel_address)
4) upsert open conversation (one open per contact)
5) insert inbound message (idempotent via provider_msg_id + tenant + provider + direction)
6) evaluate state machine and produce outbound message row (send_status=pending)
7) mark job done or retry
