# Outbound Sending (v1)

Sender loop:
- claim pending/failed messages ready for retry
- send via adapter
- update message payload send_status + timestamps + provider_msg_id
- apply backoff and eventually mark dead
