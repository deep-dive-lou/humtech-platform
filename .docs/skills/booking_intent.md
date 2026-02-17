# Skill: booking_intent (v1)

Input: user text + current state (pending_booking? last_offer?)
Output: intent label

Rules:
- if pending_booking and user changes constraints → booking_change
- if pending_booking and user confirms → booking_confirmation
- if mentions day/time/availability → booking_request
- else greeting_or_unclear
