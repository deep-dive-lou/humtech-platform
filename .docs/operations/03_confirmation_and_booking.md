# Confirmation & Booking (v1)

When pending_booking exists:
- interpret confirmation naturally (not only YES/NO)
- if confirm: call book_slot adapter
- if decline: clear pending_booking and re-offer
- if change_request: clear pending_booking, update preference, re-offer closest
