# Skill: confirmation_interpretation (v1)

When pending_booking exists, interpret message as:
- confirm (e.g. "perfect", "book it", "yeah")
- decline (e.g. "no", "not that one", "different")
- change_request (e.g. "actually Thurs pm", "before lunch Tuesday")
- unclear

Priority: change_request overrides confirm/decline.
