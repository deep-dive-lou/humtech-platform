"""
Push Chris's 14-email outreach sequence to Instantly campaign via API v2.

This ONLY loads the email templates into the campaign. No emails will be sent
until the campaign is activated and leads are added.

Campaign: 76fc48b8-aa92-426e-8291-212fa3b4ee39 (HumTech-outreach-feb26)
"""

import json
import httpx

API_KEY = "ZDQ2ZDFlMGUtYWU1My00N2JhLWE2ODgtMDA5NmUxM2I1MjUxOmZvakl3QWZoUE1BbA=="
CAMPAIGN_ID = "76fc48b8-aa92-426e-8291-212fa3b4ee39"
BASE_URL = "https://api.instantly.ai/api/v2"

DIAGNOSTIC_URL = "https://humtech.ai/free-ai-diagnostic"

# --- Email steps (14 emails) ---
# delay = days to wait after previous step (0 = send immediately on add)

STEPS = [
    # Email 1 - Day 1
    {
        "type": "email",
        "delay": 0,
        "variants": [{
            "subject": "{{companyName}} AI Diagnosis tool for {{firstName}}",
            "body": (
                "Hi {{firstName}},\n\n"
                "{{personalization}}\n\n"
                "I wondered if {{companyName}} has an AI revenue growth strategy in place?\n\n"
                "If not, our 'Done for You' AI system increases revenue or don't pay "
                "- no retainer or implementation costs, and it takes a few weeks to integrate "
                "into existing software/infrastructure, with zero upheaval.\n\n"
                "You can use our FREE AI Diagnostic here to see if this might benefit you "
                f"specifically: {DIAGNOSTIC_URL}\n\n"
                "This will estimate the potential revenue increase (if you're not already at "
                "the cutting edge of the AI revolution).\n\n"
                "The output is helpful even if you don't use our service: it highlights revenue "
                "leakage across multiple domains, with AI recommendations tailored to your "
                "responses in a handy PDF which can be shared within your organisation.\n\n"
                "If you're looking to increase revenue with AI in 2026 and don't know where "
                "to start, let's jump on a call. No big sell, just to see if we can help "
                "solve problems and/or add value.\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 2 - Day 4 (delay 3)
    {
        "type": "email",
        "delay": 3,
        "variants": [{
            "subject": "The #1 ad agency on Trust Pilot. You can check. {{companyName}} revenue increase with AI, zero cost",
            "body": (
                "Hi {{firstName}},\n\n"
                "Our brand RESG has been the UK's #1 Advertising Agency on Trust Pilot for years "
                "(see for yourself) - you'll spot my name on Companies House too "
                "(saved you a minute). We use our custom-built neural network AI system, so it's "
                "impossible for businesses to compete with our CRO/CAC. We're the only ones doing "
                "it, that's why we're on top.\n\n"
                "If you'd like to see if we can roll this out for {{companyName}}, use our "
                f"FREE AI Diagnostic: {DIAGNOSTIC_URL}\n\n"
                "Even smaller SMEs discover \u00a3200k+ revenue leakage hiding in plain sight using our "
                "AI engine. It's possible to have good P&L/margins and still be leaving a fortune on "
                "the table.\n\n"
                "If you're eager not to lose out to the competition in 2026 as they inevitably roll out AI "
                "acquisition and conversion models like ours, complete our diagnostic and we'll see if "
                "we can help.\n\n"
                "We have 3 new client spaces left for Q1.\n\n"
                "Zero retainers or upfront costs, only pay if you win.\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 3 - Day 8 (delay 4)
    {
        "type": "email",
        "delay": 4,
        "variants": [{
            "subject": "{{companyName}} \u2013 Conversion increase, zero cost",
            "body": (
                "Hi {{firstName}},\n\n"
                f"Hope you had chance to run the FREE AI Diagnostic: {DIAGNOSTIC_URL}\n\n"
                "Most financial services firms like {{companyName}} discover \u00a3200k+ revenue leakage "
                "hiding in plain sight. RESG UK increased revenue by 400% from 2023-2024 \u2013 the "
                "competition couldn't keep up.\n\n"
                "Quick question: when clients call asking for financial advice, what % convert to "
                "becoming clients? Our clients hit 34% improvement within 60 days\u2026\n\n"
                "The gap? Advisors guessing scripts and performing repetitive tasks, instead of "
                "focusing on revenue generating tasks only. We've seen sales reps:\n\n"
                "- Requesting docs and facilitating compliance\n"
                "- Engaging leads which should have booked automatically\n"
                "- Getting ghosted and losing motivation\n"
                "- Missing KPI targets and blaming lead quality\n"
                "- Following outdated sales SOPs\n\n"
                "Our system fixes this automatically - no software swap. Just revenue. Does pinning "
                "down your exact conversion gap interest you?\n\n"
                "Zero retainers or upfront costs, only pay if you win.\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 4 - Day 12 (delay 4)
    {
        "type": "email",
        "delay": 4,
        "variants": [{
            "subject": "{{companyName}}: 98% sales appointment attendance / 56% rev increase",
            "body": (
                "Hi {{firstName}},\n\n"
                "Quick case study from high-volume outbound sales agency RESG UK who used our "
                "'pay on results only' AI revenue engine to increase revenue by 56% in 12 months.\n\n"
                "Before:\n"
                "- Only 10-15% of leads scheduled appointments through manual outreach\n"
                "- Around 50% appointment attendance on a good month\n"
                "- Sales team wasting time on engagement outreach plus manual admin\n"
                "- Zero martech for multivariate testing and statistical analysis\n\n"
                "Result:\n"
                "- 80%+ automated booking rate\n"
                "- 98% appointment attendance\n"
                "- 40% revenue increase\n"
                "- 10hrs saved/rep/week\n"
                "- 50% reduced customer acquisition cost\n\n"
                "\"HumTech's system found revenue we didn't know was missing\" - CFO\n\n"
                "AI spots patterns humans miss\u2026\n\n"
                "Takes 3 weeks to deploy over your CRM and current systems. We have 2 spots left "
                "for Q1 for our zero cost/retainer, pay on results only offer.\n\n"
                "It's best to complete the diagnostic here to make sure we're a good fit first: "
                f"{DIAGNOSTIC_URL}\n\n"
                "Then you can use our AI to schedule a call.\n\n"
                "Zero retainers or upfront costs, only pay if you win.\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 5 - Day 17 (delay 5)
    {
        "type": "email",
        "delay": 5,
        "variants": [{
            "subject": "{{firstName}} \u2013 We became #1 Trustpilot Advertising Agency, as an AI agency\u2026",
            "body": (
                "Hi {{firstName}},\n\n"
                "Most firms chase AI shiny objects. Winners deploy proven revenue engines.\n\n"
                "In 24 months, our AI Revenue Engine:\n"
                "- Powered #4 rated Green Energy Supplier\n"
                "- Built #1 rated Advertising Agency on Trustpilot (4.9/5.0 stars)\n"
                "- Utilised a team of just 4 people \u2013 no big payroll\n\n"
                "They didn't swap CRM. Didn't hire data scientists. Just layered our system over "
                "existing ops and let us do the heavy lifting.\n\n"
                "Result: 380% revenue increase Year 1. 660% increase Year 2. Paid from results only.\n\n"
                "Curious how #1 Trustpilot firms operationalize AI?\n\n"
                f"Takes 3 minutes: {DIAGNOSTIC_URL} (circulate the PDF output with your colleagues)\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 6 - Day 24 (delay 7)
    {
        "type": "email",
        "delay": 7,
        "variants": [{
            "subject": "{{firstName}} \u2013 how many hours do advisors waste on admin?",
            "body": (
                "Hi {{firstName}},\n\n"
                "What % of your team's day goes to manual data entry, follow-ups, and chasing leads?\n\n"
                "Industry average: 4.2 hours/day per rep. That's 1,100 hours/year per advisor lost to "
                "admin drudgery. Do the math \u2013 hourly rate x 1,100 x total staff = shock horror. This is "
                "almost always 6 figures annually for high volume SME's.\n\n"
                "Our automation + time deflection system handles this at no cost.\n\n"
                "Advisors focus 100% on closing.\n\n"
                "Result: 2-4 extra hours/day per rep = \u00a3187k avg. capacity increase Year 1. Zero "
                "software swap. Does automating the busywork (so they sell) sound worth 3 minutes?\n\n"
                "Use our free diagnostic calculator and share the results with your colleagues: "
                f"{DIAGNOSTIC_URL}\n\n"
                "Zero retainers or upfront costs, only pay if you win.\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 7 - Day 31 (delay 7)
    {
        "type": "email",
        "delay": 7,
        "variants": [{
            "subject": "{{firstName}} \u2013 \"if you don't measure it, you can't manage it\"",
            "body": (
                "Hi {{firstName}},\n\n"
                "\"If you don't measure it, you can't manage it.\" and \"you don't know what you "
                "don't know\" are our mantras.\n\n"
                "We've seen so many firms in financial services miss the silent killers which eat "
                "profits like termites eat wood \u2013 you probably don't even see them!\n\n"
                "Here's a small sample:\n\n"
                "Time-to-lead engagement: how fast you conversationally engage (>1hr = 67% fewer conversions)\n"
                "Lead Velocity Rate: Speed of leads moving from initial contact to qualified opportunity\n"
                "Pipeline Coverage Ratio: Ratio of pipeline value to sales quota\n"
                "Sales Cycle Length: Average days from lead to close\n\n"
                "There are many more, and missing just one vital stat leaves a big hole in your bucket.\n\n"
                "Our Clarity Engine tracks EVERY vital metric automatically, and in one place.\n\n"
                "What you fix: Advisor bottlenecks, process leaks, revenue gaps.\n\n"
                "Result: MASSIVE close rate lift from metrics you didn't know existed. Does seeing "
                "your hidden conversion gaps interest you? Find out if it might work for you here:\n\n"
                f"{DIAGNOSTIC_URL}\n\n"
                "Takes less than 3 minutes - Get immediate results - Completely free.\n\n"
                "Zero retainers or upfront costs, only pay if you win.\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 8 - Day 38 (delay 7)
    {
        "type": "email",
        "delay": 7,
        "variants": [{
            "subject": "{{firstName}} - eliminate document request risk, increase adherence",
            "body": (
                "Hi {{firstName}},\n\n"
                "Quick risk question for {{companyName}}: when clients email sensitive docs, what "
                "happens if they fat-finger the address?\n\n"
                "GDPR/ICO reality: Wrong recipient = \u00a320M fine risk (E.g. British Airways in 2020). "
                "Email chains also fail \"secure processing\" audits. Most \"docu-sign\" portals claim "
                "compliance but store on US servers \u2013 not UK data protection territory. Add generic "
                "branding into the mix and you have zero trust, and poor response rates (lots of chasing).\n\n"
                "Our Branded Document Request Portal fixes this:\n\n"
                "- UK servers only \u2013 ICO/GDPR bulletproof\n"
                "- Your logo/domain \u2013 client trust goes up, and so does response rate\n"
                "- Auto chases late documents so your team don't have to\n"
                "- One-click expiry \u2013 docs auto-delete post-processing\n"
                "- Audit trail \u2013 every upload tracked\n\n"
                "Result: Zero compliance risk + 47% faster client onboarding (no back-and-forth) and "
                "40% increase in adherence. Does client doc security + speed interest you?\n\n"
                "Reach out to book a quick call or use our FREE AI Diagnostic to see if there's a good "
                f"use case for {{{{companyName}}}}: {DIAGNOSTIC_URL}\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 9 - Day 44 (delay 6)
    {
        "type": "email",
        "delay": 6,
        "variants": [{
            "subject": "{{firstName}} - No longer \"the big that beats the small\" it's \"the fast that runs over the slow\"",
            "body": (
                "Hi {{firstName}},\n\n"
                "It's no longer \"the big that beats the small\" it's \"the fast that runs over the slow\"\n\n"
                "Engagement Latency: The time gap between customer interest and your business's "
                "meaningful response.\n\n"
                "Your competition is already closing this gap with AI. If you don't program "
                "instantaneous conversational AI by phone/SMS/Email, you're invisible\u2026\n\n"
                "That lead who reached out on a Sunday or 9pm at night? Your competitor's system "
                "picked them up and closed the sale before you're even back in the office.\n\n"
                "Speed is now the ultimate currency and 'lead response latency' is the difference "
                "between closing a deal and losing it to a competitor who instantly responds.\n\n"
                "Most companies believe buying updated software fixes this, but they lack the "
                "adaptive ecosystem to make it work.\n\n"
                "You have the tools... but not the strategy.\n\n"
                "You Need a Fractional AI Growth Team That Measures and Optimizes:\n"
                "1. Engagement Latency\n"
                "2. Handling Time vs. Deflection Savings\n"
                "3. Opportunity Cost\n\n"
                "Sounds like it should be expensive, but our system works so well that you only pay if "
                "we increase your revenue.\n\n"
                "Run your engagement figures through our FREE AI Diagnostic to find out how much "
                f"money you're losing to latency: {DIAGNOSTIC_URL}\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 10 - Day 51 (delay 7)
    {
        "type": "email",
        "delay": 7,
        "variants": [{
            "subject": "{{firstName}} \u2013 are vague leads killing your close rates?",
            "body": (
                "Hi {{firstName}},\n\n"
                "Quick thought - do your Google/FB ads attract browsers and ghosts instead of "
                "qualified buyers ready to commit? This is usually down to the absence of high-level "
                "multivariate testing + weak avatar targeting.\n\n"
                "73% of leads drop because creatives/copy/AI don't pre-qualify \"serious vs curious\".\n\n"
                "Our system fixes this automatically:\n\n"
                "- Martech A/B testing across landing pages, forms, CTAs\n"
                "- Conversion modules \u2192 pixel data to ad platforms (right avatar = 3x ROAS)\n"
                "- AI copywriting + creative optimization (tested vs your current set up)\n"
                "- Integrates over existing CRM, no upheaval\n\n"
                "Result: Min 30% lead-to-sale conversion (vs industry 9%). Does dialling in your "
                "ad\u2192close funnel interest you?\n\n"
                "Zero retainers or upfront costs, only pay if you win.\n\n"
                f"{DIAGNOSTIC_URL}\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 11 - Day 65 (delay 14)
    {
        "type": "email",
        "delay": 14,
        "variants": [{
            "subject": "{{firstName}} \u2013 your custom AI Revenue Engine, Done for You?",
            "body": (
                "Hi {{firstName}},\n\n"
                "Most AI vendors sell \"boxes\". We build your custom AI Revenue Engine \u2013 "
                "diagnosed, deployed, optimised by senior specialists. Our 5-step process (used by "
                "#1 Trustpilot firms):\n\n"
                "1. Baseline Assessment \u2013 gap audit\n"
                "2. Strategic Direction \u2013 prioritise EBITDA opportunities\n"
                "3. Layered Implementation \u2013 zero software swap, 3 weeks live\n"
                "4. Go-Live Optimisation \u2013 conversion maximisation guaranteed\n"
                "5. On-Demand Team \u2013 we run it, you profit\n\n"
                "\"Companies that will succeed will be the ones that most effectively use AI. If you're "
                "making maximum use of AI and competing against someone who isn't, you will win.\" "
                "\u2013 Elon Musk\n\n"
                "Your multidisciplinary AI team, paid from results only. Does a custom revenue engine "
                "interest you?\n\n"
                f"Get your FREE AI Diagnostic PDF: {DIAGNOSTIC_URL}\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 12 - Day 67 (delay 2)
    {
        "type": "email",
        "delay": 2,
        "variants": [{
            "subject": "{{firstName}} \u2013 4 typical AI wins we layer onto your stack, zero cost",
            "body": (
                "Hi {{firstName}},\n\n"
                "No \"out-the-box\" AI packages. We add these proven modules to your existing set up:\n\n"
                "- Agents/Conversational AI \u2192 faster qualification\n"
                "- 4+ hours/rep freed: Automation \u2192 no more manual drudgery\n"
                "- 28% close rate: Operational Clarity \u2192 track time-to-engagement\n"
                "- 3x ROAS: Done-for-you martech \u2192 right avatar targeting\n\n"
                "Zero disruption. We diagnose \u2192 deploy \u2192 optimise. You carry on doing what you do "
                "best. Which module interests you most? Takes 3 minutes to map to {{companyName}}: "
                f"{DIAGNOSTIC_URL}\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 13 - Day 75 (delay 8)
    {
        "type": "email",
        "delay": 8,
        "variants": [{
            "subject": "Where do we go from here?",
            "body": (
                "Hi {{firstName}},\n\n"
                "I reached out a few times re zero cost AI systems for {{companyName}} but didn't "
                "hear back.\n\n"
                "Where do we go from here?\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
    # Email 14 - Day 82 (delay 7)
    {
        "type": "email",
        "delay": 7,
        "variants": [{
            "subject": "AI and automation revenue systems for {{companyName}}?",
            "body": (
                "Hi {{firstName}},\n\n"
                "Are you interested in AI and automation revenue systems for {{companyName}}?\n\n"
                "Best wishes,\n"
                "{{signature}}"
            )
        }]
    },
]


def main():
    payload = {
        "parent_campaign": CAMPAIGN_ID,
        "name": "Main Sequence",
        "conditions": {
            "crm_status": [],
            "lead_activity": [],
            "reply_contains": "",
        },
        "subsequence_schedule": {
            "schedules": [
                {
                    "name": "Default",
                    "timing": {"from": "08:00", "to": "17:00"},
                    "days": {"1": True, "2": True, "3": True, "4": True, "5": True},
                    "timezone": "Europe/Isle_of_Man",
                }
            ],
        },
        "sequences": [
            {"steps": STEPS}
        ],
    }

    # Also dump to file for reference
    with open("scripts/instantly_payload.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Payload written ({len(STEPS)} email steps)")

    resp = httpx.post(
        f"{BASE_URL}/subsequences",
        json=payload,
        headers={"Authorization": f"Bearer {API_KEY}"},
        timeout=30,
    )
    if resp.status_code >= 400:
        print(f"ERROR {resp.status_code}: {resp.text}")
        raise SystemExit(1)

    result = resp.json()
    print("SUCCESS - Subsequence created")
    print(json.dumps(result, indent=2)[:2000])


if __name__ == "__main__":
    main()