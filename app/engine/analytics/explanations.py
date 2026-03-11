"""
Analytics explanations — plain-English definitions for every metric and concept
shown in the analytics command centre.

Each entry is a dict with four keys:
  what    — one sentence: what is this thing?
  how     — how it's calculated (words, not symbols)
  why     — what action you'd take based on this
  example — concrete numbers from the RESG test dataset

Used in templates via the `explanations` context variable.
"""

EXPLANATIONS: dict[str, dict[str, str]] = {

    # ── Dashboard KPIs ────────────────────────────────────────────────────

    "pipeline_win_rate": {
        "what": "The percentage of all leads that ever entered your pipeline who became paying clients.",
        "how": "Divide total won deals by total leads created (all time, or within the snapshot period). "
               "For example: 595 wins ÷ 13,471 leads = 4.4%.",
        "why": "This is your single most important top-line number. A rising win rate means your entire "
               "revenue process — from lead qualification through to close — is getting better. "
               "If it drops, something has broken upstream.",
        "example": "In the RESG test dataset, the pipeline win rate is 4.4%. That means for every 100 "
                   "leads that come in, about 4 become clients. Doubling this to 8.8% would double revenue "
                   "without spending a penny more on lead generation.",
    },

    "competitive_win_rate": {
        "what": "The percentage of closed deals (won or lost) that you won — your head-to-head conversion rate.",
        "how": "Divide total won deals by the sum of won and lost deals. This excludes leads still open "
               "in the pipeline. Example: 595 wins ÷ (595 wins + 7,074 losses) = 7.7%.",
        "why": "This isolates how well you perform at the final decision point — when a lead has made up "
               "their mind. A high pipeline win rate but low competitive win rate means you're losing at "
               "the close. A low pipeline win rate but high competitive win rate means you're losing leads "
               "before they even reach close.",
        "example": "If RESG won 595 of 7,669 closed deals, the competitive win rate is 7.7%. "
                   "That's the rate that matters when benchmarking against competitors.",
    },

    "pipeline_velocity_gbp": {
        "what": "How many pounds of revenue your pipeline is generating per day, right now.",
        "how": "Multiply the number of open deals by average deal value and win rate, then divide by "
               "the median sales cycle length in days. "
               "Formula: (Open deals × Average deal value × Win rate) ÷ Median cycle days.",
        "why": "Velocity tells you whether revenue is accelerating or stalling. If it drops, "
               "you need to diagnose whether the issue is fewer open deals, a falling win rate, "
               "or deals taking longer to close — all of which have different fixes.",
        "example": "If you have 200 open deals, an average deal value of £6,000, a 4.4% win rate, "
                   "and a 45-day median cycle, velocity = (200 × £6,000 × 0.044) ÷ 45 = £1,173/day.",
    },

    "bottleneck_stage": {
        "what": "The single pipeline stage where deals move slowest and cause the most backlog.",
        "how": "Identifies the stage with the lowest throughput (fewest deals exiting per week) "
               "using Little's Law — see Bottleneck Analysis for full detail.",
        "why": "By the Theory of Constraints, improving any stage other than the bottleneck has "
               "zero impact on overall pipeline flow. Fix the bottleneck first, always.",
        "example": "If 'Processing' has a throughput of 2 deals/week while all other stages process "
                   "10+ deals/week, every deal queuing before Processing is stuck waiting. "
                   "Fixing that one stage unlocks the whole pipeline.",
    },

    "active_anomalies": {
        "what": "The number of rate metrics that have triggered a statistical alarm in the latest period.",
        "how": "Each rate metric is tested against Western Electric rules — specific patterns of points "
               "outside the normal control limits that signal a non-random shift. Each violation counts "
               "as one anomaly.",
        "why": "Anomalies are early warnings. A single violation might be noise; persistent violations "
               "across multiple metrics mean something has structurally changed in your pipeline — "
               "good or bad. Zero anomalies means everything is within expected statistical variation.",
        "example": "If lead-to-qualified rate drops sharply in one week, Rule 1 fires (1 point beyond "
                   "3-sigma). If it stays below average for 8 weeks in a row, Rule 5 fires. "
                   "Both are worth investigating.",
    },

    "confidence_interval": {
        "what": "A range that contains the true rate with 95% probability, accounting for sample size.",
        "how": "For proportions with 30+ samples, we use the Wilson score interval — a method that "
               "stays accurate even near 0% or 100%. For small samples (under 30), we use "
               "Beta-Binomial conjugate inference, which is more conservative.",
        "why": "A point estimate (e.g. '4.4% win rate') is never exact — it's an estimate from a "
               "finite sample. The CI tells you how much uncertainty surrounds it. "
               "A wide CI means you need more data before making decisions. "
               "A narrow CI means you can trust the number.",
        "example": "With 595 wins from 13,471 leads, the 95% Wilson CI is approximately [4.1%, 4.8%]. "
                   "This means you can be 95% confident the true win rate is between 4.1% and 4.8%.",
    },

    "sparkline": {
        "what": "A miniature trend chart showing how a metric has moved over the last 12 periods.",
        "how": "Each data point is the metric value from one monthly snapshot. "
               "The line is drawn without axes — only the shape and direction matter.",
        "why": "Sparklines let you spot directional trends at a glance without needing to read "
               "a full chart. A rising line is good; a falling line needs investigation; "
               "a flat line may indicate stability or stagnation.",
        "example": "If the pipeline win rate sparkline trends up over the last 6 months, "
                   "something HumTech is doing is working. If it dips sharply in the last 2 months, "
                   "that warrants a drill-down on control charts.",
    },

    "narratives": {
        "what": "Auto-generated plain-English summaries of the most important metric movements.",
        "how": "The system reads the latest snapshot, computes CIs and compares trends, "
               "then constructs a sentence for each key metric — win rate, velocity, bottleneck, "
               "and anomaly count.",
        "why": "Raw numbers need interpretation. Narratives translate statistics into business language, "
               "so you can share the gist with the Chrises without them needing to read charts.",
        "example": "If win rate is 4.4% with CI [4.1%, 4.8%] from 13,471 leads, the narrative reads: "
                   "'Pipeline win rate is 4.4% (95% CI: 4.1–4.8%). Based on 595 wins from 13,471 leads.'",
    },

    # ── Control Charts ────────────────────────────────────────────────────

    "p_chart": {
        "what": "A statistical process control chart for rate (proportion) metrics — it shows whether "
                "variations are random noise or a genuine signal.",
        "how": "Calculate the average rate (center line) across all periods, then compute control limits "
               "at ±3 standard deviations above and below. Standard deviation uses the binomial formula: "
               "sqrt(p × (1-p) / n), where n is the sample size for that period.",
        "why": "Without control limits, every dip looks alarming and every rise looks promising. "
               "P-charts let you distinguish noise (random variation within limits) from signal "
               "(a genuine shift that warrants action). Only respond to signals, not noise.",
        "example": "If your close rate averages 8% over 12 months, control limits might fall at "
                   "3% and 13%. A month at 6% is noise. A month at 1% is a signal. "
                   "RESG's 4.4% win rate with its sigma bands would look similar.",
    },

    "control_limits": {
        "what": "The upper and lower boundaries on a control chart that define 'normal' statistical variation.",
        "how": "Set at 3 standard deviations above (UCL) and below (LCL) the center line. "
               "Also shown: 1-sigma and 2-sigma bands, which shade progressively darker toward the center. "
               "The sigma is calculated using the binomial formula for proportions.",
        "why": "Limits based on 3 standard deviations catch 99.7% of random variation. "
               "Anything outside is almost certainly a real signal, not chance. "
               "The sigma bands help identify subtler Western Electric rule violations.",
        "example": "With an average win rate of 4.4% and 1,000 leads per period, "
                   "UCL ≈ 6.9%, LCL ≈ 1.9%. "
                   "A month where win rate hits 7.5% is outside the UCL — investigate.",
    },

    "cusum": {
        "what": "Cumulative Sum — a chart that detects slow, sustained drifts in a metric that individual "
                "points might not reveal.",
        "how": "At each time period, add the difference between the observed value and the target (center). "
               "CUSUM+ accumulates positive deviations; CUSUM- accumulates negative ones. "
               "A signal fires when either exceeds a threshold (typically 5 standard deviations cumulative).",
        "why": "P-charts are good at detecting sudden jumps (one point way outside limits). "
               "CUSUM is good at detecting gradual drift — e.g. win rate slowly falling 0.1% per month "
               "for 6 months. Without CUSUM you'd miss it until it's too late.",
        "example": "If close rate falls from 8% to 7.8% to 7.5% to 7.2% over four months, "
                   "each individual month stays within control limits. "
                   "But CUSUM accumulates the downward deviation and fires a signal by month 4.",
    },

    "western_electric_rules": {
        "what": "A set of 8 specific patterns in control chart data that signal a non-random process shift.",
        "how": "Rule 1: 1 point beyond 3 sigma. "
               "Rule 2: 2 of 3 consecutive points beyond 2 sigma (same side). "
               "Rule 3: 4 of 5 consecutive points beyond 1 sigma (same side). "
               "Rule 4: 8 consecutive points on the same side of the center line. "
               "Rules 5-8 cover other patterns like trends and alternating values.",
        "why": "A single point outside 3 sigma (Rule 1) is obvious. But patterns within the bands "
               "(Rules 2-8) are equally diagnostic — they indicate a process that has shifted "
               "but hasn't yet produced an extreme value. Catching these early prevents problems "
               "from compounding.",
        "example": "If your lead-to-qualified rate is slightly above average 8 weeks in a row (Rule 4), "
                   "something has genuinely improved — perhaps HumTech's bot is qualifying better. "
                   "Conversely, 8 weeks below average is a warning sign worth acting on.",
    },

    "anomaly_violation": {
        "what": "A flagged data point where one or more Western Electric rules have been violated.",
        "how": "Each violation records: which rule fired, which time period, and a description. "
               "Orange diamond markers on the chart show exactly where violations occurred.",
        "why": "Violations are not emergencies — they're prompts to investigate. "
               "Look at what changed in that period: new lead source, sales team changes, "
               "seasonal effects, or a genuine HumTech impact. "
               "Cluster violations = systemic shift; isolated violation = likely noise.",
        "example": "A Rule 1 violation on 'Qualified to Booked' in March 2026 with a value of "
                   "2.1% vs UCL of 1.8% means the booked rate that month was unusually high. "
                   "Was it an unusually good month, or the start of a genuine trend? "
                   "Look at the CUSUM to find out.",
    },

    # ── Survival Analysis ─────────────────────────────────────────────────

    "kaplan_meier": {
        "what": "A statistical curve showing the probability that a deal is still 'alive' (not exited) "
                "in a given pipeline stage after a certain number of days.",
        "how": "At each day where at least one deal exited the stage, calculate the proportion that "
               "survived (didn't exit). Multiply all these proportions together progressively. "
               "The 'step' shape of the curve comes from this product updating only at event days.",
        "why": "KM curves reveal stage-level deal aging patterns. A steep early drop means deals "
               "move through quickly (healthy). A long flat tail means deals are stalling. "
               "Comparing curves across stages shows where your pipeline loses momentum.",
        "example": "In 'Processing', if 50% of deals exit within 3 days, the KM curve drops to 0.5 "
                   "at day 3. If another 25% exit by day 10 but some deals stay for 60+ days, "
                   "you'd see a long flat tail — those are deals worth chasing.",
    },

    "censoring": {
        "what": "A deal that is still open (hasn't exited the stage yet) when you run the analysis.",
        "how": "Kaplan-Meier handles censored deals correctly by including them in the 'at risk' count "
               "up to their last known date, then removing them from the denominator. "
               "This is statistically correct — you know they survived up to that point, "
               "just not what happened after.",
        "why": "Without censoring, you'd either have to exclude open deals (biasing toward faster "
               "exits) or wait until every deal resolves (impossible). KM is designed specifically "
               "to handle this — it's one reason survival analysis is the right tool here.",
        "example": "If a deal entered 'Qualified' 10 days ago and is still there today, "
                   "it contributes 10 days of 'survival' to the curve. "
                   "It doesn't count as a loss or a win — it's censored at day 10.",
    },

    "median_survival": {
        "what": "The number of days at which 50% of deals have exited a given pipeline stage.",
        "how": "Find the point on the Kaplan-Meier curve where survival probability crosses 0.5 (50%). "
               "This is the median, not the mean — median is more robust because a few very slow "
               "deals (long tail) don't distort it.",
        "why": "Median survival time tells you the 'typical' dwell time for a stage. "
               "Compare it across stages to find where deals slow down. "
               "Compare it before and after HumTech to prove time-to-close has improved.",
        "example": "If median survival in 'Proposal Sent' is 14 days pre-HumTech and 8 days post, "
                   "HumTech reduced the average wait for a decision by nearly a week. "
                   "At £6,000 avg deal value and 50 proposals/month, that's real cash flow improvement.",
    },

    "dead_deals": {
        "what": "Open leads that have been stuck in their current stage more than 3 times the median "
                "dwell time — statistically unlikely to close without intervention.",
        "how": "For each open lead, calculate days since it entered its current stage. "
               "If that exceeds 3 times the median for that stage (from KM analysis), "
               "it's flagged as 'statistically dead'. The 3x threshold means only the top ~5% "
               "slowest-moving deals get flagged.",
        "why": "These are deals that need active intervention now — either a phone call, "
               "a different approach, or explicit disqualification so they stop polluting "
               "your pipeline metrics. Carrying dead deals inflates WIP and distorts velocity.",
        "example": "If median dwell in 'Processing' is 5 days, any deal there for 15+ days is flagged. "
                   "If that deal is worth £6,000, it's worth a 5-minute call to find out if it's "
                   "still alive before you write it off.",
    },

    "hazard": {
        "what": "The instantaneous rate at which deals exit a stage at any given point in time — "
                "the 'risk' of the deal moving on (or dying) right now.",
        "how": "Conceptually: at any moment, given that a deal has survived this long in the stage, "
               "what is the probability it exits in the next short interval? "
               "High hazard early = fast-moving deals. Rising hazard later = deals that survived "
               "initial processing are now at risk of being chased or lost.",
        "why": "Hazard patterns reveal the mechanics of your pipeline. "
               "A decreasing hazard (deals that survive early are likely to keep surviving) "
               "suggests a selection effect — the 'survivors' are your strongest leads. "
               "An increasing hazard means deals that linger eventually get escalated or dropped.",
        "example": "In 'Appointment Booked', hazard might be highest on days 1-3 (most people show up "
                   "or cancel quickly), then drop to near zero for deals still open after a week "
                   "(those that haven't cancelled yet are very likely to show).",
    },

    # ── Bottleneck Analysis ───────────────────────────────────────────────

    "littles_law": {
        "what": "A mathematical law that links three pipeline metrics: the number of deals in a stage, "
                "how fast they arrive, and how long they stay.",
        "how": "L = lambda times W. L is the average number of deals in the stage (WIP). "
               "Lambda is the average arrival rate (deals entering per week). "
               "W is the average time a deal spends in the stage (dwell time in weeks). "
               "If you know any two, you can calculate the third.",
        "why": "Little's Law applies to any pipeline where arrivals and departures are in balance. "
               "It lets you predict: 'if I speed up this stage (reduce W), how many fewer deals "
               "will pile up (reduce L) for the same arrival rate?' No simulation needed.",
        "example": "If 10 deals/week arrive at 'Processing' and each deal stays for 5 days (0.71 weeks), "
                   "Little's Law predicts 7.1 deals in WIP at any time. "
                   "If you halve dwell time to 2.5 days, WIP drops to 3.5 — "
                   "pipeline looks cleaner and velocity increases.",
    },

    "wip": {
        "what": "Work In Progress — the number of deals currently sitting in a pipeline stage.",
        "how": "Count of leads whose current stage matches this stage right now. "
               "A high WIP means many deals are queued or actively being worked.",
        "why": "High WIP is not always bad — it depends on throughput. "
               "High WIP + high throughput = healthy busy stage. "
               "High WIP + low throughput = backlog and bottleneck. "
               "WIP is only meaningful alongside throughput and dwell time.",
        "example": "If 'Lead Qualified' has WIP=150 and throughput of 5/week, "
                   "those 150 deals will take 30 weeks to process — that's a serious bottleneck. "
                   "If throughput is 50/week, it'll clear in 3 weeks — totally fine.",
    },

    "throughput": {
        "what": "The number of deals exiting a pipeline stage per week.",
        "how": "Count all leads that left this stage (moved to the next stage or exited the pipeline) "
               "within the lookback window, then divide by the number of weeks in that window.",
        "why": "Throughput is the most actionable number in the bottleneck analysis. "
               "The stage with the lowest throughput is the bottleneck — it constrains everything "
               "upstream and downstream. Improving any other stage first is wasted effort.",
        "example": "If 'Appointment Booked' processes 3 deals/week but 'Processing' sends it "
                   "15 new deals/week, the backlog before 'Appointment Booked' grows at 12/week. "
                   "After 8 weeks, 96 deals are queued. The fix is to increase booking throughput.",
    },

    "utilisation_rho": {
        "what": "The ratio of how fast deals arrive to how fast they leave — a measure of how loaded "
                "a stage is.",
        "how": "Divide arrival rate by departure rate (throughput). "
               "Rho = 1.0 means arrivals exactly match departures. "
               "Rho > 1.0 means deals arrive faster than they leave — the stage will fill up "
               "indefinitely. Rho < 1.0 means the stage has spare capacity.",
        "why": "Queue theory proves that any stage with rho >= 1 is unstable — the queue grows "
               "without bound over time. Even rho = 0.9 leads to significant queuing. "
               "You want rho well below 0.8 for a stage to run smoothly.",
        "example": "If 'Processing' receives 12 deals/week and processes 10/week, rho = 1.2. "
                   "This is unsustainable — the backlog grows by 2 deals/week forever. "
                   "To fix it, either reduce arrivals (tighter qualification) or increase capacity.",
    },

    "cycle_time": {
        "what": "The total time a deal spends moving through the entire pipeline from creation to close.",
        "how": "Measured as the elapsed time between a lead's 'lead_created' event and its terminal "
               "event (won, lost, or other final stage). Median is used rather than mean "
               "because a small number of very slow deals would otherwise distort the average.",
        "why": "Cycle time = your sales cycle length. Shorter cycle time means faster revenue, "
               "better cash flow, and less risk of leads going cold. "
               "A key HumTech goal is to reduce median cycle time, which shows up directly here.",
        "example": "If median cycle time is 45 days pre-HumTech and 30 days post-HumTech, "
                   "the pipeline is clearing 33% faster. "
                   "At £6,000/deal and 50 deals/month, that's £300,000/month clearing 15 days sooner.",
    },

    "constraint": {
        "what": "The single pipeline stage that limits overall revenue throughput — fixing it has "
                "more impact than improving any other stage.",
        "how": "Identified as the stage with the lowest throughput across the pipeline. "
               "This comes directly from Goldratt's Theory of Constraints (1984).",
        "why": "It seems counterintuitive, but optimising non-bottleneck stages is waste. "
               "If you hire more salespeople to work on proposals but the bottleneck is in "
               "lead qualification, they'll just sit idle. Fix the constraint first, then "
               "the next constraint becomes visible.",
        "example": "In many B2B pipelines, the bottleneck is 'Proposal Sent' — deals sit there "
                   "waiting for a decision. The fix isn't more salespeople, it's shorter "
                   "proposal-to-decision cycle: tighter proposals, clearer calls to action, "
                   "follow-up cadence automation.",
    },

    # ── Causal Attribution ────────────────────────────────────────────────

    "its": {
        "what": "Interrupted Time Series — a method that uses the pre-intervention trend to predict "
                "what would have happened without HumTech, then measures the gap.",
        "how": "Fit a linear regression to the pre-intervention data to model the trend. "
               "Extend that trend line into the post-period as the 'counterfactual'. "
               "Compare actual post-period values to the counterfactual. "
               "The vertical jump at the intervention point is the 'level change'. "
               "The difference in slope before vs after is the 'slope change'.",
        "why": "ITS is the gold standard for evaluating policy changes when you can't run "
               "a randomised trial. It controls for pre-existing trends — so if win rate was "
               "already rising before HumTech, ITS won't credit that to HumTech. "
               "Only the change above-and-beyond the pre-trend counts.",
        "example": "If win rate was trending up at 0.1%/month before HumTech, and after HumTech "
                   "it trends up at 0.3%/month, the slope change is +0.2%/month. "
                   "Over 12 months, that's an additional 2.4% win rate attributable to HumTech.",
    },

    "counterfactual": {
        "what": "The predicted outcome if HumTech had never been implemented — what would have "
                "happened based purely on the pre-intervention trend.",
        "how": "The pre-intervention regression line is extended forward in time as if nothing changed. "
               "For BSTS/CausalImpact, the Bayesian model generates a distribution of plausible "
               "counterfactuals, not just a single line, which gives us a credible interval.",
        "why": "The counterfactual is the comparison point. Without it, you'd compare post-HumTech "
               "performance to the pre-HumTech average — which ignores any underlying trend. "
               "This could overstate HumTech's impact (if things were already improving) "
               "or understate it (if conditions deteriorated).",
        "example": "If the market was contracting and win rate would have fallen from 4.4% to 3.8% "
                   "without HumTech, but it actually stayed at 4.4%, HumTech prevented a 0.6% "
                   "decline — even though on the surface nothing changed.",
    },

    "level_change": {
        "what": "The immediate jump (or drop) in the metric at the exact moment of intervention.",
        "how": "The ITS model includes a dummy variable that switches from 0 (pre) to 1 (post) "
               "at the intervention date. Its coefficient is the level change. "
               "The 95% CI comes from the regression standard error (HC1 heteroskedasticity-corrected).",
        "why": "A significant level change means the metric shifted immediately on intervention — "
               "suggesting a direct, fast-acting effect. "
               "A non-significant level change with a significant slope change suggests a slower, "
               "cumulative effect — which is actually more credible for strategic interventions.",
        "example": "If HumTech's bot was switched on in January and win rate immediately jumped "
                   "from 4.4% to 5.2% in January, the level change is +0.8%. "
                   "If p < 0.05, that jump is statistically significant.",
    },

    "slope_change": {
        "what": "The change in the rate of improvement per time period, after intervention vs before.",
        "how": "The ITS model includes a time-since-intervention variable. Its coefficient is the "
               "slope change — how much faster (or slower) the metric is improving each period. "
               "A positive slope change means performance is accelerating post-intervention.",
        "why": "Slope change is often more meaningful than level change for strategic interventions. "
               "It means the intervention didn't just cause a one-time bump — it changed the "
               "trajectory. A compounding slope change has exponential impact over time.",
        "example": "A slope change of +0.2%/month means win rate is improving 0.2% faster each month "
                   "than it was pre-HumTech. After 12 months, that's +2.4% additional win rate. "
                   "At 13,471 leads/year and £6,000/deal, that's 323 more deals = ~£1.9M revenue.",
    },

    "bsts": {
        "what": "Bayesian Structural Time Series — the same method used by Google's CausalImpact package, "
                "which estimates the causal effect using a probabilistic model.",
        "how": "Fit a local linear trend model to the pre-intervention data using Bayesian inference "
               "(1,000 posterior samples). This gives a distribution over possible counterfactuals, "
               "not just a single line. Post-intervention, compare observed values to the "
               "distribution of counterfactuals to compute the probability that the effect is real.",
        "why": "BSTS is more robust than ITS because it quantifies uncertainty properly — "
               "the shaded credible interval band shows 'this is the range of outcomes that "
               "could plausibly have occurred without intervention'. "
               "If the observed line is outside that band, the effect is almost certainly real.",
        "example": "If the BSTS credible interval says the counterfactual win rate would have been "
                   "3.8% to 4.2%, and actual win rate is 4.8%, the observed value is outside the "
                   "95% credible interval. P(causal) = 96% means there's a 96% chance this "
                   "difference is due to HumTech, not random variation.",
    },

    "prob_causal": {
        "what": "The probability that the observed change in the metric was caused by HumTech's "
                "intervention, rather than being a coincidence.",
        "how": "Of the 1,000 posterior samples from the BSTS model, count what fraction show a "
               "cumulative effect in the right direction (e.g. positive uplift). "
               "That fraction is P(causal).",
        "why": "P(causal) is the most interpretable output of the Bayesian analysis — "
               "it's a direct probability statement. A 95% P(causal) means: if you repeated this "
               "experiment 100 times under identical conditions, 95 would show this pattern "
               "by chance. That's strong evidence of a real effect.",
        "example": "P(causal) = 87% means there's an 87% chance HumTech caused the observed "
                   "improvement. That maps to 'likely' in IPCC probability language. "
                   "P(causal) = 99% maps to 'virtually certain'.",
    },

    "cumulative_effect": {
        "what": "The total accumulated difference between observed and counterfactual across all "
                "post-intervention periods combined.",
        "how": "Sum the pointwise differences (observed minus counterfactual) for each post-period. "
               "The 95% CI is computed from the BSTS posterior samples.",
        "why": "The cumulative effect tells you the total impact over the measurement window, "
               "not just the most recent period. This is the number to quote to clients: "
               "'since HumTech started, your win rate has improved by X percentage points "
               "above trend, cumulatively.'",
        "example": "If HumTech has been running for 6 months and the average monthly pointwise "
                   "effect is +0.5% win rate, the cumulative effect is +3%. "
                   "At 1,000 leads/month and £6,000/deal, that's 18 additional deals = £108,000.",
    },

    "relative_effect": {
        "what": "The cumulative effect expressed as a percentage of what the counterfactual would "
                "have been — a normalised measure of impact.",
        "how": "Divide cumulative effect by the cumulative counterfactual total, then multiply by 100. "
               "This controls for baseline differences between tenants with different absolute rates.",
        "why": "Relative effect is useful for comparing HumTech's impact across different clients. "
               "A +1% absolute improvement on a 4% baseline is a 25% relative improvement, "
               "which is much more impressive than the same +1% on a 20% baseline (5% relative).",
        "example": "If pre-HumTech win rate was 4.4% and post-HumTech it's 5.5%, "
                   "the relative effect is +25%. That's a 25% improvement in win rate — "
                   "a compelling number for the client presentation.",
    },

    "doubly_robust": {
        "what": "An estimator that measures the causal effect at the individual lead level, "
                "controlling for differences between leads contacted quickly vs slowly.",
        "how": "Two models are combined: (1) a propensity model that predicts the probability of "
               "being treated (contacted within 24h) based on lead characteristics, and "
               "(2) an outcome model that predicts conversion probability. "
               "Combining both makes the estimate correct even if one model is imperfect — "
               "hence 'doubly robust'.",
        "why": "ITS and BSTS work on aggregate time series. DR works on individual leads, "
               "which is more granular. It answers: 'for a specific lead, how much does "
               "contacting within 24h improve their probability of converting?' "
               "This directly quantifies the value of HumTech's speed-to-lead automation.",
        "example": "ATE = 0.03 means leads contacted within 24h are 3 percentage points more likely "
                   "to convert than leads contacted later. At 1,000 leads/month and £6,000/deal, "
                   "that's 30 additional deals/month = £180,000/month from speed-to-lead alone.",
    },

    "propensity": {
        "what": "The probability that a given lead would have been contacted within 24 hours, "
                "based on observable characteristics (source, time of day, day of week, etc.).",
        "how": "Fit a logistic regression on the pre-intervention leads, predicting whether "
               "treatment (contact within 24h) occurred. "
               "The output probabilities are the propensity scores.",
        "why": "Propensity scores balance the comparison between treated and control groups. "
               "Without them, you might compare high-quality inbound leads (likely to be "
               "contacted fast) to low-quality outbound leads (likely contacted slowly) — "
               "which would confound the treatment effect with lead quality.",
        "example": "If premium website leads always get called within 1 hour (high propensity) "
                   "and they also convert at 8% (vs 3% for slow-contact leads), you need to "
                   "control for lead quality to isolate the effect of speed alone. "
                   "Propensity weighting does this.",
    },

    "propensity_auc": {
        "what": "A measure of how well the propensity model distinguishes treated from control leads.",
        "how": "AUC (Area Under the ROC Curve) ranges from 0.5 (random) to 1.0 (perfect). "
               "0.55-0.95 is the target range — below 0.55 means the model learned nothing "
               "(treatment was random); above 0.95 means there's near-perfect selection bias "
               "(only certain leads ever got treated), making comparison unreliable.",
        "why": "AUC is a model quality check. If it's outside the target range, the DR estimate "
               "should be interpreted cautiously. Good AUC (0.65-0.80) means the model found "
               "real signal in lead characteristics, and the propensity-weighted comparison is valid.",
        "example": "AUC = 0.71 means: given two leads — one treated, one not — the model correctly "
                   "identifies which is which 71% of the time. That's solid for a real-world dataset "
                   "where many factors influence speed-to-contact.",
    },

    "ate": {
        "what": "Average Treatment Effect — the estimated average improvement in conversion probability "
                "caused by contacting a lead within 24 hours.",
        "how": "Computed by the Doubly Robust estimator: for each lead, calculate the difference "
               "in expected outcome under treatment vs control (using the propensity-weighted outcome "
               "models). Average this difference across all leads.",
        "why": "ATE is the most actionable number from the DR analysis. It tells you exactly "
               "how much faster lead response increases win rate. "
               "This becomes the justification for HumTech's speed-to-lead automation investment.",
        "example": "ATE = 0.025 means a 2.5 percentage point lift in conversion from 24h response. "
                   "The 95% CI might be [0.01, 0.04]. "
                   "If the lower bound is above zero, the effect is statistically significant "
                   "and you can quote it with confidence.",
    },

    "uplift_consensus": {
        "what": "A synthesis of all three causal methods (ITS, CausalImpact, Doubly Robust) into "
                "a single verdict on whether HumTech caused a measurable improvement.",
        "how": "Each method that converged provides a significance verdict. The consensus statement "
               "uses IPCC probability language calibrated to the proportion of significant results: "
               "'Virtually certain' (3/3 significant), 'Likely' (2/3), 'Uncertain' (1/3), "
               "'No evidence' (0/3).",
        "why": "No single method is perfect. ITS can be fooled by seasonal trends. "
               "BSTS needs enough pre-period data. DR needs variation in treatment timing. "
               "Using three independent methods and finding they agree is much stronger evidence "
               "than any single method alone — this is scientific triangulation.",
        "example": "If ITS shows a significant level change, BSTS shows P(causal) = 93%, "
                   "and DR shows ATE = 0.03 with p < 0.05, consensus is 'virtually certain'. "
                   "That's the claim you put in front of the client: three independent analyses "
                   "agree HumTech caused the improvement.",
    },

    "ipcc_language": {
        "what": "A standardised vocabulary for communicating probability that avoids vague words "
                "like 'significant' or 'strong' — borrowed from climate science.",
        "how": "The IPCC (Intergovernmental Panel on Climate Change) defines: "
               "'Virtually certain' = >99% probability. 'Extremely likely' = >95%. "
               "'Very likely' = >90%. 'Likely' = >66%. 'About as likely as not' = 33-66%. "
               "'Unlikely' = <33%. 'Extremely unlikely' = <5%.",
        "why": "Business people are not statisticians. 'p=0.03' means nothing to most people. "
               "'Virtually certain that HumTech caused the improvement' means everything. "
               "IPCC language translates statistics into understandable probability claims "
               "that clients can evaluate for themselves.",
        "example": "If HumTech's consensus is 'likely' (2/3 methods significant), you say: "
                   "'It is likely that HumTech caused the observed improvement in win rate.' "
                   "That's a defensible, calibrated claim — not statistical hand-waving.",
    },

    # ── Cohort Analysis ───────────────────────────────────────────────────

    "cohort_heatmap": {
        "what": "A grid showing how the conversion rate of each monthly intake of leads evolves "
                "over time — rows are cohorts, columns are months since entry.",
        "how": "Group leads by the month they entered the pipeline (their 'cohort'). "
               "For each cohort, calculate the cumulative conversion rate at 0, 1, 2... months "
               "after entry. Colour cells from light (low conversion) to dark (high conversion). "
               "Recent cohorts have fewer elapsed months, so their later columns are blank.",
        "why": "The heatmap reveals maturation patterns — how long it takes different cohorts "
               "to reach their final conversion rate. If older cohorts have much higher final "
               "rates than newer ones, your pipeline may just need more time to mature. "
               "If newer cohorts are already converting faster at Month 1 than older cohorts were, "
               "something has genuinely improved.",
        "example": "If leads from Aug 2022 had a 4.4% conversion rate by month 6, "
                   "but leads from Jan 2026 already show 5.2% at month 1, "
                   "the newer cohort is ahead of pace — evidence of improvement.",
    },

    "cohort_triangular": {
        "what": "The blank region in the bottom-right of the heatmap — not missing data, "
                "but time that hasn't happened yet for recent cohorts.",
        "how": "A cohort that entered in February 2026 cannot have Month 3 data in March 2026 — "
               "those leads haven't had 3 months to convert yet. "
               "The triangle of blanks grows as cohorts become more recent.",
        "why": "Understanding the triangular structure prevents a common mistake: "
               "concluding that new cohorts are 'converting poorly' when in fact they just "
               "haven't had time to convert yet. Always compare cohorts at the same elapsed time.",
        "example": "Do not compare Oct 2022's final conversion rate (all months elapsed) "
                   "to Jan 2026's month-1 rate — that's like comparing a marathon runner's "
                   "finish time to their pace at the first mile. "
                   "Compare both cohorts at Month 1 only for a fair comparison.",
    },

    "cumulative_conversion": {
        "what": "The running total percentage of a cohort that has converted by a given elapsed month — "
                "each cell represents 'all wins up to and including this month'.",
        "how": "For each cohort and each elapsed month, count all leads from that cohort that "
               "won by month M, divided by cohort size. Because it's cumulative, each column "
               "is always >= the previous column.",
        "why": "Cumulative conversion is the right metric here because deals close at different "
               "speeds. Some leads convert in week 1; others take 6 months. "
               "Cumulative figures capture the full picture — a 'point in time' view would "
               "undercount slow-converting cohorts.",
        "example": "A cohort with 3% cumulative conversion at Month 1, 4% at Month 3, "
                   "and 4.4% at Month 6 tells you most wins came quickly, with a slow trickle "
                   "of late conversions. That's a typical B2B pattern.",
    },

    "simpsons_paradox": {
        "what": "A statistical illusion where a trend appears to go one direction in aggregate "
                "but goes the opposite direction within every sub-group.",
        "how": "Compare the aggregate win rate pre vs post intervention. Then compare win rates "
               "within each lead source separately. If the sub-group trends consistently disagree "
               "with the aggregate trend, Simpson's Paradox is present.",
        "why": "This can make a genuine improvement look like a decline, or vice versa. "
               "It typically happens when the mix of lead sources shifts (e.g. more low-quality "
               "sources post-intervention) and distorts the aggregate. "
               "Without this check, you could falsely conclude HumTech harmed the business "
               "when in fact it improved performance within every source.",
        "example": "The classic example: Berkeley admissions 1973. Overall, women were admitted "
                   "at lower rates than men. But within almost every department, women were "
                   "admitted at higher or equal rates. The paradox occurred because women applied "
                   "more to competitive departments. The aggregate was misleading.",
    },

    "pre_post_comparison": {
        "what": "A direct comparison of conversion rates in the period before HumTech started "
                "versus after, split by lead source to control for mix shifts.",
        "how": "Split all leads into pre-intervention and post-intervention groups. "
               "For each lead source with at least 10 leads in both periods, "
               "calculate pre and post win rates. Compare directions.",
        "why": "This is the simplest sanity check before reading the causal models. "
               "If every source shows improvement post-HumTech, the case is clear. "
               "If some sources got worse, the overall picture is mixed and the causal "
               "models need to account for that complexity.",
        "example": "If 'Google' leads improved from 5% to 7% and 'Facebook' leads improved "
                   "from 2% to 3%, but the aggregate went from 4% to 3.8% because Facebook "
                   "volume tripled, that's Simpson's Paradox. "
                   "Each source improved; the mix shift created a false decline.",
    },
}
