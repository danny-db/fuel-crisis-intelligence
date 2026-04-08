# Adelaide Meetup Demo: Fuel Crisis Intelligence

## Duration: 15-20 minutes
## Presenter: Danny Wong, Solutions Architect, Databricks

---

## 1. HOOK (2 min)

**[Open with map showing Strait of Hormuz on command centre]**

> "It's April 2026. Iran and the US are in a military standoff. Iran threatens to close the Strait of Hormuz — 20% of the world's oil passes through this narrow strait."
>
> "Australia imports 90% of its refined fuel. We have just 21 days of onshore reserves — well below the IEA's 90-day obligation."
>
> "The Prime Minister's office calls. They need answers NOW. How exposed are we? What happens to prices? Which states go dark first?"
>
> "Tonight I'll show you how we built a complete crisis intelligence platform — from raw data to PM briefing — on Databricks. In days, not months."

---

## 2. NOTEBOOK FLASH (1 min)

**[Switch to Databricks workspace — show 03_enrichment_ingestion.py]**

> "First, data. We ingested 30+ tables from free public sources — FRED for oil prices and exchange rates, EIA for OPEC production, ABS for CPI, Geoscience Australia for infrastructure, FuelWatch for station-level prices."

**[Show the verification cell output — all tables green]**

> "All running on serverless compute. No clusters to manage. The data lands in Unity Catalog — governed, lineage-tracked, ready for analytics."

**[Point at the schema list]**

> "That's 30+ tables covering global oil markets, Australian fuel prices, infrastructure, trade flows, and a 90-day crisis scenario projection. All from publicly available data."

---

## 3. STORY DASHBOARD (5 min)

**[Open the 6-page AI/BI Dashboard]**

### Page 1: "The Trigger"
> "Brent crude has been sitting around $80. Our model shows a spike to $143 within two weeks if Iran goes offline."

**[Point to OPEC bar chart]**
> "Iran produces 13.6% of OPEC output. That's 3.2 million barrels per day. The largest supply shock since the 1979 Iranian Revolution."

### Page 2: "Australia's Exposure"
> "Look at where our crude comes from. Over a third from the Middle East. And our IEA days of coverage? We're below the 90-day obligation."

### Page 3: "Impact on Consumers"
> "Adelaide ULP wholesale is already at X cents per litre. With the crisis, our scenario projects over 250 c/L at the pump."
> "The AUD is falling too — every cent we lose adds about $1.5 billion to our annual fuel import bill."

### Page 4: "State Vulnerability"
> "Which states are most exposed? The ones consuming the most diesel — and that's the transport-heavy states: Queensland, NSW, WA."

### Page 5: "Industry at Risk"
> "These are Australia's top 15 carbon emitters — they're also our biggest fuel consumers. And look at the power stations — how many run on gas or diesel?"

### Page 6: "Infrastructure"
> "This is the punchline. Australia has FOUR operational refineries. Four. Down from eight a decade ago. If imports stop, we're processing just 50% of demand domestically."
>
> "And here are the fuel terminals — the nodes in our supply chain. Lose one or two, and entire regions go dry."

---

## 4. GENIE SPACE (3 min)

**[Open Genie Space — "Australian Fuel Crisis Intelligence"]**

> "Now, imagine you're a policy analyst. You don't write SQL. You just need answers."

**[Type: "How exposed is Australia to Middle East oil?"]**

> "Genie understands natural language, generates SQL under the hood, and returns the answer. Let me show the SQL."

**[Click "Show SQL"]**

> "Clean, auditable SQL. The analyst gets the answer, the data engineer can verify it."

**[Try: "How many operational refineries does Australia have?"]**

> "Four. Lytton, Altona, Kwinana, Geelong. If you want more detail..."

**[Try: "What's the projected Brent price on day 14 of the crisis?"]**

> "Genie hits our scenario overlay table. $143 per barrel. AUD at $0.58. Adelaide ULP at 250+ cents per litre."

---

## 5. COMMAND CENTRE APP (5 min)

**[Open fuel-crisis-app]**

### Metrics Ticker
> "The top bar gives you real-time KPIs. Brent crude, AUD/USD, IEA reserves, operational refineries."

### Map Tab
> "This is MapLibre GL with vector tiles served from Lakebase — Databricks' new PostGIS-enabled database. The tiles are generated on-demand using ST_AsMVT."

**[Toggle layers on/off]**
- Refineries: "Green = operational. Red = closed. Only four green dots."
- Terminals: "Blue dots — the supply chain nodes. Click one." **[Show popup]**
- WA Fuel Stations: "Price gradient — green is cheap, red is expensive. Even before the crisis, some stations are hitting $2+."
- Minerals: "5,000 mineral deposits. Mining runs on diesel. No fuel, no mining, no export revenue."
- Strait of Hormuz: "The red dashed line. This is the chokepoint."

### Genie Tab
> "Same Genie space, embedded in the app. Let me ask..."
**[Click sample question chip]**

### AI Briefing Tab
**[Click "Generate PM Briefing"]**
> "This calls the Foundation Model API — Claude Sonnet on Databricks. It gathers real-time data from multiple tables and generates a classified briefing note."

**[Wait for response — show the formatted briefing]**
> "Situation summary, immediate impact, vulnerabilities, 14-day projection, and five specific recommendations. All generated from live data. This is what lands on the PM's desk."

---

## 6. CLOSE (1 min)

> "So what did we just see?"
>
> "30+ datasets from free public sources — ingested in minutes with serverless compute."
>
> "An AI/BI Dashboard that tells the story from global trigger to local impact."
>
> "A Genie space where anyone can ask questions in plain English."
>
> "A geospatial command centre with vector tiles from Lakebase, live Genie chat, and AI-generated briefings."
>
> "All on one platform. All governed by Unity Catalog. Built in days, not months."
>
> "That's the power of Databricks for crisis intelligence. Thank you."

---

## BACKUP PLAN

| Scenario | Fallback |
|----------|----------|
| Genie slow/down | Pre-screenshot answers, narrate |
| App not loading | Screen recording video backup |
| FMAPI timeout | Pre-generated briefing in markdown |
| Wi-Fi issues | All demos run on mobile hotspot too |
| Awkward silence | "Let me show you the SQL behind that..." |

## PRE-DEMO CHECKLIST

- [ ] Run `03_enrichment_ingestion.py` — verify all 12 tables have rows
- [ ] Run `05_create_story_dashboard.py` — note dashboard ID
- [ ] Run `06_create_enhanced_genie_space.py` — note space ID
- [ ] Run `07_setup_lakebase_tiles.py` — note Lakebase credentials
- [ ] Deploy `fuel-crisis-app/` — set GENIE_SPACE_ID in app.yaml
- [ ] Test all 4 tabs in the app
- [ ] Pre-warm Genie with 2-3 questions
- [ ] Take screenshots of every key screen (backup)
- [ ] Test on mobile hotspot (backup connectivity)
- [ ] Charge laptop to 100%
