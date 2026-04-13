# AI Holiday Hunter — Final Package Guide

## What this package is
A local, evidence-backed holiday deal engine that lets you describe the kind of trip you want, then searches and ranks options across multiple travel sources with deeper provider journeys, historical comparisons, shortlist reporting, map-led exploration, and manual takeover for sticky provider flows.

## What it is best at
- Turning a broad brief into a search plan
- Comparing multiple options quickly
- Surfacing strong exact fits and smart near-misses
- Preferring stronger basket-style pricing over weak teaser prices
- Saving evidence and audit outputs so you can trust the shortlist more

## What it does not do
- It does not complete bookings or submit payment details
- It does not guarantee the absolute cheapest live price every single run
- It still depends on how provider websites behave on the day

## Best way to run it
### Windows
Double-click `START_AI_Holiday_Hunter.bat`

### Desktop-style window on Windows
Double-click `START_AI_Holiday_Hunter_Desktop.bat`

### Mac / Linux
Run `./start_ai_holiday_hunter.sh`

The launcher bootstraps the virtual environment if needed, installs dependencies, installs Playwright Chromium, starts the local app, and opens it.

## Recommended first use
1. Save your search brief
2. Run **Demo scan** once
3. Run **Live scan**
4. Review:
   - `results/operator_briefing_pack.md`
   - `results/market_overview.md`
   - `results/truth_ranked_shortlist.md`
   - `results/basket_audit.md`

## Core folders
- `static/` — website-style frontend
- `results/` — outputs, reports, evidence indexes, shortlist files
- `logs/` — bootstrap and server logs
- `main.py` — FastAPI app entry point

## Honest final note
This package is built to improve your odds, widen coverage, and get closer to true payable pricing than ordinary manual searching. It should help you find better deals more consistently. It cannot honestly promise the cheapest possible deal every time.