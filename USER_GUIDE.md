# FMM — Football Match Monitor

## What It Does

FMM automatically monitors football matches across selected competitions and checks whether key pre-game, live, and post-game data sources are properly set up. It runs every 30 minutes, generates an Excel report, and uploads it to a shared Google Drive folder. If new issues are found, a Slack notification is sent to `#notifications-fmm`.

---

## How It Works

1. **Every 30 minutes**, the script runs automatically via GitHub Actions.
2. It fetches match data from the Genius Sports API for all competitions listed in the whitelist.
3. For each match, it performs the following checks:
   - **Pre-game DM check** — is the match set up in the system?
   - **Pre-game CoreTools / WHST Live Data Source** — are the live data sources configured?
   - **Live game Statistician check** — is a statistician assigned?
   - **Live game Webcast check** — is the webcast available?
   - **End game Past match data** — is post-match data available?
4. Results are written to an Excel file and uploaded to Google Drive, replacing the previous version.
5. A Slack summary is sent showing how many matches were processed, how many are new, and any new issues.

---

## The Excel File

The Excel file has two tabs:

### Tab 1: Match Data
Each competition has its own section with columns:

| Column | Description |
|--------|-------------|
| League | Competition name |
| Date | Match date |
| Time Local / UTC / Tallinn / Medellin | Kick-off in different time zones |
| Game ID | Unique match identifier |
| Game | Home vs Away team names |
| Pre-game DM check | ✅ OK or ❌ issue description |
| Pre-game CoreTools / WHST | ✅ OK or ❌ issue description |
| Live game Statistician check | ✅ OK or ❌ issue description |
| Live game Webcast check | ✅ OK or ❌ issue description |
| End game Past match data | ✅ OK or ❌ issue description |

- **Green cells** = everything is fine
- **Red cells** = there is an issue that needs attention
- **Column A is editable** — if you add a note (e.g. "Match cancelled"), it will be preserved across runs

### Tab 2: Whitelist
This is the list of competitions being monitored. You can manage it directly from the Excel file:

- **To add a competition**: enter its Competition ID in column A on a new row. The name and league info will be filled automatically on the next run.
- **To remove a competition**: delete its row.

Changes you make in the Whitelist tab will be picked up on the next run.

---

## Slack Notifications

After every run, a summary is posted to `#notifications-fmm`:

```
FMM run completed at 2026-04-01 12:00:00 UTC
• Competitions processed: 15
• Total matches in Excel: 120
• New matches added: 3
• Open issues: 5
• New issues: 1

New issues:
  [Malaysian Football League] Match 12345 — Pre-game DM check
```

If there are no new issues, it will say "No new issues detected."
Issue notifications are **not duplicated** — the same issue is only reported once.

---

## Match Cleanup

Matches are automatically removed from the Excel file **2 weeks after kick-off**, so the report only shows recent and upcoming matches.

---

## Quick Reference

| Item | Detail |
|------|--------|
| Runs every | 30 minutes (automated) |
| Output | Excel file on Google Drive |
| Slack channel | `#notifications-fmm` |
| Whitelist | Editable from the Excel "Whitelist" tab |
| Match retention | 14 days after kick-off |
| Manual trigger | GitHub → Actions → Run workflow |
