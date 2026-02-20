# FMM Football Data Fetch – Files, Setup, and Execution

## Project Structure & Files

The project has been optimized for both local execution and automated scheduling on **Databricks**. It follows a modular structure to separate concerns:

| File / Folder | Purpose |
|------|--------|
| **databricks_job.py** | Main entrypoint for running the data fetch process. Runs validations and generates the Excel. |
| **football_data_fetch.py** | Legacy monolithic script (kept for reference, but use `databricks_job.py` instead). |
| **api/** | `genius_client.py`: API interactions, response caching, and rate-limit handling (via `tenacity`). |
| **config/** | `settings.py`: Configuration and secrets management (supports Databricks `dbutils.secrets` and `os.getenv()`). |
| **notifications/** | `slack.py`: Slack alerts and error reporting. |
| **processing/** | `match_evaluator.py`: Business logic for pre/post-match checks, DM checks, Webcast checks, WHST checks, etc. |
| **storage/** | `excel_writer.py`, `state.py`: Handles reading/writing the `.xlsx` files and `football_fetch_state.json` states. |
| **competition_whitelist.json** | List of competitions to process (IDs, names, league_id, etc.). |
| **football_competitions_fetch.xlsx** | Output Excel. Generated/updated each run. |
| **football_fetch_state.json** | State file tracking written and manually deleted match IDs. |

The script only *reads* the whitelist and (if present) the existing Excel and state file. It *writes* the Excel and state file each run.

---

## 💻 Running Locally

1. **Python Requirement**  
   Python 3.8+ (3.9+ recommended).

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
   *Required packages: `requests`, `openpyxl`, `slack_sdk`, `tenacity`*

3. **Configure Secrets via Environment Variables**  
   Because the script is optimized for headless execution (no manual `input()` prompts), you **must** configure your secrets via environment variables before running:
   
   - **API Key (Required):**
     - Windows PowerShell: `$env:GENIUS_API_KEY = "your-api-key"`
     - Mac/Linux: `export GENIUS_API_KEY="your-api-key"`
   
   - **Slack Token (Optional):**
     - Windows PowerShell: `$env:SLACK_BOT_TOKEN = "xoxb-your-token"`
     - Mac/Linux: `export SLACK_BOT_TOKEN="xoxb-your-token"`
     - *Alternatively, create a `.slack_bot_token` file in the script root with just the token.*

4. **Execution**  
   Run the new modular script:
   ```bash
   python databricks_job.py
   ```

---

## ☁️ Running on Databricks

This project is built to run smoothly as a Databricks Job.

1. **Databricks Secrets Management:**
   - The script automatically checks `dbutils.secrets.get()` if running inside Databricks.
   - You need to add your secrets to a Databricks secret scope named `fmm_scope`.
   - Keys required: `genius_api_key` and optionally `slack_bot_token`.
     ```bash
     databricks secrets create-scope --scope fmm_scope
     databricks secrets put --scope fmm_scope --key genius_api_key
     databricks secrets put --scope fmm_scope --key slack_bot_token
     ```

2. **Databricks File System (DBFS) Storage:**
   - Databricks cluster nodes are ephemeral. Local files are lost on termination.
   - To persist state and output files between scheduled runs, set the output path to DBFS using environment variables in your Databricks Job cluster settings:
     - `OUTPUT_EXCEL_PATH=/dbfs/mnt/fmm_data/football_competitions_fetch.xlsx`
     - `WHITELIST_PATH=/dbfs/mnt/fmm_data/competition_whitelist.json`
   - The script detects `/dbfs/` paths and seamlessly writes the JSON state file and Excel document to the persistent storage.

3. **Job Configuration:**
   - **Task Type:** Python Script (point to `databricks_job.py` within your cloned repo).
   - **Dependencies:** Ensure `requests`, `openpyxl`, `slack_sdk`, and `tenacity` are installed on the job cluster.

---

**Optional Setup:** You can copy an existing `football_competitions_fetch.xlsx` and `football_fetch_state.json` from your current PC into the working folder (or DBFS volume) if you want to resume with existing manual table edits and "deleted" matches tracking. Not required for a fresh start.
