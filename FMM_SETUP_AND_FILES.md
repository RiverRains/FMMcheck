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
     databricks secrets create-scope fmm_scope
     databricks secrets put-secret fmm_scope genius_api_key
     databricks secrets put-secret fmm_scope slack_bot_token
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

## Next steps after pushing the project (Git) to Databricks

Do these in order so the job runs with the right code, dependencies, and secrets.

### 1. Create a secret scope and store secrets

In your Databricks workspace (or via CLI):

```bash
# Create scope (if not exists). Scope name is positional (no --scope flag).
databricks secrets create-scope fmm_scope

# Store the API key and optional Slack token. Scope and key are positional; you’ll be prompted for the value.
databricks secrets put-secret fmm_scope genius_api_key
databricks secrets put-secret fmm_scope slack_bot_token
```

Or in the UI: **Settings → Secret scopes → Create scope**, then add the keys.

### 2. Persist whitelist and output in DBFS (recommended)

So runs don’t depend on ephemeral repo files:

- Create a folder (e.g. `/mnt/fmm_data` or a Unity Catalog volume) and put `competition_whitelist.json` there.
- You’ll pass this path and the output path to the job via environment variables (see below).

### 3. Create a Job that runs your repo code

- In the Databricks UI: **Workflows → Jobs → Create job**.
- **Task type:** Python script.
- **Source:** **Git** (not Workspace). Connect your repo and choose the branch; set **Script path** to the path of `databricks_job.py` inside the repo (e.g. `databricks_job.py` if it’s at repo root).
- **Cluster:** Create or select a **Job cluster** (e.g. single-node, latest LTS or “Standard” runtime).

### 4. Install dependencies on the job cluster

Use one of these:

- **Libraries (recommended):** In the job’s cluster configuration, under **Libraries**:
  - **+ Add** → **PyPI** and add: `requests`, `openpyxl`, `slack_sdk`, `tenacity`, `python-dotenv`, `aiohttp` (one by one or as a single line if your UI supports it).
- **Or requirements file:** Upload `requirements.txt` to the Workspace (or a UC volume). In the same **Libraries** section, **+ Add** → **Workspace** (or **Requirements**) and select that file so the cluster installs from it at startup.

After adding libraries, the cluster will install them before running the task.

### 5. Set environment variables (and secrets) for the job

**Important:** Add these in the job’s **Environment variables** only—**never** in **Libraries** or **Requirements** (Databricks would try to install them as pip packages and fail with `ERROR_INVALID_REQUIREMENT`).

**Where to find “Environment variables” in the UI**

- **If you use serverless compute:** The job UI often does **not** show an “Environment variables” section for serverless. Use **classic jobs compute** instead so you can set env vars (see below).
- **If you use classic jobs compute:**
  1. Open your **Job** → in the job details panel, find the **Compute** section (list of compute resources used by the task).
  2. Click the **compute resource** used by your Python script task (or **Configure** / the pencil next to it).
  3. Open **Advanced** (toggle or section).
  4. Open the **Spark** tab.
  5. Find the **Environment variables** field and add the key-value pairs below.

**If you don’t see “Environment variables”:** Switch the task to classic jobs compute: in the task’s **Compute** dropdown, choose **Classic** (or create/select a classic job cluster) instead of serverless. Then configure that cluster as above.

**Staying on serverless:** The script tries to load secrets from the `fmm_scope` scope at startup when running in Databricks (via `dbutils`). In runtimes where `dbutils` is available (e.g. some serverless or notebook-backed runs), you may not need to set any environment variables—ensure the secret scope and keys exist (step 1) and the job identity has **READ** on the scope. If `dbutils` is not available (e.g. a pure Python script task with no Spark), the script cannot read secrets and you must use **classic jobs compute** and set the env vars above.

**Values to set** (in the Environment variables field when using classic compute):

- **Required:** API key from the secret scope:
  - Name: `GENIUS_API_KEY`  
  - Value: `{{secrets/fmm_scope/genius_api_key}}`
- **Optional:** Slack token:
  - Name: `SLACK_BOT_TOKEN`  
  - Value: `{{secrets/fmm_scope/slack_bot_token}}`
- **Optional:** Paths (if you use DBFS/volume paths):
  - `WHITELIST_PATH=/dbfs/mnt/fmm_data/competition_whitelist.json`
  - `OUTPUT_EXCEL_PATH=/dbfs/mnt/fmm_data/football_competitions_fetch.xlsx`

The script reads `GENIUS_API_KEY` / `SLACK_BOT_TOKEN` from the environment; using `{{secrets/...}}` here is the correct way to inject Databricks secrets.

### 6. Schedule the job

In the job definition:

- Open **Schedule** (or **Triggers**).
- Choose **Cron** or **Interval** (e.g. daily at 6:00 AM).
- Save the job.

After that, the job will run on schedule with repo code, dependencies, and secrets; output and whitelist will be on DBFS/volume if you configured the paths above.

---

**Optional Setup:** You can copy an existing `football_competitions_fetch.xlsx` and `football_fetch_state.json` from your current PC into the working folder (or DBFS volume) if you want to resume with existing manual table edits and "deleted" matches tracking. Not required for a fresh start.
