# DQX Data Quality Manager

A Databricks App for managing data quality rules and profiling on Unity Catalog tables. It uses DQX-style profiling and quality checks via the Databricks SQL Statement API and stores rules and profiles in Lakebase (PostgreSQL).

**Status: In development.** Functionality and configuration may change. An automatic deployment script for Databricks Apps is planned and will be added in a future update.

## Features

- **Catalog browser** – List catalogs, schemas, and tables in your workspace.
- **Table profiling** – Run DQX-style profiling on selected tables (row counts, column stats).
- **Data quality rules** – Define, edit, and run rules per table (e.g. null checks, ranges, uniqueness).
- **AI rule suggestions** – Optional rule suggestions using the Databricks Foundation Model API (configurable serving endpoint).
- **Persistent storage** – Rules and profiles stored in Lakebase when the app is configured with a Lakebase database resource; otherwise uses in-memory storage.

## Requirements

- Python 3.10+
- A Databricks workspace with Unity Catalog and (for full features) a SQL warehouse and Lakebase instance.
- For AI suggestions: a Foundation Model serving endpoint (e.g. `databricks-claude-sonnet-4-5`).

## Setup

### Local development

1. Clone the repo and create a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate   # or .venv\Scripts\activate on Windows
   pip install -r requirements.txt
   ```

2. Configure Databricks (one of):
   - `databricks configure` and use the default profile, or
   - Set `DATABRICKS_PROFILE` to a profile in `~/.databrickscfg`.

3. (Optional) Lakebase for persistent rules/profiles: set `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and use your OAuth token as the password, or point the app at a Lakebase instance that accepts token auth.

4. Run the app:

   ```bash
   python app.py
   ```

   By default the app serves at `http://127.0.0.1:5000`.

### Running as a Databricks App

The app is designed to run as a [Databricks App](https://docs.databricks.com/apps). The `app.yaml` defines:

- **Command:** `python app.py`
- **Env:** `SERVING_ENDPOINT` for the Foundation Model endpoint used for AI rule suggestions (e.g. `databricks-claude-sonnet-4-5`).
- **Resources:** A Lakebase database resource `dqx-db` (instance `dqx-lakebase`, database `dqx_rules`) with `CAN_CONNECT_AND_CREATE`, used for storing rules and profiles.

When deployed as an app, the platform sets `DATABRICKS_APP_NAME`, `DATABRICKS_HOST`, and the Lakebase connection environment variables (e.g. `PGHOST`, `PGUSER`, etc.) so the app can connect to the workspace and database without extra local configuration.

**Deployment:** An automatic deployment script for publishing this app to Databricks Apps is planned and will be documented here once available.

## Project layout

- `app.py` – Flask app: API routes, Lakebase/SQL execution, rule and profile storage, AI suggestion integration.
- `app.yaml` – Databricks App manifest (command, env, Lakebase resource).
- `requirements.txt` – Python dependencies (Flask, Databricks SDK, DQX, OpenAI client, psycopg2, etc.).
- `templates/index.html` – Single-page UI for catalogs, profiling, and rules.
