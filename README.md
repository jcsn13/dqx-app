# DQX Data Quality Manager

A Databricks App for managing data quality rules and profiling on Unity Catalog tables. It uses DQX-style profiling and quality checks via the Databricks SQL Statement API and stores rules and profiles in Lakebase (PostgreSQL).

**Status: In development.** Functionality and configuration may change.

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

### Deploy to Databricks (DABs)

Deploy using [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/). The app uses the **existing** Unity Catalog catalog `agentbricks_catalog` (Lakebase-backed); the bundle does not create any catalog or instance, so no `CREATE CATALOG` permission is required.

**Prerequisites:** Databricks CLI 0.239.0+, `databricks auth login`, and a `~/.databrickscfg` profile. Set `workspace.profile` in `databricks.yml` per target. Ensure the Lakebase instance that backs `agentbricks_catalog` exists; if its name is not `agentbricks-lakebase`, set `lakebase_instance_name` in `databricks.yml` for that target.

**Commands:**

```bash
# Validate bundle (uses target dev by default)
databricks bundle validate -t dev

# Deploy app (attaches to existing catalog agentbricks_catalog)
databricks bundle deploy -t dev

# Start the app (required after deploy)
databricks bundle run dqx -t dev
```

For production, use `-t prod` and set the `prod` target’s `workspace.profile` and `lakebase_instance_name` as needed. **Bundle layout:** `databricks.yml` (targets and `lakebase_instance_name`), `resources/dqx.app.yml` (app bound to existing catalog). App env and command stay in `app.yaml`.

## Project layout

- `app.py` – Flask app: API routes, Lakebase/SQL execution, rule and profile storage, AI suggestion integration.
- `app.yaml` – Databricks App manifest (command, env, Lakebase resource).
- `requirements.txt` – Python dependencies (Flask, Databricks SDK, DQX, OpenAI client, psycopg2, etc.).
- `templates/index.html` – Single-page UI for catalogs, profiling, and rules.
- `databricks.yml` – Asset Bundle config (targets, `lakebase_instance_name` for existing catalog).
- `resources/dqx.app.yml` – App resource bound to existing catalog `agentbricks_catalog` (used by `databricks bundle deploy` / `bundle run`).
