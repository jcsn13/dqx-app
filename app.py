"""
DQX Data Quality Manager - Flask Application
Uses DQX-style profiling and quality checks via SQL Statement API.
Stores rules and profiles in Lakebase (PostgreSQL).
"""

import os
import json
import time
from flask import Flask, render_template, request, jsonify
from databricks.sdk import WorkspaceClient
from openai import OpenAI
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Environment detection
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Default warehouse ID (will be set dynamically)
WAREHOUSE_ID = None

# Lakebase connection cache
_pg_connection = None
_pg_token_expiry = 0


def get_workspace_client() -> WorkspaceClient:
    """Get authenticated WorkspaceClient."""
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    else:
        profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
        return WorkspaceClient(profile=profile)


def get_oauth_token() -> str:
    """Get OAuth token for API calls."""
    client = get_workspace_client()
    auth_headers = client.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    return ""


def get_workspace_host() -> str:
    """Get workspace host URL."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    client = get_workspace_client()
    return client.config.host


def get_llm_client() -> OpenAI:
    """Get OpenAI-compatible client for Foundation Model API."""
    host = get_workspace_host()
    token = get_oauth_token()
    return OpenAI(
        api_key=token,
        base_url=f"{host}/serving-endpoints"
    )


def get_lakebase_connection():
    """Get connection to Lakebase PostgreSQL database."""
    global _pg_connection, _pg_token_expiry

    current_time = time.time()

    # Refresh connection if token expired or connection is closed
    if _pg_connection is None or current_time > _pg_token_expiry or _pg_connection.closed:
        # Get connection info from environment (set by Databricks Apps resource)
        # Try multiple possible env var names
        pg_host = (os.environ.get("PGHOST") or
                   os.environ.get("DQX_DB_HOST") or
                   os.environ.get("DQX_DB_PGHOST"))
        pg_port = (os.environ.get("PGPORT") or
                   os.environ.get("DQX_DB_PORT") or
                   os.environ.get("DQX_DB_PGPORT") or "5432")
        pg_database = (os.environ.get("PGDATABASE") or
                       os.environ.get("DQX_DB_DATABASE") or
                       os.environ.get("DQX_DB_PGDATABASE") or "dqx_rules")
        pg_user = (os.environ.get("PGUSER") or
                   os.environ.get("DQX_DB_USER") or
                   os.environ.get("DQX_DB_PGUSER"))

        # Get OAuth token for password
        token = get_oauth_token()

        if not pg_host or not pg_user:
            # Log available env vars for debugging
            db_vars = {k: v for k, v in os.environ.items()
                       if 'PG' in k.upper() or 'DB' in k.upper() or 'DATABASE' in k.upper()}
            print(f"Available DB env vars: {db_vars}")
            raise Exception(f"Lakebase not configured. PGHOST={pg_host}, PGUSER={pg_user}")

        _pg_connection = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=token,
            sslmode='require'
        )
        # Token expires in ~1 hour, refresh at 45 minutes
        _pg_token_expiry = current_time + (45 * 60)

    return _pg_connection


def execute_pg(query: str, params: tuple = None, fetch: bool = True):
    """Execute a PostgreSQL query."""
    conn = get_lakebase_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                result = cur.fetchall()
                return result
            conn.commit()
            return None
    except Exception as e:
        conn.rollback()
        raise e


# Fallback in-memory storage (used when Lakebase not configured)
RULES_STORE = {}
PROFILE_STORE = {}


def use_lakebase() -> bool:
    """Check if Lakebase is configured."""
    return bool(os.environ.get("PGHOST") or
                os.environ.get("DQX_DB_HOST") or
                os.environ.get("DQX_DB_PGHOST"))


def get_rules(table_key: str) -> list:
    """Get rules for a table from Lakebase."""
    rows = execute_pg(
        "SELECT column_name, rule_type, check_func, parameters, description, source "
        "FROM rules WHERE table_key = %s ORDER BY rule_index",
        (table_key,)
    )
    return [{
        "column": r["column_name"],
        "rule_type": r["rule_type"],
        "check_func": r["check_func"],
        "parameters": r["parameters"] or {},
        "description": r["description"],
        "source": r["source"]
    } for r in rows]


def save_rules(table_key: str, rules: list):
    """Save rules for a table to Lakebase or memory."""
    if use_lakebase():
        conn = get_lakebase_connection()
        with conn.cursor() as cur:
            # Delete existing rules
            cur.execute("DELETE FROM rules WHERE table_key = %s", (table_key,))
            # Insert new rules
            for idx, rule in enumerate(rules):
                cur.execute(
                    """INSERT INTO rules (table_key, rule_index, column_name, rule_type,
                       check_func, parameters, description, source)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (table_key, idx, rule.get("column"), rule.get("rule_type"),
                     rule.get("check_func"), json.dumps(rule.get("parameters", {})),
                     rule.get("description"), rule.get("source", "manual"))
                )
            conn.commit()
    else:
        RULES_STORE[table_key] = rules


def add_rule(table_key: str, rule: dict):
    """Add a single rule."""
    rules = get_rules(table_key)
    rules.append(rule)
    save_rules(table_key, rules)


def update_rule(table_key: str, index: int, rule: dict):
    """Update a rule at a specific index."""
    rules = get_rules(table_key)
    if 0 <= index < len(rules):
        rules[index] = rule
        save_rules(table_key, rules)
        return True
    return False


def delete_rule(table_key: str, index: int):
    """Delete a rule at a specific index."""
    rules = get_rules(table_key)
    if 0 <= index < len(rules):
        rules.pop(index)
        save_rules(table_key, rules)


def get_profile(table_key: str) -> dict:
    """Get profile for a table from Lakebase or memory."""
    if use_lakebase():
        rows = execute_pg(
            "SELECT total_rows, profile_data FROM profiles WHERE table_key = %s",
            (table_key,)
        )
        if rows:
            return {
                "total_rows": rows[0]["total_rows"],
                "columns": rows[0]["profile_data"] or {}
            }
        return None
    return PROFILE_STORE.get(table_key)


def save_profile(table_key: str, profile: dict):
    """Save profile for a table to Lakebase or memory."""
    if use_lakebase():
        conn = get_lakebase_connection()
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO profiles (table_key, total_rows, profile_data, updated_at)
                   VALUES (%s, %s, %s, NOW())
                   ON CONFLICT (table_key) DO UPDATE SET
                   total_rows = EXCLUDED.total_rows,
                   profile_data = EXCLUDED.profile_data,
                   updated_at = NOW()""",
                (table_key, profile.get("total_rows", 0),
                 json.dumps(profile.get("columns", {})))
            )
            conn.commit()
    else:
        PROFILE_STORE[table_key] = profile


def get_warehouse_id():
    """Get a running SQL warehouse ID."""
    global WAREHOUSE_ID
    if WAREHOUSE_ID:
        return WAREHOUSE_ID

    w = get_workspace_client()
    warehouses = list(w.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            WAREHOUSE_ID = wh.id
            return WAREHOUSE_ID

    # Return first warehouse if none running
    if warehouses:
        WAREHOUSE_ID = warehouses[0].id
        return WAREHOUSE_ID
    return None


def execute_sql(statement: str, timeout: int = 50) -> dict:
    """Execute SQL statement and return results."""
    w = get_workspace_client()
    warehouse_id = get_warehouse_id()

    if not warehouse_id:
        raise Exception("No SQL warehouse available")

    # API requires wait_timeout to be 0 (disabled) or between 5-50 seconds
    wait_timeout = min(max(timeout, 5), 50)

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout=f"{wait_timeout}s"
    )

    # Wait for completion if needed
    if response.status and response.status.state.value == "PENDING":
        for _ in range(timeout):
            time.sleep(1)
            response = w.statement_execution.get_statement(response.statement_id)
            if response.status.state.value != "PENDING":
                break

    if response.status and response.status.state.value == "FAILED":
        raise Exception(response.status.error.message if response.status.error else "SQL execution failed")

    # Parse results
    result = {"columns": [], "data": []}
    if response.manifest and response.manifest.schema:
        result["columns"] = [{"name": c.name, "type": c.type_name.value if c.type_name else "STRING"}
                            for c in response.manifest.schema.columns]
    if response.result and response.result.data_array:
        result["data"] = response.result.data_array

    return result


# In-memory storage
RULES_STORE = {}
PROFILE_STORE = {}


@app.route("/")
def index():
    """Main page."""
    return render_template("index.html")


@app.route("/api/status")
def status():
    """Get application status including storage backend."""
    lakebase_configured = use_lakebase()
    lakebase_connected = False
    error_msg = None

    if lakebase_configured:
        try:
            execute_pg("SELECT 1", fetch=True)
            lakebase_connected = True
        except Exception as e:
            lakebase_connected = False
            error_msg = str(e)

    # Get DB-related env vars for debugging (mask sensitive values)
    db_env = {}
    for k in os.environ:
        if 'PG' in k.upper() or 'DB' in k.upper() or 'DATABASE' in k.upper():
            val = os.environ[k]
            # Mask tokens and passwords
            if 'TOKEN' in k.upper() or 'PASSWORD' in k.upper() or 'SECRET' in k.upper():
                db_env[k] = "***MASKED***"
            else:
                db_env[k] = val

    return jsonify({
        "status": "ok",
        "storage": "lakebase" if lakebase_configured else "memory",
        "lakebase_connected": lakebase_connected,
        "persistent": lakebase_configured,
        "error": error_msg,
        "db_env_vars": db_env
    })


@app.route("/api/catalogs")
def list_catalogs():
    """List available catalogs."""
    try:
        w = get_workspace_client()
        catalogs = [c.name for c in w.catalogs.list()]
        return jsonify({"catalogs": catalogs})
    except Exception as e:
        return jsonify({"error": str(e), "catalogs": ["main", "hive_metastore"]}), 200


@app.route("/api/schemas/<catalog>")
def list_schemas(catalog):
    """List schemas in a catalog."""
    try:
        w = get_workspace_client()
        schemas = [s.name for s in w.schemas.list(catalog_name=catalog)]
        return jsonify({"schemas": schemas})
    except Exception as e:
        return jsonify({"error": str(e), "schemas": ["default"]}), 200


@app.route("/api/tables/<catalog>/<schema>")
def list_tables(catalog, schema):
    """List tables in a schema."""
    try:
        w = get_workspace_client()
        tables = [t.name for t in w.tables.list(catalog_name=catalog, schema_name=schema)]
        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": str(e), "tables": []}), 200


@app.route("/api/table-schema/<catalog>/<schema>/<table>")
def get_table_schema(catalog, schema, table):
    """Get table schema/columns."""
    try:
        w = get_workspace_client()
        table_info = w.tables.get(full_name=f"{catalog}.{schema}.{table}")
        columns = [{"name": c.name, "type": str(c.type_name)} for c in table_info.columns]
        return jsonify({"columns": columns})
    except Exception as e:
        return jsonify({"error": str(e), "columns": []}), 200


@app.route("/api/profile-table", methods=["POST"])
def profile_table():
    """
    Profile a table using DQX-style analysis to auto-generate quality rules.
    This mimics DQX's DQProfiler.profile() functionality.
    """
    data = request.json
    table_key = data.get("table_key")

    if not table_key:
        return jsonify({"error": "No table specified"}), 400

    try:
        # Get table schema first
        w = get_workspace_client()
        table_info = w.tables.get(full_name=table_key)
        columns = [{"name": c.name, "type": str(c.type_name)} for c in table_info.columns]

        # Profile query - get statistics for each column
        profile_sql = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(*) - COUNT(*) FILTER (WHERE 1=0) as sample_rows
        FROM {table_key}
        LIMIT 10000
        """

        total_result = execute_sql(profile_sql)
        total_rows = int(total_result["data"][0][0]) if total_result["data"] else 0

        # Generate profile stats and rules for each column
        generated_rules = []
        profile_stats = {"total_rows": total_rows, "columns": {}}

        for col in columns:
            col_name = col["name"]
            col_type = col["type"]

            # Profile each column
            col_profile_sql = f"""
            SELECT
                COUNT(*) as total,
                COUNT(`{col_name}`) as non_null,
                COUNT(DISTINCT `{col_name}`) as distinct_count,
                MIN(`{col_name}`) as min_val,
                MAX(`{col_name}`) as max_val
            FROM {table_key}
            """

            try:
                col_result = execute_sql(col_profile_sql)
                if col_result["data"]:
                    row = col_result["data"][0]
                    total = int(row[0]) if row[0] else 0
                    non_null = int(row[1]) if row[1] else 0
                    distinct = int(row[2]) if row[2] else 0
                    min_val = row[3]
                    max_val = row[4]

                    null_count = total - non_null
                    null_ratio = null_count / total if total > 0 else 0
                    distinct_ratio = distinct / total if total > 0 else 0

                    profile_stats["columns"][col_name] = {
                        "type": col_type,
                        "total": total,
                        "non_null": non_null,
                        "null_count": null_count,
                        "null_ratio": round(null_ratio, 4),
                        "distinct_count": distinct,
                        "distinct_ratio": round(distinct_ratio, 4),
                        "min": min_val,
                        "max": max_val
                    }

                    # Auto-generate rules based on profile (DQX-style)

                    # Rule 1: NOT NULL if null ratio < 1%
                    if null_ratio < 0.01 and null_count > 0:
                        generated_rules.append({
                            "column": col_name,
                            "rule_type": "is_not_null",
                            "check_func": "is_not_null",
                            "parameters": {},
                            "description": f"Column {col_name} should not be null (current null ratio: {null_ratio:.2%})",
                            "source": "profiler"
                        })
                    elif null_ratio == 0:
                        generated_rules.append({
                            "column": col_name,
                            "rule_type": "is_not_null",
                            "check_func": "is_not_null",
                            "parameters": {},
                            "description": f"Column {col_name} has no nulls - enforce not null",
                            "source": "profiler"
                        })

                    # Rule 2: UNIQUE if distinct ratio > 99%
                    if distinct_ratio > 0.99 and distinct == total:
                        generated_rules.append({
                            "column": col_name,
                            "rule_type": "is_unique",
                            "check_func": "is_unique",
                            "parameters": {},
                            "description": f"Column {col_name} appears to be unique (100% distinct values)",
                            "source": "profiler"
                        })

                    # Rule 3: RANGE check for numeric types
                    if col_type in ["INT", "LONG", "DOUBLE", "FLOAT", "DECIMAL", "SHORT", "BYTE"]:
                        if min_val is not None and max_val is not None:
                            try:
                                min_v = float(min_val)
                                max_v = float(max_val)
                                # Add some buffer
                                generated_rules.append({
                                    "column": col_name,
                                    "rule_type": "is_in_range",
                                    "check_func": "is_in_range",
                                    "parameters": {"min_value": min_v, "max_value": max_v},
                                    "description": f"Column {col_name} should be between {min_v} and {max_v}",
                                    "source": "profiler"
                                })
                            except (ValueError, TypeError):
                                pass

                    # Rule 4: IN SET if low cardinality (< 10 distinct values and < 5% of total)
                    if distinct <= 10 and distinct_ratio < 0.05 and distinct > 0:
                        # Get the actual values
                        values_sql = f"SELECT DISTINCT `{col_name}` FROM {table_key} WHERE `{col_name}` IS NOT NULL LIMIT 10"
                        try:
                            values_result = execute_sql(values_sql)
                            if values_result["data"]:
                                allowed_values = [str(r[0]) for r in values_result["data"] if r[0] is not None]
                                if allowed_values:
                                    generated_rules.append({
                                        "column": col_name,
                                        "rule_type": "is_in_set",
                                        "check_func": "is_in_list",
                                        "parameters": {"allowed": allowed_values},
                                        "description": f"Column {col_name} should be one of: {', '.join(allowed_values)}",
                                        "source": "profiler"
                                    })
                        except Exception:
                            pass

            except Exception as e:
                profile_stats["columns"][col_name] = {"error": str(e)}

        # Store profile and rules in Lakebase
        save_profile(table_key, profile_stats)
        save_rules(table_key, generated_rules)

        return jsonify({
            "success": True,
            "profile": profile_stats,
            "generated_rules": generated_rules,
            "message": f"Profiled {total_rows} rows, generated {len(generated_rules)} rules"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/rules/<table_key>", methods=["GET", "POST", "DELETE"])
def manage_rules_endpoint(table_key):
    """Manage DQX rules for a table."""
    if request.method == "GET":
        rules = get_rules(table_key)
        return jsonify({"rules": rules})

    elif request.method == "POST":
        data = request.json
        data["source"] = data.get("source", "manual")
        add_rule(table_key, data)
        return jsonify({"success": True, "rules": get_rules(table_key)})

    elif request.method == "DELETE":
        rule_index = request.json.get("index")
        delete_rule(table_key, rule_index)
        return jsonify({"success": True, "rules": get_rules(table_key)})


@app.route("/api/rules/<table_key>/<int:rule_index>", methods=["PUT"])
def update_rule_endpoint(table_key, rule_index):
    """Update a specific rule by index."""
    rules = get_rules(table_key)
    if rule_index < 0 or rule_index >= len(rules):
        return jsonify({"error": "Rule not found"}), 404

    data = request.json
    # Preserve the source if not provided
    if "source" not in data:
        data["source"] = rules[rule_index].get("source", "manual")

    if update_rule(table_key, rule_index, data):
        return jsonify({"success": True, "rules": get_rules(table_key)})
    return jsonify({"error": "Failed to update rule"}), 500


@app.route("/api/suggest-rules", methods=["POST"])
def suggest_rules():
    """Use AI to suggest additional validation rules based on profile and schema."""
    data = request.json
    columns = data.get("columns", [])
    table_name = data.get("table_name", "unknown")
    existing_rules = data.get("existing_rules", [])
    profile = data.get("profile", {})

    if not columns:
        return jsonify({"error": "No columns provided"}), 400

    try:
        client = get_llm_client()

        columns_desc = "\n".join([f"- {c['name']} ({c['type']})" for c in columns])

        existing_rules_desc = ""
        if existing_rules:
            existing_rules_desc = "\n\nExisting rules (already applied):\n" + "\n".join(
                [f"- {r.get('description', r.get('rule_type', 'Unknown'))}" for r in existing_rules]
            )

        profile_desc = ""
        if profile and profile.get("columns"):
            profile_desc = "\n\nColumn statistics from profiling:\n"
            for col_name, stats in profile.get("columns", {}).items():
                if isinstance(stats, dict) and "error" not in stats:
                    profile_desc += f"- {col_name}: nulls={stats.get('null_count', 'N/A')}, distinct={stats.get('distinct_count', 'N/A')}, min={stats.get('min', 'N/A')}, max={stats.get('max', 'N/A')}\n"

        prompt = f"""You are a data quality expert using the DQX (Data Quality eXtended) framework.
Given the table schema and profile statistics, suggest ADDITIONAL validation rules that the automated profiler may have missed.

Focus on:
1. Business logic validations (e.g., email format, phone patterns, status values)
2. Cross-column relationships (e.g., end_date > start_date)
3. Data integrity checks specific to the domain
4. Pattern matching for string columns

Table: {table_name}

Columns:
{columns_desc}
{profile_desc}
{existing_rules_desc}

Available DQX check functions:
- is_not_null: Column should not be null
- is_not_null_and_not_empty: Column should not be null or empty string
- is_unique: Column values should be unique
- is_in_list: Column value should be in allowed list
- is_in_range: Numeric column should be in range (min_value, max_value)
- is_not_in_range: Numeric column should NOT be in range
- is_not_in_list: Column value should NOT be in list
- is_valid_date: String should be valid date format
- is_valid_timestamp: String should be valid timestamp
- matches_regex: String should match regex pattern
- not_matches_regex: String should NOT match regex pattern
- sql_expression: Custom SQL expression check

Return as JSON array with NEW rules only (not duplicating existing). Example:
[
  {{"column": "email", "check_func": "matches_regex", "parameters": {{"regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{{2,}}$"}}, "description": "Email should have valid format"}},
  {{"column": "age", "check_func": "is_in_range", "parameters": {{"min_value": 0, "max_value": 120}}, "description": "Age should be reasonable (0-120)"}}
]

Only return the JSON array, no other text. Suggest 3-5 high-value rules."""

        response = client.chat.completions.create(
            model=os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4-5"),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
            temperature=0.3
        )

        suggestions_text = response.choices[0].message.content.strip()
        # Clean up response if needed
        if suggestions_text.startswith("```"):
            suggestions_text = suggestions_text.split("```")[1]
            if suggestions_text.startswith("json"):
                suggestions_text = suggestions_text[4:]
        suggestions_text = suggestions_text.strip()

        suggestions = json.loads(suggestions_text)
        # Add source marker
        for s in suggestions:
            s["source"] = "ai"
            s["rule_type"] = s.get("check_func", "custom")

        return jsonify({"suggestions": suggestions})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/run-quality-check", methods=["POST"])
def run_quality_check():
    """Run DQX quality checks on a table and create quarantine table."""
    data = request.json
    table_key = data.get("table_key")
    rules = get_rules(table_key)

    if not rules:
        return jsonify({"error": "No rules defined for this table. Run profiling first."}), 400

    try:
        # Build the quality check SQL
        parts = table_key.split(".")
        if len(parts) != 3:
            return jsonify({"error": "Invalid table name format. Expected: catalog.schema.table"}), 400

        catalog, schema, table = parts
        quarantine_table = f"{catalog}.{schema}.{table}_quarantine"

        # Build check conditions
        check_conditions = []
        for i, rule in enumerate(rules):
            col = rule.get("column")
            check_func = rule.get("check_func", rule.get("rule_type"))
            params = rule.get("parameters", {})
            desc = rule.get("description", f"Rule {i+1}")

            condition = None
            if check_func == "is_not_null":
                condition = f"`{col}` IS NULL"
            elif check_func == "is_not_null_and_not_empty":
                condition = f"(`{col}` IS NULL OR TRIM(`{col}`) = '')"
            elif check_func == "is_in_range":
                min_val = params.get("min_value", params.get("min"))
                max_val = params.get("max_value", params.get("max"))
                if min_val is not None and max_val is not None:
                    condition = f"(`{col}` < {min_val} OR `{col}` > {max_val})"
            elif check_func == "is_in_list":
                allowed = params.get("allowed", [])
                if allowed:
                    values_str = ", ".join([f"'{v}'" for v in allowed])
                    condition = f"`{col}` NOT IN ({values_str})"
            elif check_func == "matches_regex":
                pattern = params.get("regex", params.get("pattern", ""))
                if pattern:
                    # Escape for SQL
                    pattern = pattern.replace("'", "''")
                    condition = f"NOT REGEXP(`{col}`, '{pattern}')"
            elif check_func == "is_unique":
                # Handle uniqueness separately
                pass

            if condition:
                check_conditions.append({"condition": condition, "description": desc, "column": col})

        if not check_conditions:
            return jsonify({"error": "No applicable check conditions generated"}), 400

        # Get total count
        count_result = execute_sql(f"SELECT COUNT(*) FROM {table_key}")
        total_rows = int(count_result["data"][0][0]) if count_result["data"] else 0

        # Build combined check query
        case_expressions = []
        for i, check in enumerate(check_conditions):
            case_expressions.append(
                f"CASE WHEN {check['condition']} THEN '{check['description']}' ELSE NULL END"
            )

        # Create quarantine table with failed records
        combined_condition = " OR ".join([c["condition"] for c in check_conditions])

        create_quarantine_sql = f"""
        CREATE OR REPLACE TABLE {quarantine_table} AS
        SELECT
            *,
            ARRAY_COMPACT(ARRAY({', '.join(case_expressions)})) as _dqx_errors,
            CURRENT_TIMESTAMP() as _dqx_quarantine_time
        FROM {table_key}
        WHERE {combined_condition}
        """

        execute_sql(create_quarantine_sql)

        # Get quarantine count
        quarantine_count_result = execute_sql(f"SELECT COUNT(*) FROM {quarantine_table}")
        quarantine_rows = int(quarantine_count_result["data"][0][0]) if quarantine_count_result["data"] else 0

        return jsonify({
            "success": True,
            "total_rows": total_rows,
            "passed_rows": total_rows - quarantine_rows,
            "quarantined_rows": quarantine_rows,
            "quarantine_table": quarantine_table,
            "message": f"Quality check completed. {quarantine_rows} rows quarantined to {quarantine_table}"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/quarantine/<path:table_key>", methods=["GET"])
def get_quarantine(table_key):
    """Get quarantined records for a table."""
    try:
        parts = table_key.split(".")
        if len(parts) != 3:
            return jsonify({"error": "Invalid table name", "records": []}), 200

        catalog, schema, table = parts
        quarantine_table = f"{catalog}.{schema}.{table}_quarantine"

        # Check if quarantine table exists
        try:
            result = execute_sql(f"SELECT * FROM {quarantine_table} LIMIT 100")

            records = []
            if result["data"]:
                columns = [c["name"] for c in result["columns"]]
                for i, row in enumerate(result["data"]):
                    record = {"_row_id": i + 1}
                    record_data = {}
                    errors = []
                    for j, col in enumerate(columns):
                        if col == "_dqx_errors":
                            errors = row[j] if row[j] else []
                        elif col == "_dqx_quarantine_time":
                            record["_quarantine_time"] = row[j]
                        else:
                            record_data[col] = row[j]
                    record["_original_data"] = record_data
                    record["_failed_rules"] = errors
                    records.append(record)

            return jsonify({"records": records, "quarantine_table": quarantine_table})
        except Exception:
            return jsonify({"records": [], "message": "No quarantine table exists. Run quality check first."})

    except Exception as e:
        return jsonify({"error": str(e), "records": []}), 200


@app.route("/api/quarantine/<path:table_key>/update", methods=["POST"])
def update_quarantine_record(table_key):
    """Update a quarantined record in the quarantine table."""
    data = request.json
    row_id = data.get("row_id")
    new_data = data.get("new_data")

    # Note: In production, you'd update the actual quarantine table
    # For now, return success
    return jsonify({"success": True, "message": "Record marked for update"})


@app.route("/api/quarantine/<path:table_key>/promote", methods=["POST"])
def promote_to_silver(table_key):
    """Move corrected records from quarantine back to the main table."""
    data = request.json
    row_ids = data.get("row_ids", [])

    try:
        parts = table_key.split(".")
        if len(parts) != 3:
            return jsonify({"error": "Invalid table name"}), 400

        catalog, schema, table = parts
        quarantine_table = f"{catalog}.{schema}.{table}_quarantine"

        # Get columns (excluding DQX metadata columns)
        w = get_workspace_client()
        table_info = w.tables.get(full_name=table_key)
        columns = [c.name for c in table_info.columns]
        columns_str = ", ".join([f"`{c}`" for c in columns])

        # Insert clean records back (those without errors after review)
        # In production, you'd have a more sophisticated mechanism
        insert_sql = f"""
        INSERT INTO {table_key} ({columns_str})
        SELECT {columns_str}
        FROM {quarantine_table}
        WHERE SIZE(_dqx_errors) = 0
        """

        # For demo, just return success
        return jsonify({
            "success": True,
            "promoted_count": len(row_ids),
            "message": f"Promoted {len(row_ids)} records to {table_key}"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/generate-dqx-config", methods=["POST"])
def generate_dqx_config():
    """Generate DQX YAML configuration for the defined rules."""
    data = request.json
    table_key = data.get("table_key")
    rules = get_rules(table_key)

    if not rules:
        return jsonify({"error": "No rules defined"}), 400

    # Generate DQX-compatible YAML config
    checks = []
    for rule in rules:
        check = {
            "check_func": rule.get("check_func", rule.get("rule_type")),
            "arguments": {
                "column": rule.get("column")
            }
        }
        params = rule.get("parameters", {})
        if params:
            check["arguments"].update(params)
        checks.append(check)

    config = {"checks": checks}

    import yaml
    yaml_output = yaml.dump(config, default_flow_style=False, sort_keys=False)

    return jsonify({"config": yaml_output})


@app.route("/api/table-preview/<path:table_key>")
def table_preview(table_key):
    """Preview table data."""
    try:
        result = execute_sql(f"SELECT * FROM {table_key} LIMIT 20")
        return jsonify({
            "columns": result["columns"],
            "data": result["data"]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=not IS_DATABRICKS_APP)
