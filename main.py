import csv
import io

from flask import Flask, render_template, redirect, url_for, request, flash, abort, jsonify, Response
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    logout_user,
    login_required,
    current_user,
)
from datetime import date, timedelta, datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # Python < 3.9
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import re
import secrets
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from werkzeug.middleware.proxy_fix import ProxyFix

import auth
import vehicle_notify
import firebase_admin
from firebase_admin import credentials, firestore

# Simple TTL cache for expensive read-only BigQuery results (key -> (expiry_ts, value))
_cache_ttl_sec = 60
_cache_store = {}


def _cache_get(key):
    if key not in _cache_store:
        return None
    exp, val = _cache_store[key]
    if time.monotonic() < exp:
        return val
    del _cache_store[key]
    return None


def _cache_set(key, value, ttl_sec=None):
    ttl_sec = ttl_sec or _cache_ttl_sec
    _cache_store[key] = (time.monotonic() + ttl_sec, value)

# Hardcoded filter options for Machine Dashboard (labels shown in UI)
UNIT_OPTIONS = ["Unit I", "Unit II"]
SHIFT_OPTIONS = ["Shift I", "Shift II"]
DEPARTMENT_OPTIONS = ["PDC", "CNC"]
# Temporary toggle: keep IoT health monitoring page hidden/disabled.
IOT_HEALTH_MONITORING_ENABLED = False

app = Flask(__name__)
# Cloud Run / reverse proxies: trust X-Forwarded-* for correct scheme and URLs
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

_default_secret = "change-this-to-a-random-secret-key-in-production"
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", _default_secret).strip() or _default_secret
if app.config["SECRET_KEY"] == _default_secret:
    app.logger.warning(
        "SECRET_KEY env var not set; using default (sessions reset on redeploy). Set SECRET_KEY in Cloud Run."
    )

login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message = "Please log in to access this page."


SECURITY_UNIT_I_EMAIL = "security.1@alubee.com"
SECURITY_UNIT_II_EMAIL = "security.2@alubee.com"
HR_SECURITY_EMAIL = "hr@alubee.com"
IT_EMAIL = "it@alubee.com"
LOGISTICS_EMAIL = "ppc@alubee.com"

# Built-in accounts (SQLite). Change passwords after deploy in production.
_DEFAULT_ADMIN = ("admin@alubee.com", "admin123")
_DEFAULT_BUILTIN_VIEWERS = (
    (SECURITY_UNIT_I_EMAIL, "security@alubee", ("security",)),
    (SECURITY_UNIT_II_EMAIL, "security@alubee", ("security",)),
    (HR_SECURITY_EMAIL, "hr@alubee", ("hr",)),
    (IT_EMAIL, "it@alubee", ("it",)),
    (LOGISTICS_EMAIL, "ppc@alubee", ("logistics",)),
)
_VIEWER_LANDING_ROUTES = {
    "security": "security",
    "hr": "hr",
    "it": "it",
    "logistics": "logistics",
    "maintenance": "logistics",
}

_BUILTIN_EMAIL_LANDING_PAGES = {
    SECURITY_UNIT_I_EMAIL: "security",
    SECURITY_UNIT_II_EMAIL: "security",
    HR_SECURITY_EMAIL: "hr",
    IT_EMAIL: "it",
    LOGISTICS_EMAIL: "logistics",
}

_PORTAL_LANDING_PAGE_KEYS = ("security", "hr", "it", "logistics", "maintenance")


def _ensure_builtin_viewer_user(email: str, password: str, pages: tuple[str, ...]) -> None:
    """Built-in viewer logins with access to specific portal tabs only."""
    email = email.strip().lower()
    existing = auth.get_user_by_email(email)
    if existing is None:
        user_id = auth.create_user(email, password, "viewer")
        if user_id is None:
            existing = auth.get_user_by_email(email)
            user_id = existing["id"] if existing else None
    else:
        user_id = existing["id"]
        auth.set_password(user_id, password)
        conn = auth.get_db()
        conn.execute(
            "UPDATE users SET role = 'viewer' WHERE email = ?",
            (email,),
        )
        conn.commit()
        conn.close()
    if user_id:
        auth.set_viewer_pages(user_id, list(pages))


def _ensure_auth_database():
    """Create SQLite auth tables and default users. Runs on import so Gunicorn/Cloud Run work
    (the ``if __name__ == '__main__'`` block is never executed under gunicorn)."""
    try:
        auth.init_db()
        admin_email, admin_password = _DEFAULT_ADMIN
        existing = auth.get_user_by_email(admin_email)
        if existing is None:
            auth.create_user(admin_email, admin_password, "admin")
        else:
            auth.set_password(existing["id"], admin_password)
            conn = auth.get_db()
            conn.execute(
                "UPDATE users SET role = 'admin' WHERE email = ?",
                (admin_email,),
            )
            conn.commit()
            conn.close()
        for email, password, pages in _DEFAULT_BUILTIN_VIEWERS:
            _ensure_builtin_viewer_user(email, password, pages)
    except Exception as e:
        app.logger.exception("Auth database initialization failed: %s", e)


_ensure_auth_database()

# Page key for each route (used for permission checks)
PAGE_KEYS = [p[0] for p in auth.PAGE_KEYS]


@app.template_filter("minutes_hm")
def minutes_hm(value):
    """Format minutes as 'XH YM' or 'XM'. Keeps non-numeric values unchanged."""
    if value is None:
        return "-"
    try:
        minutes = int(value)
    except (TypeError, ValueError):
        return value
    if minutes <= 0:
        return "-"
    if minutes < 60:
        return f"{minutes}M"
    hours = minutes // 60
    rem = minutes % 60
    if rem == 0:
        return f"{hours}H"
    return f"{hours}H {rem}M"


_ROMAN_NUMERAL_CHARS = frozenset("IVXLCDM")


def _sentence_case_word(word):
    """Title-case one token; keep Roman numerals (e.g. II, III) uppercase."""
    if not word:
        return word
    up = word.upper()
    if len(up) <= 8 and all(c in _ROMAN_NUMERAL_CHARS for c in up):
        return up
    return word[:1].upper() + word[1:].lower()


@app.template_filter("sentence_case")
def sentence_case(value):
    """Readable label case for table text: words split on whitespace/underscore; '-' when empty."""
    if value is None or value == "":
        return "-"
    s = str(value).strip()
    if not s or s == "-":
        return "-"
    if s.upper() == "N/A":
        return "N/A"
    compact = re.sub(r"[\s\-+]+", "", s)
    if compact.isdigit():
        return s
    # Treat underscores like spaces so e.g. UNIT_II -> Unit II
    normalized = re.sub(r"[_\s]+", " ", s)
    parts = [p for p in normalized.split(" ") if p]
    if not parts:
        return "-"
    return " ".join(_sentence_case_word(p) for p in parts)


@app.template_filter("approval_cell_class")
def approval_cell_class(value):
    """CSS class for OD approval cells: approved (green), denied (red), pending (yellow)."""
    if value is None:
        return "security-appr-pending"
    s = str(value).strip().lower()
    if not s or s in ("-", "—"):
        return "security-appr-pending"
    if s in ("n/a", "na", "none") or s.startswith("n/a"):
        return "security-appr-na"
    if s == "offline":
        return "security-appr-offline"
    if "cancel" in s:
        return "security-appr-na"
    if (
        "deny" in s
        or "rejected" in s
        or "reject" in s
        or "not approved" in s
        or "disapprov" in s
        or s == "no"
    ):
        return "security-appr-denied"
    if "pending" in s or "await" in s or "wait" in s or "submitted" in s:
        return "security-appr-pending"
    if "approv" in s:
        return "security-appr-approved"
    return "security-appr-pending"


@app.template_filter("it_status_cell_class")
def it_status_cell_class(value):
    """CSS class for IT ticket status cells."""
    s = (value or "").strip().upper()
    if s == "CLOSED":
        return "security-appr-approved"
    if s == "CANCELLED":
        return "security-appr-denied"
    if s == "ASSIGNED":
        return "security-appr-pending"
    if s == "QUEUED":
        return "security-appr-na"
    return "security-appr-pending"


def _init_bigquery_client():
    """BigQuery client: env path → local JSON next to app → Application Default Credentials.

    - **Cloud Run / GCE:** use ADC (no key file in the image); set service account on the service.
    - **Local:** set ``BQ_CREDENTIALS_PATH`` to a JSON key outside the repo, or use
      ``gcloud auth application-default login``, or (legacy) place ``bq_service_acc.json`` next to
      ``main.py`` (never commit that file).

    Returns None if credentials are missing or invalid so the app can still run.
    """
    # 1) Explicit path (recommended for local dev; keep keys out of the repo)
    cred_path = (os.environ.get("BQ_CREDENTIALS_PATH") or "").strip()
    if cred_path and os.path.isfile(cred_path):
        try:
            credentials = service_account.Credentials.from_service_account_file(cred_path)
            return bigquery.Client(credentials=credentials, project=credentials.project_id)
        except Exception as e:
            app.logger.warning("BigQuery: could not load BQ_CREDENTIALS_PATH: %s", e)
            return None

    # 2) Legacy local file next to main.py (not used in production deploy)
    sa_path = os.path.join(os.path.dirname(__file__), "bq_service_acc.json")
    if os.path.isfile(sa_path):
        try:
            credentials = service_account.Credentials.from_service_account_file(sa_path)
            return bigquery.Client(credentials=credentials, project=credentials.project_id)
        except Exception as e:
            app.logger.warning("BigQuery: could not load bq_service_acc.json: %s", e)
            return None

    # 3) Application Default Credentials (Cloud Run, GCE, `gcloud auth application-default login`)
    try:
        project = (os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip() or None
        if project:
            return bigquery.Client(project=project)
        return bigquery.Client()
    except Exception as e:
        app.logger.warning("BigQuery: ADC / default client failed: %s", e)
        return None


# Lazy init: creating bigquery.Client() at import can block on metadata/ADC and miss Cloud Run's
# startup deadline before Gunicorn binds to PORT. Initialize on first use instead.
_bq_singleton = None
_bq_initialized = False


def get_bq_client():
    global _bq_singleton, _bq_initialized
    if not _bq_initialized:
        _bq_initialized = True
        _bq_singleton = _init_bigquery_client()
    return _bq_singleton


def _get_max_date_machine_idle():
    """Return the latest Date in fact_machine_idle, or None on error. Cached briefly."""
    if get_bq_client() is None:
        return None
    cache_key = ("max_date_machine_idle",)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        job = get_bq_client().query(
            "SELECT MAX(Date) AS max_date FROM `alubee_production_marts.fact_machine_idle`"
        )
        row = next(job.result(), None)
        if row and row.max_date:
            out = row.max_date.strftime("%Y-%m-%d")
        else:
            out = None
        _cache_set(cache_key, out, ttl_sec=120)
        return out
    except Exception as e:
        app.logger.warning("BigQuery max date: %s", e)
        return None


def fetch_machine_idle_rows(date_str=None, shift=None, unit=None, department=None):
    """Fetch machine idle rows from BigQuery with optional filters. Results cached briefly."""
    if get_bq_client() is None:
        return []
    cache_key = ("machine_idle", date_str, shift, unit, department)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    query = """
        SELECT
            Date,
            Shift,
            Machine_no,
            Unit,
            department,
            Break,
            Mould,
            Powercut,
            Maintenance,
            Setting,
            Manpower,
            Noload,
            Without_Notice,
            Total_Downtime_Minutes,
            Usage_Percent
        FROM `alubee_production_marts.fact_machine_idle`
        WHERE 1 = 1
    """
    params = []

    if date_str:
        query += " AND Date = @date"
        params.append(bigquery.ScalarQueryParameter("date", "DATE", date_str))

    if shift and shift != "All":
        query += " AND Shift = @shift"
        params.append(bigquery.ScalarQueryParameter("shift", "STRING", shift))

    if unit and unit != "All":
        query += " AND Unit = @unit"
        params.append(bigquery.ScalarQueryParameter("unit", "STRING", unit))

    if department and department != "All":
        query += " AND department = @department"
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))

    query += " ORDER BY Machine_no"

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        result = get_bq_client().query(query, job_config=job_config).result()
        rows = [dict(row) for row in result]
        app.logger.info("Machine idle: date=%s unit=%s shift=%s dept=%s -> %d rows", date_str, unit, shift, department, len(rows))
        _cache_set(cache_key, rows)
        return rows
    except Exception as e:
        app.logger.warning("BigQuery machine idle query failed: %s", e)
        return []


IOT_MASTER_TABLE = "alubee_production_marts.fact_iot_master"


def _iot_part_machine_map_key(part_no, shift_id, department, unit=None):
    """Stable map key for part + shift + department + unit (matches template/modal/export)."""
    p = re.sub(r"\s+", " ", str(part_no or "-").strip().upper())
    s = str(shift_id if shift_id is not None else "").strip()
    d = str(department if department is not None else "").strip()
    u = str(unit if unit is not None else "").strip()
    return f"{p}\x1f{s}\x1f{d}\x1f{u}"


def fetch_iot_master_rows(date_str=None, shift=None, unit=None, department=None):
    """Fetch production/IoT rows from fact_iot_master with machine excluded."""
    if get_bq_client() is None:
        return []
    # Versioned cache key to invalidate older grouped payloads.
    cache_key = ("iot_master_v6", date_str, shift, unit, department)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    query = """
        WITH base AS (
            SELECT
                item_code,
                cycle_time_sec,
                components_in_fixture,
                plan,
                shot,
                quantity,
                COALESCE(
                    NULLIF(UPPER(TRIM(CAST(partNo AS STRING))), ''),
                    NULLIF(UPPER(TRIM(CAST(item_code AS STRING))), ''),
                    '-'
                ) AS part_no_norm,
                COALESCE(NULLIF(TRIM(CAST(unit AS STRING)), ''), '-') AS unit_norm,
                COALESCE(NULLIF(TRIM(CAST(shift_id AS STRING)), ''), '-') AS shift_norm,
                COALESCE(NULLIF(TRIM(CAST(department AS STRING)), ''), '-') AS dept_norm
            FROM `""" + IOT_MASTER_TABLE + """`
            WHERE 1 = 1
    """
    params = []

    if date_str:
        query += " AND shift_date = @date"
        params.append(bigquery.ScalarQueryParameter("date", "DATE", date_str))

    if shift and shift != "All":
        query += " AND shift_id = @shift"
        params.append(bigquery.ScalarQueryParameter("shift", "STRING", shift))

    if unit and unit != "All":
        query += " AND unit = @unit"
        params.append(bigquery.ScalarQueryParameter("unit", "STRING", unit))

    if department and department != "All":
        query += " AND department = @department"
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))

    query += """
        )
        SELECT
            ANY_VALUE(item_code) AS item_code,
            part_no_norm AS partNo,
            unit_norm AS unit,
            shift_norm AS shift_id,
            dept_norm AS department,
            ROUND(AVG(COALESCE(CAST(cycle_time_sec AS FLOAT64), 0)), 2) AS cycle_time_sec,
            ROUND(AVG(COALESCE(CAST(components_in_fixture AS FLOAT64), 0)), 2) AS components_in_fixture,
            SUM(COALESCE(CAST(plan AS INT64), 0)) AS plan,
            SUM(COALESCE(CAST(shot AS INT64), 0)) AS shot,
            SUM(COALESCE(CAST(quantity AS INT64), 0)) AS quantity
        FROM base
        GROUP BY part_no_norm, unit_norm, shift_norm, dept_norm
        ORDER BY part_no_norm, unit_norm, shift_norm, dept_norm
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        result = get_bq_client().query(query, job_config=job_config).result()
        rows = [dict(row) for row in result]
        for row in rows:
            row["iot_context_key"] = _iot_part_machine_map_key(
                row.get("partNo"),
                row.get("shift_id"),
                row.get("department"),
                row.get("unit"),
            )
        app.logger.info("IoT master: date=%s unit=%s shift=%s dept=%s -> %d rows", date_str, unit, shift, department, len(rows))
        _cache_set(cache_key, rows)
        return rows
    except Exception as e:
        app.logger.warning("BigQuery IoT master query failed: %s", e)
        return []


def fetch_iot_part_machine_rows(date_str=None, shift=None, unit=None, department=None):
    """Fetch machine-wise IoT shot/qty for each part number."""
    if get_bq_client() is None:
        return {}
    cache_key = ("iot_part_machine_v3", date_str, shift, unit, department)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    query = """
        WITH base AS (
            SELECT
                COALESCE(
                    NULLIF(UPPER(TRIM(CAST(partNo AS STRING))), ''),
                    NULLIF(UPPER(TRIM(CAST(item_code AS STRING))), ''),
                    '-'
                ) AS part_no_norm,
                COALESCE(NULLIF(TRIM(CAST(unit AS STRING)), ''), '-') AS unit_norm,
                COALESCE(NULLIF(TRIM(CAST(shift_id AS STRING)), ''), '-') AS shift_norm,
                COALESCE(NULLIF(TRIM(CAST(department AS STRING)), ''), '-') AS dept_norm,
                COALESCE(NULLIF(TRIM(CAST(machine AS STRING)), ''), '-') AS machine,
                CAST(shot AS INT64) AS shot,
                CAST(quantity AS INT64) AS quantity
            FROM `""" + IOT_MASTER_TABLE + """`
            WHERE 1 = 1
    """
    params = []
    if date_str:
        query += " AND shift_date = @date"
        params.append(bigquery.ScalarQueryParameter("date", "DATE", date_str))
    if shift and shift != "All":
        query += " AND shift_id = @shift"
        params.append(bigquery.ScalarQueryParameter("shift", "STRING", shift))
    if unit and unit != "All":
        query += " AND unit = @unit"
        params.append(bigquery.ScalarQueryParameter("unit", "STRING", unit))
    if department and department != "All":
        query += " AND department = @department"
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))

    query += """
        )
        SELECT
            part_no_norm AS partNo,
            unit_norm AS unit,
            shift_norm AS shift_id,
            dept_norm AS department,
            machine,
            SUM(COALESCE(shot, 0)) AS shot,
            SUM(COALESCE(quantity, 0)) AS quantity
        FROM base
        GROUP BY part_no_norm, unit_norm, shift_norm, dept_norm, machine
        ORDER BY part_no_norm, unit_norm, shift_norm, dept_norm, machine
    """

    try:
        result = get_bq_client().query(
            query, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result()
        part_map = {}
        for row in result:
            key = _iot_part_machine_map_key(
                row.get("partNo"),
                row.get("shift_id"),
                row.get("department"),
                row.get("unit"),
            )
            part_map.setdefault(key, []).append(
                {
                    "machine": row.get("machine") or "-",
                    "shot": int(row.get("shot") or 0),
                    "quantity": int(row.get("quantity") or 0),
                }
            )
        _cache_set(cache_key, part_map)
        return part_map
    except Exception as e:
        app.logger.warning("BigQuery IoT part-machine query failed: %s", e)
        return {}


def fetch_realtime_latest_rows(
    unit: str | None = None,
    department: str | None = None,
    bypass_cache: bool = False,
):
    """Fetch realtime latest per machine for Production dashboard."""
    if get_bq_client() is None:
        return []

    cache_key = ("realtime_master_catalog", unit, department)
    if not bypass_cache:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    query = f"""
        WITH
          now_bounds AS (
            SELECT
              DATETIME(CURRENT_TIMESTAMP(), 'Asia/Kolkata') AS now_ist,
              DATE(CURRENT_TIMESTAMP(), 'Asia/Kolkata') AS today_ist
          ),
          shift_window AS (
            SELECT
              CASE
                WHEN EXTRACT(HOUR FROM now_ist) >= 8 AND EXTRACT(HOUR FROM now_ist) < 20 THEN
                  TIMESTAMP(DATETIME(today_ist, TIME(8, 0, 0)), 'Asia/Kolkata')
                WHEN EXTRACT(HOUR FROM now_ist) >= 20 THEN
                  TIMESTAMP(DATETIME(today_ist, TIME(20, 0, 0)), 'Asia/Kolkata')
                ELSE
                  TIMESTAMP(DATETIME(DATE_SUB(today_ist, INTERVAL 1 DAY), TIME(20, 0, 0)), 'Asia/Kolkata')
              END AS shift_start_ts,
              CASE
                WHEN EXTRACT(HOUR FROM now_ist) >= 8 AND EXTRACT(HOUR FROM now_ist) < 20 THEN
                  TIMESTAMP(DATETIME(today_ist, TIME(20, 0, 0)), 'Asia/Kolkata')
                WHEN EXTRACT(HOUR FROM now_ist) >= 20 THEN
                  TIMESTAMP(DATETIME(DATE_ADD(today_ist, INTERVAL 1 DAY), TIME(8, 0, 0)), 'Asia/Kolkata')
                ELSE
                  TIMESTAMP(DATETIME(today_ist, TIME(8, 0, 0)), 'Asia/Kolkata')
              END AS shift_end_ts
            FROM now_bounds
          ),
          shift_catalog AS (
            SELECT
              src.*,
              CASE
                WHEN SAFE_CAST(src.measurement AS INT64) = 16 THEN 'break'
                WHEN SAFE_CAST(src.measurement AS INT64) = 32 THEN 'shot'
                WHEN SAFE_CAST(src.measurement AS INT64) = 45 THEN 'without notice'
                WHEN SAFE_CAST(src.measurement AS INT64) = 18 THEN 'maintenance'
                WHEN SAFE_CAST(src.measurement AS INT64) = 5 THEN 'power cut'
                WHEN SAFE_CAST(src.measurement AS INT64) = 19 THEN 'setting'
                WHEN SAFE_CAST(src.measurement AS INT64) = 34 THEN 'mould'
                WHEN SAFE_CAST(src.measurement AS INT64) = 33 THEN 'reset'
                WHEN SAFE_CAST(src.measurement AS INT64) = 4 THEN 'manpower'
                WHEN SAFE_CAST(src.measurement AS INT64) = 17 THEN 'no load'
                ELSE NULL
              END AS description
            FROM `{REALTIME_MASTER_CATALOG_SOURCE}` AS src
            CROSS JOIN shift_window AS w
            WHERE src.publish_time >= w.shift_start_ts
              AND src.publish_time < w.shift_end_ts
          ),
          latest_any AS (
            SELECT *
            FROM shift_catalog
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY device_id
              ORDER BY publish_time DESC
            ) = 1
          ),
          latest_m32 AS (
            SELECT *
            FROM shift_catalog
            WHERE SAFE_CAST(measurement AS INT64) = 32
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY device_id
              ORDER BY publish_time DESC
            ) = 1
          )
        SELECT
          mm.Machine_no AS machine_no,
          CASE
            WHEN SAFE_CAST(la.measurement AS INT64) = 32 THEN 'Running'
            WHEN SAFE_CAST(la.value AS INT64) = 1 THEN 'Stopped'
            WHEN la.description IS NOT NULL AND SAFE_CAST(la.value AS INT64) = 0 THEN 'Running'
            ELSE 'Stopped'
          END AS status,
          COALESCE(NULLIF(CAST(l32.partNo AS STRING), ''), '-') AS part_no,
          COALESCE(CAST(l32.value AS STRING), '-') AS quantity,
          CASE
            WHEN SAFE_CAST(la.measurement AS INT64) = 32 THEN '-'
            WHEN la.description IS NOT NULL AND SAFE_CAST(la.value AS INT64) = 0 THEN '-'
            ELSE COALESCE(CAST(la.description AS STRING), '-')
          END AS idle_desc,
          CASE
            WHEN SAFE_CAST(la.measurement AS INT64) = 32 THEN NULL
            WHEN la.description IS NOT NULL AND SAFE_CAST(la.value AS INT64) = 0 THEN NULL
            ELSE
              GREATEST(
                0,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), CAST(la.publish_time AS TIMESTAMP), MINUTE)
              )
          END AS time_elapsed_minutes,
          FORMAT_TIMESTAMP(
            '%I:%M %p',
            TIMESTAMP(la.publish_time),
            'Asia/Kolkata'
          ) AS last_updated_ist
        FROM latest_any la
        LEFT JOIN latest_m32 l32
          ON l32.device_id = la.device_id
        LEFT JOIN `{DIM_MACHINE_MAPPER_TABLE}` mm
          ON mm.Device_ID = SAFE_CAST(la.device_id AS INT64)
        WHERE 1=1
    """
    params = []
    if unit and unit != "All":
        query += " AND mm.Unit = @unit"
        params.append(bigquery.ScalarQueryParameter("unit", "STRING", unit))
    if department and department != "All":
        query += " AND mm.Machine_Type = @department"
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))
    query += " ORDER BY mm.Machine_no"

    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    try:
        result = get_bq_client().query(query, job_config=job_config).result()
        rows = []
        for row in result:
            rows.append(
                {
                    "machine_no": row.get("machine_no"),
                    "status": row.get("status"),
                    "part_no": row.get("part_no") or "-",
                    "quantity": row.get("quantity") or "-",
                    "idle_desc": row.get("idle_desc") or "-",
                    "time_elapsed_minutes": row.get("time_elapsed_minutes"),
                    "last_updated_ist": row.get("last_updated_ist") or "-",
                }
            )
        _cache_set(cache_key, rows)
        return rows
    except Exception as e:
        app.logger.warning("BigQuery realtime (vw_master_catalog) query failed: %s", e)
        return []


class User(UserMixin):
    def __init__(self, id_, email, role="viewer", allowed_pages=None):
        self.id = id_
        self.email = email
        self.role = role or "viewer"
        self.allowed_pages = allowed_pages or []

    @staticmethod
    def get(user_id):
        conn = auth.get_db()
        row = conn.execute(
            "SELECT id, email, role FROM users WHERE id = ?", (int(user_id),)
        ).fetchone()
        conn.close()
        if not row:
            return None
        role = (row["role"] or "viewer").strip().lower()
        allowed = auth.get_viewer_pages(row["id"]) if role == "viewer" else []
        return User(
            id_=row["id"],
            email=row["email"],
            role=role,
            allowed_pages=allowed,
        )


@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id)


def _landing_url_for_page(page_key: str) -> str:
    route = _VIEWER_LANDING_ROUTES.get(page_key)
    if not route:
        return url_for("index")
    if page_key == "maintenance":
        return url_for(route, tab="maintenance")
    return url_for(route)


def _login_landing_url(user: User) -> str:
    """Where to send the user after login."""
    role = (user.role or "").strip().lower()
    if role in ("admin", "editor"):
        return url_for("index")

    if role == "viewer":
        email = (user.email or "").strip().lower()
        builtin_page = _BUILTIN_EMAIL_LANDING_PAGES.get(email)
        if builtin_page:
            return _landing_url_for_page(builtin_page)

        pages = list(user.allowed_pages or [])
        portal_pages = [p for p in pages if p in _PORTAL_LANDING_PAGE_KEYS]
        if len(portal_pages) == 1:
            return _landing_url_for_page(portal_pages[0])

    return url_for("index")


def _user_has_ppc_access():
    """Same rule as Department / PPC menu: admin, editor, or viewer with 'ppc' page."""
    if not current_user.is_authenticated:
        return False
    role = getattr(current_user, "role", "") or ""
    if role == "admin" or role == "editor":
        return True
    pages = getattr(current_user, "allowed_pages", None) or []
    return "ppc" in pages


def _user_has_iot_access():
    """IoT: admin/editor, or viewer with 'iot' or legacy 'ppc' (same shop floor users)."""
    if not current_user.is_authenticated:
        return False
    role = getattr(current_user, "role", "") or ""
    if role == "admin" or role == "editor":
        return True
    pages = getattr(current_user, "allowed_pages", None) or []
    return "iot" in pages or "ppc" in pages


def _user_is_admin() -> bool:
    return (
        current_user.is_authenticated
        and (getattr(current_user, "role", "") or "").strip().lower() == "admin"
    )


def _user_can_access_security():
    """Security gate APIs: admin/editor, or viewer with 'security' page."""
    if not current_user.is_authenticated:
        return False
    role = (getattr(current_user, "role", "") or "").strip().lower()
    if role in ("admin", "editor"):
        return True
    pages = getattr(current_user, "allowed_pages", None) or []
    return "security" in pages


def _firebase_cred_path():
    for key in ("FIREBASE_CREDENTIALS_PATH", "GOOGLE_APPLICATION_CREDENTIALS"):
        env_path = (os.environ.get(key) or "").strip()
        if env_path:
            return env_path
    base = os.path.dirname(os.path.abspath(__file__))
    in_app = os.path.join(base, "firebase-adminsdk.json")
    if os.path.isfile(in_app):
        return in_app
    return os.path.normpath(os.path.join(os.path.dirname(base), "firebase-adminsdk.json"))


def _firebase_project_id():
    return (
        (os.environ.get("FIREBASE_PROJECT_ID") or "").strip()
        or "whatsapp-approval-system"
    )


def _running_on_cloud_run():
    return bool(os.environ.get("K_SERVICE"))


# Env vars that force a JSON key file (disabled keys cause invalid_grant / Invalid JWT).
_CLOUD_RUN_STRIP_CRED_ENV = (
    "GOOGLE_APPLICATION_CREDENTIALS",
    "FIREBASE_CREDENTIALS_PATH",
    "FIREBASE_CREDENTIALS_JSON",
)


@contextmanager
def _cloud_run_metadata_credentials_env():
    """On Cloud Run, use the runtime service account (metadata), not mounted JSON keys."""
    saved = {k: os.environ.pop(k) for k in _CLOUD_RUN_STRIP_CRED_ENV if k in os.environ}
    try:
        yield
    finally:
        os.environ.update(saved)


def _delete_firebase_app():
    try:
        firebase_admin.delete_app(firebase_admin.get_app())
    except (ValueError, AttributeError):
        pass


def _init_firebase_app():
    """Initialize Firebase once. Cloud Run: metadata ADC only. Returns error text or None."""
    project_id = _firebase_project_id()
    init_options = {"projectId": project_id} if project_id else None

    def _try_adc():
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, init_options)

    if _running_on_cloud_run():
        if any(os.environ.get(k) for k in _CLOUD_RUN_STRIP_CRED_ENV):
            app.logger.warning(
                "Cloud Run: ignoring FIREBASE_CREDENTIALS_JSON / GOOGLE_APPLICATION_CREDENTIALS "
                "(disabled keys cause Invalid JWT). Remove those secrets from the service."
            )
        _delete_firebase_app()
        try:
            with _cloud_run_metadata_credentials_env():
                _try_adc()
            return None
        except Exception as e:
            app.logger.exception("Firebase ADC on Cloud Run failed")
            return (
                "Firestore auth failed on Cloud Run. Remove FIREBASE_CREDENTIALS_JSON and "
                "GOOGLE_APPLICATION_CREDENTIALS from the service, then grant the Cloud Run "
                "service account role Cloud Datastore User on whatsapp-approval-system. "
                f"Detail: {e}"
            )

    try:
        firebase_admin.get_app()
        return None
    except ValueError:
        pass

    json_raw = (os.environ.get("FIREBASE_CREDENTIALS_JSON") or "").strip()
    if json_raw:
        try:
            cred = credentials.Certificate(json.loads(json_raw))
            firebase_admin.initialize_app(cred, init_options)
            return None
        except Exception as e:
            app.logger.exception("Firebase FIREBASE_CREDENTIALS_JSON failed")
            if _running_on_cloud_run():
                return (
                    "Firebase secret key failed (often disabled after a leak). Remove "
                    "FIREBASE_CREDENTIALS_JSON from Cloud Run and grant the Cloud Run "
                    "service account Cloud Datastore User on whatsapp-approval-system."
                )
            return f"Invalid FIREBASE_CREDENTIALS_JSON: {e}"

    path = _firebase_cred_path()
    if path and os.path.isfile(path):
        try:
            cred = credentials.Certificate(path)
            firebase_admin.initialize_app(cred, init_options)
            return None
        except Exception as e:
            app.logger.exception("Firebase credentials file failed: %s", path)
            return str(e)

    try:
        _try_adc()
        return None
    except Exception as e:
        app.logger.warning("Firebase Application Default Credentials failed: %s", e)

    return (
        "Firebase is not configured. On Cloud Run: grant the service account "
        "Cloud Datastore User on project whatsapp-approval-system and set "
        "FIREBASE_PROJECT_ID=whatsapp-approval-system (remove disabled JSON secrets)."
    )


def _ist_tzinfo():
    """Asia/Kolkata; fixed offset fallback if zoneinfo is unavailable."""
    if ZoneInfo:
        return ZoneInfo("Asia/Kolkata")
    return timezone(timedelta(hours=5, minutes=30))


def _ist_today_date():
    return datetime.now(_ist_tzinfo()).date()


APPROVER_STATUS_COLLECTION = "approver_status"
# Cap Firestore reads per security tab load (avoids full-collection scan → 429 quota).
_SECURITY_REQUESTS_QUERY_LIMIT = 400
_LEGACY_MD_BYPASS_WRITES_PER_LOAD = 25


def _firestore_user_message(exc: Exception) -> str:
    msg = str(exc)
    low = msg.lower()
    if "429" in msg or "quota" in low or "resource exhausted" in low:
        return (
            "Firestore read quota exceeded. Wait about one minute, then click Refresh once. "
            "Avoid rapid Refresh on OD Request / Visitor Request tabs."
        )
    return msg


def _security_requests_snapshots(db, req_type: str, *, limit: int | None = None):
    """Load recent requests of one type (not the entire collection)."""
    cap = limit or _SECURITY_REQUESTS_QUERY_LIMIT
    coll = db.collection("requests")
    try:
        q = (
            coll.where("type", "==", req_type)
            .order_by("requested_datetime", direction=firestore.Query.DESCENDING)
            .limit(cap)
        )
        return list(q.stream())
    except Exception as e:
        app.logger.warning("Firestore ordered query failed (%s): %s", req_type, e)
        return list(coll.where("type", "==", req_type).limit(cap).stream())


def _security_requests_by_type(db, req_type: str, *, limit: int | None = None):
    """Load requests of one type without order_by (no composite index required)."""
    cap = limit or _SECURITY_REQUESTS_QUERY_LIMIT
    return list(
        db.collection("requests").where("type", "==", req_type).limit(cap).stream()
    )


def _visitor_snapshots_for_ist_day(db, ist_day, *, limit: int = 200):
    """Visitor rows for one Coming On date (IST)."""
    date_str = ist_day.strftime("%d-%m-%Y")
    coll = db.collection("requests")
    try:
        q = (
            coll.where("type", "==", "VISITOR")
            .where("coming_on_date", "==", date_str)
            .limit(limit)
        )
        return list(q.stream())
    except Exception as e:
        app.logger.warning(
            "Firestore visitor date query failed, using type filter: %s", e
        )
    snaps = _security_requests_snapshots(db, "VISITOR", limit=limit * 2)
    out = []
    for snap in snaps:
        d = snap.to_dict() or {}
        visit_day = _visitor_coming_on_date(d)
        if visit_day is not None and visit_day == ist_day:
            out.append(snap)
        if len(out) >= limit:
            break
    return out


def _wa_id_from_env(*env_keys: str, default_mobile: str = "") -> str:
    """Resolve whatsapp:+91… from env (same names as Interakt bot)."""
    for key in env_keys:
        raw = (os.environ.get(key) or "").strip().strip('"').strip("'")
        if not raw:
            continue
        if raw.lower().startswith("whatsapp:"):
            return raw.lower()
        digits = "".join(c for c in raw if c.isdigit())
        if len(digits) == 10:
            return f"whatsapp:+91{digits}"
        if len(digits) >= 12 and digits.startswith("91"):
            return f"whatsapp:+{digits[-12:]}" if len(digits) > 12 else f"whatsapp:+{digits}"
    digits = "".join(c for c in (default_mobile or "") if c.isdigit())
    if len(digits) == 10:
        return f"whatsapp:+91{digits}"
    return ""


def _md_whatsapp_for_security(db) -> str:
    return _wa_id_from_env("MD_WHATSAPP_NUMBER")


def _approver_is_offline(db, wa_id: str) -> bool:
    """True only when approver_status explicitly set to offline (missing doc = online)."""
    if not db or not wa_id:
        return False
    snap = db.collection(APPROVER_STATUS_COLLECTION).document(
        wa_id.strip().lower()
    ).get()
    if not snap.exists:
        return False
    return (snap.to_dict() or {}).get("availability", "").strip().lower() == "offline"


def _format_firestore_datetime_ist(val):
    dtu = _firestore_value_to_utc_datetime(val)
    if dtu is None:
        return ""
    try:
        return dtu.astimezone(_ist_tzinfo()).strftime("%d-%m-%Y %I:%M %p")
    except Exception:
        return ""


def _fetch_security_approver_status(db):
    """JMD/MD availability from Firestore ``approver_status`` (default online)."""
    cached = _cache_get("security_approver_status")
    if cached is not None:
        return cached
    approvers = [
        ("JMD I", _wa_id_from_env("JMD_I_WHATSAPP_NUMBER", "JMD_WHATSAPP_NUMBER")),
        ("JMD II", _wa_id_from_env("JMD_II_WHATSAPP_NUMBER")),
        ("MD", _wa_id_from_env("MD_WHATSAPP_NUMBER")),
    ]
    rows = []
    for role, wa_id in approvers:
        if not wa_id:
            rows.append({
                "role": role,
                "wa_id": "Not configured",
                "availability": "Not configured",
                "status_cell_class": "security-appr-pending",
                "updated_at": "",
                "saved_role": "",
            })
            continue
        snap = db.collection(APPROVER_STATUS_COLLECTION).document(wa_id.lower()).get()
        data = snap.to_dict() if snap.exists else {}
        availability = (data.get("availability") or "online").strip().lower()
        if availability != "offline":
            availability = "online"
        rows.append({
            "role": role,
            "wa_id": wa_id,
            "availability": availability.title(),
            "status_cell_class": "security-appr-na"
            if availability == "online"
            else "security-appr-offline",
            "updated_at": _format_firestore_datetime_ist(data.get("updated_at")),
            "saved_role": (data.get("role") or "").strip(),
        })
    _cache_set("security_approver_status", rows)
    return rows


def _jmd_approved_for_od(d: dict) -> bool:
    jmd_u = (d.get("jmd_status") or "").strip().upper()
    return jmd_u == "APPROVED"


def _md_offline_bypass_on_request(d: dict) -> bool:
    """Persisted when JMD approved while MD was offline — stays even when MD is back online."""
    if d.get("md_offline_bypass"):
        return True
    return (d.get("md_status") or "").strip().upper() == "OFFLINE"


def _legacy_md_offline_bypass_candidate(d: dict) -> bool:
    """Older requests: JMD done, MD still PENDING, but already treated as approved offline."""
    if _md_offline_bypass_on_request(d):
        return False
    md_u = (d.get("md_status") or "").strip().upper()
    if md_u != "PENDING" or not _jmd_approved_for_od(d):
        return False
    req_type = (d.get("type") or "").strip().upper()
    if req_type == "VISITOR":
        return bool(_normalize_visitor_otp(d.get("visitor_otp")))
    if req_type == "OD":
        return bool(d.get("security_out_at") or d.get("odo_out") is not None)
    return False


def _maybe_persist_legacy_md_offline_bypass(
    ref, d: dict, *, writes_left: list[int] | None = None
) -> dict:
    if not _legacy_md_offline_bypass_candidate(d):
        return d
    if writes_left is not None:
        if writes_left[0] <= 0:
            return d
        writes_left[0] -= 1
    ref.update({"md_status": "OFFLINE", "md_offline_bypass": True})
    snap = ref.get()
    return snap.to_dict() if snap.exists else d


def _md_step_satisfied_for_security(
    d: dict, *, md_offline_live: bool, for_visitor: bool
) -> bool:
    md_u = (d.get("md_status") or "").strip().upper()
    if md_u == "DENIED":
        return False
    if _md_offline_bypass_on_request(d) or md_u == "APPROVED":
        return True
    jmd_ok = _jmd_approved_for_visitor(d) if for_visitor else _jmd_approved_for_od(d)
    if not jmd_ok:
        return False
    if md_offline_live or _legacy_md_offline_bypass_candidate(d):
        return True
    return False


def _od_security_fully_approved(d: dict, md_offline: bool) -> bool:
    """Security OUT/IN: MD approved/offline-bypass and JMD approved."""
    if (d.get("jmd_status") or "").strip().upper() == "DENIED":
        return False
    if not _jmd_approved_for_od(d):
        return False
    return _md_step_satisfied_for_security(
        d, md_offline_live=md_offline, for_visitor=False
    )


def _firestore_value_to_utc_datetime(val):
    """Normalize Firestore / datetime values to timezone-aware UTC."""
    if val is None:
        return None
    try:
        if hasattr(val, "timestamp") and callable(val.timestamp):
            return datetime.fromtimestamp(val.timestamp(), tz=timezone.utc)
        if isinstance(val, datetime):
            if val.tzinfo is None:
                return val.replace(tzinfo=timezone.utc)
            return val.astimezone(timezone.utc)
    except Exception:
        pass
    return None


def _requested_datetime_ist_date(val):
    """Calendar date in IST for the request's ``requested_datetime`` (for table filtering)."""
    dtu = _firestore_value_to_utc_datetime(val)
    if dtu is None:
        return None
    try:
        return dtu.astimezone(_ist_tzinfo()).date()
    except Exception:
        return None


def _parse_security_table_date(date_str, default_day):
    if date_str:
        try:
            return datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
    return default_day


def _visitor_coming_on_date(d: dict):
    """Visit date from Firestore (DD-MM-YYYY as stored by the WhatsApp bot)."""
    raw = (d.get("coming_on_date") or d.get("visit_date") or "").strip()
    if not raw:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw[:10], fmt).date()
        except ValueError:
            continue
    return None


def _visitor_coming_on_label(d: dict) -> str:
    return (d.get("coming_on_date") or d.get("visit_date") or "").strip() or "—"


def _parse_security_unit_filter(unit: str) -> str | None:
    """Map UI unit filter to Firestore ``jmd_route``: JMD1 = Unit I, JMD2 = Unit II."""
    key = (unit or "").strip().lower().replace(" ", "").replace("-", "").replace("_", "")
    if key in ("i", "uniti", "jmd1", "jmdi"):
        return "JMD1"
    if key in ("ii", "unitii", "jmd2", "jmdii"):
        return "JMD2"
    return None


def _security_unit_for_session(unit_arg: str | None) -> tuple[str, bool]:
    """
    Unit filter for Security page: unit-i | unit-ii, and whether the user may change it.
    security.1@alubee.com → Unit I (locked); security.2@alubee.com → Unit II (locked).
    """
    email = (getattr(current_user, "email", None) or "").strip().lower()
    if email == SECURITY_UNIT_I_EMAIL:
        return "unit-i", True
    if email == SECURITY_UNIT_II_EMAIL:
        return "unit-ii", True
    unit = (unit_arg or "unit-i").strip().lower()
    if unit not in ("unit-i", "unit-ii"):
        unit = "unit-i"
    return unit, False


def _request_jmd_route(d: dict) -> str:
    route = (d.get("jmd_route") or "").strip().upper()
    if route in ("JMD1", "JMD2"):
        return route
    return "JMD1"


def _vehicle_from_unit_jmd_route(d: dict) -> str:
    """Vehicle Request From field → Unit I (JMD1) or Unit II (JMD2)."""
    unit = (d.get("from_unit") or "").strip().lower().replace(" ", "_").replace("-", "_")
    if unit in ("unit_ii", "unit2", "unit_2", "ii"):
        return "JMD2"
    if unit in ("unit_i", "unit1", "unit_1", "i"):
        return "JMD1"
    label = (d.get("from_unit_label") or "").strip().upper()
    if label in ("UNIT II", "UNIT 2"):
        return "JMD2"
    if label in ("UNIT I", "UNIT 1"):
        return "JMD1"
    return "JMD1"


def _vehicle_matches_unit_filter(d: dict, jmd_route_filter: str | None) -> bool:
    """Vehicle Request tab only — filter by form ``from_unit``, not employee ``jmd_route``."""
    if not jmd_route_filter:
        return True
    return _vehicle_from_unit_jmd_route(d) == jmd_route_filter


def _visitor_matches_unit_filter(d: dict, jmd_route_filter: str | None) -> bool:
    """Unit tab filter: destination unit (and BOTH shows on both tabs)."""
    if not jmd_route_filter:
        return True
    vt = (d.get("visiting_to") or "").strip().upper()
    if vt == "BOTH":
        return True
    if vt == "UNIT_I":
        return jmd_route_filter == "JMD1"
    if vt == "UNIT_II":
        return jmd_route_filter == "JMD2"
    return _request_jmd_route(d) == jmd_route_filter


def _format_approval_label(status: str) -> str:
    """Dashboard approval column — avoid sentence_case breaking Roman numerals."""
    s = (status or "").strip().upper()
    if s == "APPROVED":
        return "Approved"
    if s == "DENIED":
        return "Denied"
    if s == "CANCELLED":
        return "Cancelled"
    if s == "OFFLINE":
        return "Offline"
    if s in ("PENDING", "AWAITING_JMD", "AWAITING_MANAGER"):
        return "Pending"
    if s in ("N/A", "NA", ""):
        return "N/A"
    return (status or "").strip() or "Pending"


def _aggregate_dual_jmd_status(d: dict) -> str:
    """Single JMD label for Both-units requests (both JMDs must approve)."""
    i = (d.get("jmd_i_status") or "PENDING").strip().upper()
    ii = (d.get("jmd_ii_status") or "PENDING").strip().upper()
    if "DENIED" in (i, ii):
        return "DENIED"
    if i == "APPROVED" and ii == "APPROVED":
        return "APPROVED"
    jmd = (d.get("jmd_status") or "").strip().upper()
    if jmd == "APPROVED":
        return "APPROVED"
    if jmd == "DENIED":
        return "DENIED"
    return "PENDING"


def _visitor_visiting_to_label(d: dict) -> str:
    label = (d.get("visiting_to_label") or "").strip()
    if label:
        return label
    vt = (d.get("visiting_to") or "").strip().upper()
    return {
        "UNIT_I": "Unit I",
        "UNIT_II": "Unit II",
        "BOTH": "Both",
    }.get(vt, vt or "—")


def _visitor_security_unit_key(d: dict, tab_unit: str) -> str:
    """Firestore suffix: unit_i | unit_ii for per-unit gate times (Both visits)."""
    vt = (d.get("visiting_to") or "").strip().upper()
    tab = (tab_unit or "unit-i").strip().lower()
    if vt == "BOTH" or d.get("visitor_dual_jmd"):
        return "unit_i" if tab == "unit-i" else "unit_ii"
    if vt == "UNIT_II":
        return "unit_ii"
    return "unit_i"


def _visitor_uses_per_unit_gate(d: dict) -> bool:
    vt = (d.get("visiting_to") or "").strip().upper()
    return vt == "BOTH" or bool(d.get("visitor_dual_jmd"))


def _visitor_gate_timestamps(d: dict, tab_unit: str) -> tuple:
    """Return raw Firestore in/out timestamps for the active security tab unit."""
    uk = _visitor_security_unit_key(d, tab_unit)
    if _visitor_uses_per_unit_gate(d):
        in_at = d.get(f"security_in_at_{uk}")
        out_at = d.get(f"security_out_at_{uk}")
        if in_at is None and uk == "unit_i" and d.get("security_in_at") is not None:
            in_at = d.get("security_in_at")
        if out_at is None and uk == "unit_i" and d.get("security_out_at") is not None:
            out_at = d.get("security_out_at")
        return in_at, out_at
    in_at = d.get("security_in_at")
    out_at = d.get("security_out_at")
    if in_at is None:
        in_at = d.get(f"security_in_at_{uk}")
    if out_at is None:
        out_at = d.get(f"security_out_at_{uk}")
    return in_at, out_at


def _visitor_gate_field_names(d: dict, security_unit: str) -> tuple[str, str]:
    """Firestore field names for IN/OUT on this security tab."""
    uk = _visitor_security_unit_key(d, security_unit)
    if _visitor_uses_per_unit_gate(d):
        return f"security_in_at_{uk}", f"security_out_at_{uk}"
    return "security_in_at", "security_out_at"


def _format_firestore_date_ist(val):
    """Date only in IST: DD-MM-YYYY."""
    if val is None:
        return ""
    try:
        dtu = _firestore_value_to_utc_datetime(val)
        if dtu is None:
            return str(val)
        return dtu.astimezone(_ist_tzinfo()).strftime("%d-%m-%Y")
    except Exception:
        return str(val)


def _format_firestore_time_ist_12h(val):
    """Time only in IST, 12-hour with AM/PM (e.g. 04:40 PM)."""
    if val is None:
        return ""
    try:
        dtu = _firestore_value_to_utc_datetime(val)
        if dtu is None:
            return str(val)
        return dtu.astimezone(_ist_tzinfo()).strftime("%I:%M %p")
    except Exception:
        return str(val)


def _firestore_ts_to_sort_key(val):
    if val is None:
        return 0.0
    try:
        if hasattr(val, "timestamp") and callable(val.timestamp):
            return float(val.timestamp())
        if isinstance(val, datetime):
            return val.timestamp()
    except Exception:
        pass
    return 0.0


def _get_firestore_client():
    err = _init_firebase_app()
    if err:
        return None, err
    try:
        return firestore.client(), None
    except Exception as e:
        app.logger.exception("Firestore client failed")
        return None, str(e)


def _od_approval_statuses_for_display(
    d: dict, *, md_offline: bool = False
) -> tuple[str, str]:
    """JMD/MD labels for security dashboard (fixes legacy MD-deny overwriting jmd_status)."""
    if d.get("visitor_dual_jmd"):
        jmd_raw = _aggregate_dual_jmd_status(d)
        md_raw = (d.get("md_status") or "").strip()
        jmd_display = _format_approval_label(jmd_raw)
        md_display = _format_approval_label(md_raw)
    else:
        jmd_raw = (d.get("jmd_status") or "").strip()
        md_raw = (d.get("md_status") or "").strip()
        jmd_u = jmd_raw.upper()
        md_u = md_raw.upper()
        # MD can only deny after JMD approved; old bot wrote jmd_status=DENIED on MD deny
        if md_u == "DENIED" and jmd_u == "DENIED":
            jmd_display = _format_approval_label("APPROVED")
            md_display = _format_approval_label(md_raw or "DENIED")
        else:
            jmd_display = _format_approval_label(jmd_raw)
            md_display = _format_approval_label(md_raw)
    if _md_offline_bypass_on_request(d) or _legacy_md_offline_bypass_candidate(d):
        md_display = "Offline"
    elif md_offline and md_display not in ("Approved", "Denied"):
        md_display = "Offline"
    return jmd_display, md_display


def _fetch_security_od_requests_inner(
    ist_day,
    db,
    *,
    jmd_route_filter: str | None = None,
    company_vehicle_only: bool = False,
):
    buf = []
    legacy_writes = [_LEGACY_MD_BYPASS_WRITES_PER_LOAD]
    for snap in _security_requests_snapshots(db, "OD"):
        d = snap.to_dict() or {}
        ts = d.get("requested_datetime")
        if ist_day is not None:
            if _requested_datetime_ist_date(ts) != ist_day:
                continue
        if jmd_route_filter and _request_jmd_route(d) != jmd_route_filter:
            continue
        if company_vehicle_only and not _request_uses_company_vehicle(d):
            continue
        buf.append((_firestore_ts_to_sort_key(ts), d, snap.id))
    buf.sort(key=lambda x: x[0], reverse=True)
    buf = buf[:200]
    md_wa = _md_whatsapp_for_security(db)
    md_offline = _approver_is_offline(db, md_wa)
    rows = []
    for _, d, snap_id in buf:
        ref = db.collection("requests").document(snap_id)
        d = _maybe_persist_legacy_md_offline_bypass(
            ref, d, writes_left=legacy_writes
        )
        fully_ok = _od_security_fully_approved(d, md_offline)
        distance_km = d.get("distance_km")
        if distance_km is None and d.get("odo_out") is not None and d.get("odo_in") is not None:
            try:
                distance_km = round(float(d["odo_in"]) - float(d["odo_out"]), 2)
            except (TypeError, ValueError):
                distance_km = None
        jmd_display, md_display = _od_approval_statuses_for_display(
            d, md_offline=md_offline
        )
        rows.append(
            {
                "request_id": d.get("request_id") or snap_id,
                "requested_datetime": _format_firestore_date_ist(
                    d.get("requested_datetime")
                ),
                "employee_id": d.get("employee_id") or "",
                "employee_name": d.get("employee_name") or "",
                "department": d.get("department") or "",
                "reason": d.get("reason") or "",
                "company_vehicle": d.get("company_vehicle_description") or "",
                "uses_company_vehicle": _request_uses_company_vehicle(d),
                "manager": d.get("manager") or "",
                "manager_status": d.get("manager_status") or "",
                "jmd_status": jmd_display,
                "md_status": md_display,
                "jmd_route": _request_jmd_route(d),
                "fully_approved": fully_ok,
                "security_out_at": _format_firestore_time_ist_12h(
                    d.get("security_out_at")
                ),
                "security_in_at": _format_firestore_time_ist_12h(
                    d.get("security_in_at")
                ),
                "odo_out": _format_odo_reading(d.get("odo_out")),
                "odo_in": _format_odo_reading(d.get("odo_in")),
                "distance_km": _format_distance_km(distance_km),
            }
        )
    return rows


def _format_odo_reading(value):
    """Display odometer reading from Firestore (number or empty)."""
    if value is None or value == "":
        return ""
    try:
        n = float(value)
    except (TypeError, ValueError):
        return ""
    if n == int(n):
        return str(int(n))
    return f"{n:.1f}"


def _format_distance_km(value):
    if value is None or value == "":
        return ""
    try:
        n = float(value)
    except (TypeError, ValueError):
        return ""
    if n == int(n):
        return str(int(n))
    return f"{n:.2f}"


def _request_uses_company_vehicle(d: dict) -> bool:
    """Whether this OD used a company vehicle (ODO required at gate)."""
    if not d:
        return False
    flag = d.get("uses_company_vehicle")
    if flag is True:
        return True
    if flag is False:
        return False
    return bool((d.get("company_vehicle_id") or "").strip())


def _parse_odo_reading(value):
    """Validate ODO from API. Returns (float|None, error_message)."""
    if value is None or (isinstance(value, str) and not str(value).strip()):
        return None, "ODO meter reading is required"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None, "Invalid ODO meter reading"
    if n < 0:
        return None, "ODO meter reading cannot be negative"
    return n, None


def fetch_security_od_requests(
    ist_day=None,
    *,
    jmd_route_filter: str | None = None,
    company_vehicle_only: bool = False,
):
    """Load OD requests for one IST calendar day (``requested_datetime``), newest first, cap 200."""
    db, err = _get_firestore_client()
    if err:
        return [], err
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _fetch_security_od_requests_inner,
                ist_day,
                db,
                jmd_route_filter=jmd_route_filter,
                company_vehicle_only=company_vehicle_only,
            )
            return future.result(timeout=25), None
    except TimeoutError:
        app.logger.error("Firestore security OD fetch timed out")
        return [], (
            "Firestore request timed out. Check that the Cloud Run service account has "
            "Cloud Datastore User on whatsapp-approval-system."
        )
    except Exception as e:
        app.logger.exception("Firestore security OD fetch failed")
        msg = _firestore_user_message(e)
        if any(
            x in msg.lower()
            for x in ("403", "permission", "disabled", "invalid_grant", "invalid jwt")
        ):
            msg += (
                " Remove FIREBASE_CREDENTIALS_JSON / GOOGLE_APPLICATION_CREDENTIALS from "
                "Cloud Run and grant the runtime service account Cloud Datastore User on "
                "whatsapp-approval-system."
            )
        return [], msg


def _security_record_od_gate(request_id: str, action: str, odo_reading):
    """Persist OUT / IN timestamps and ODO on the OD request. Returns (ok, error_message)."""
    db, err = _get_firestore_client()
    if err:
        return False, err
    action = (action or "").strip().lower()
    if action not in ("out", "in"):
        return False, "Invalid action"
    rid = (request_id or "").strip()
    if not rid:
        return False, "Missing request id"
    try:
        ref = db.collection("requests").document(rid)
        snap = ref.get()
        if not snap.exists:
            return False, "Request not found"
        d = snap.to_dict() or {}
        if (d.get("type") or "").strip().upper() != "OD":
            return False, "Not an OD request"
        md_wa = _md_whatsapp_for_security(db)
        md_offline = _approver_is_offline(db, md_wa)
        if not _od_security_fully_approved(d, md_offline):
            return False, "OD is not fully approved yet (JMD/MD approval pending)"
        needs_odo = _request_uses_company_vehicle(d)
        odo = None
        if needs_odo:
            odo, odo_err = _parse_odo_reading(odo_reading)
            if odo_err:
                return False, odo_err
        out_at = d.get("security_out_at")
        in_at = d.get("security_in_at")
        now = datetime.now(timezone.utc)
        if action == "out":
            if out_at is not None:
                return False, "Out time is already recorded"
            update = {"security_out_at": now}
            if needs_odo:
                update["odo_out"] = odo
            ref.update(update)
            return True, None
        if in_at is not None:
            return False, "This visit is already closed"
        if out_at is None:
            return False, "Record OUT before IN"
        update = {"security_in_at": now}
        if needs_odo:
            odo_out = d.get("odo_out")
            if odo_out is not None:
                try:
                    odo_out_f = float(odo_out)
                except (TypeError, ValueError):
                    odo_out_f = None
                if odo_out_f is not None and odo < odo_out_f:
                    return False, "IN reading cannot be less than OUT reading"
            update["odo_in"] = odo
            if odo_out is not None:
                try:
                    update["distance_km"] = round(odo - float(odo_out), 2)
                except (TypeError, ValueError):
                    pass
        ref.update(update)
        return True, None
    except Exception as e:
        app.logger.exception("security OD gate update failed")
        return False, str(e)


def _jmd_approved_for_visitor(d: dict) -> bool:
    if d.get("visitor_dual_jmd"):
        return _aggregate_dual_jmd_status(d).strip().upper() == "APPROVED"
    return _jmd_approved_for_od(d)


def _visitor_security_fully_approved(d: dict, md_offline: bool) -> bool:
    """Security IN: MD step complete (incl. offline bypass) and entry OTP present."""
    if d.get("visitor_dual_jmd"):
        if _aggregate_dual_jmd_status(d).strip().upper() == "DENIED":
            return False
    elif (d.get("jmd_status") or "").strip().upper() == "DENIED":
        return False
    if (d.get("md_status") or "").strip().upper() == "DENIED":
        return False
    if not _jmd_approved_for_visitor(d):
        return False
    if not _md_step_satisfied_for_security(
        d, md_offline_live=md_offline, for_visitor=True
    ):
        return False
    return bool(_normalize_visitor_otp(d.get("visitor_otp")))


def _backfill_visitor_otp_if_md_offline(
    ref, d: dict, *, writes_left: list[int] | None = None
) -> dict:
    """Legacy rows only: OTP + OFFLINE md_status (do not write on every refresh)."""
    if not _legacy_md_offline_bypass_candidate(d):
        return d
    if writes_left is not None:
        if writes_left[0] <= 0:
            return d
        writes_left[0] -= 1
    patch = {}
    if not _normalize_visitor_otp(d.get("visitor_otp")):
        patch["visitor_otp"] = f"{secrets.randbelow(1_000_000):06d}"
        patch["guest_otp_sent"] = False
    if (d.get("md_status") or "").strip().upper() == "PENDING":
        patch["md_status"] = "OFFLINE"
        patch["md_offline_bypass"] = True
    if patch:
        ref.update(patch)
        snap = ref.get()
        return snap.to_dict() if snap.exists else d
    return d


def _normalize_visitor_otp(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if s.isdigit() and len(s) < 6:
        return s.zfill(6)
    return s


def _fetch_security_visitor_requests_inner(
    ist_day,
    db,
    *,
    jmd_route_filter: str | None = None,
    security_tab_unit: str = "unit-i",
):
    buf = []
    legacy_writes = [_LEGACY_MD_BYPASS_WRITES_PER_LOAD]
    snaps = (
        _visitor_snapshots_for_ist_day(db, ist_day)
        if ist_day is not None
        else _security_requests_snapshots(db, "VISITOR")
    )
    for snap in snaps:
        d = snap.to_dict() or {}
        visit_day = _visitor_coming_on_date(d)
        if ist_day is not None:
            if visit_day is None or visit_day != ist_day:
                continue
        if not _visitor_matches_unit_filter(d, jmd_route_filter):
            continue
        ts = d.get("requested_datetime")
        sort_day = visit_day or datetime.min.date()
        buf.append((sort_day, _firestore_ts_to_sort_key(ts), d, snap.id))
    buf.sort(key=lambda x: (x[0], x[1]), reverse=True)
    buf = buf[:200]
    md_wa = _md_whatsapp_for_security(db)
    md_offline = _approver_is_offline(db, md_wa) if md_wa else False
    rows = []
    for _, _, d, snap_id in buf:
        ref = db.collection("requests").document(snap_id)
        d = _backfill_visitor_otp_if_md_offline(ref, d, writes_left=legacy_writes)
        d = _maybe_persist_legacy_md_offline_bypass(
            ref, d, writes_left=legacy_writes
        )
        names = d.get("visitor_names") or []
        if isinstance(names, str):
            names = [n.strip() for n in names.split(",") if n.strip()]
        jmd_display, md_display = _od_approval_statuses_for_display(
            d, md_offline=md_offline
        )
        in_raw, out_raw = _visitor_gate_timestamps(d, security_tab_unit)
        fully_ok = _visitor_security_fully_approved(d, md_offline)
        rows.append(
            {
                "request_id": d.get("request_id") or snap_id,
                "coming_on_date": _visitor_coming_on_label(d),
                "requested_datetime": _format_firestore_date_ist(
                    d.get("requested_datetime")
                ),
                "employee_id": d.get("employee_id") or "",
                "employee_name": d.get("employee_name") or "",
                "department": d.get("department") or "",
                "people_count": d.get("people_count") or "",
                "visitor_names": ", ".join(names) if names else "",
                "coming_from": (
                    d.get("coming_from")
                    or d.get("coming_from_label")
                    or d.get("organization")
                    or ""
                ),
                "coming_for": (
                    d.get("coming_for_label")
                    or d.get("visit_for_label")
                    or ""
                ),
                "visiting_to": _visitor_visiting_to_label(d),
                "guest_phone": d.get("guest_phone") or "",
                "jmd_status": jmd_display,
                "md_status": md_display,
                "jmd_route": _request_jmd_route(d),
                "fully_approved": fully_ok,
                "has_visitor_otp": bool(_normalize_visitor_otp(d.get("visitor_otp"))),
                "security_in_at": _format_firestore_time_ist_12h(in_raw),
                "security_out_at": _format_firestore_time_ist_12h(out_raw),
                "gate_per_unit": _visitor_uses_per_unit_gate(d),
            }
        )
    return rows


def _parse_leave_ddmmy(raw: str):
    s = (raw or "").strip()
    if not s:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def _leave_overlaps_ist_day(d: dict, ist_day) -> bool:
    """True if this LEAVE request covers the given IST calendar date."""
    if ist_day is None:
        return True
    target_ddmmy = ist_day.strftime("%d-%m-%Y")
    leave_dates = d.get("leave_dates") or []
    for x in leave_dates:
        if str(x).strip() == target_ddmmy:
            return True

    from_s = d.get("leave_from_date") or ""
    to_s = d.get("leave_to_date") or from_s
    from_d = _parse_leave_ddmmy(str(from_s))
    to_d = _parse_leave_ddmmy(str(to_s))
    if from_d and to_d:
        if to_d < from_d:
            from_d, to_d = to_d, from_d
        return from_d <= ist_day <= to_d
    return False


def _leave_approval_statuses_for_display(
    d: dict, *, md_offline: bool = False
) -> tuple[str, str]:
    """JMD + MD columns for Security leave tab."""
    if d.get("cancelled_by_employee"):
        return "Cancelled", "N/A"
    return _od_approval_statuses_for_display(d, md_offline=md_offline)


def _leave_jmd_display_label(d: dict) -> str:
    """First approval column (legacy helper)."""
    return _leave_approval_statuses_for_display(d)[0]


def _permission_approval_statuses_for_display(
    d: dict, *, md_offline: bool = False
) -> tuple[str, str]:
    """Two-step approval columns for Security permission tab."""
    if d.get("cancelled_by_employee"):
        return "Cancelled", "N/A"
    return _od_approval_statuses_for_display(d, md_offline=md_offline)


def _leave_snapshots_for_ist_day(db, ist_day, *, limit: int = 200):
    """LEAVE rows for one IST calendar day (leave_dates array_contains)."""
    if ist_day is None:
        return _security_requests_by_type(db, "LEAVE", limit=limit)
    date_str = ist_day.strftime("%d-%m-%Y")
    coll = db.collection("requests")
    try:
        q = (
            coll.where("type", "==", "LEAVE")
            .where("leave_dates", "array_contains", date_str)
            .limit(limit)
        )
        return list(q.stream())
    except Exception as e:
        app.logger.warning(
            "Firestore leave date query failed, using type filter: %s", e
        )
    snaps = _security_requests_by_type(db, "LEAVE", limit=limit * 2)
    out = []
    for snap in snaps:
        d = snap.to_dict() or {}
        if _leave_overlaps_ist_day(d, ist_day):
            out.append(snap)
        if len(out) >= limit:
            break
    return out


def _fetch_security_leave_requests_inner(
    ist_day,
    db,
    *,
    jmd_route_filter: str | None = None,
):
    buf = []
    for snap in _leave_snapshots_for_ist_day(db, ist_day, limit=400):
        d = snap.to_dict() or {}
        if jmd_route_filter and _request_jmd_route(d) != jmd_route_filter:
            continue
        ts = d.get("requested_datetime")
        buf.append((_firestore_ts_to_sort_key(ts), d, snap.id))

    buf.sort(key=lambda x: x[0], reverse=True)
    buf = buf[:200]

    md_wa = _md_whatsapp_for_security(db)
    md_offline = _approver_is_offline(db, md_wa)
    rows = []
    for _, d, snap_id in buf:
        leave_days = d.get("leave_days")
        if leave_days is None:
            leave_days = len(d.get("leave_dates") or [])
        leave_duration = (d.get("leave_duration") or "").strip().lower()
        if leave_duration == "half_day" or leave_days == 0.5:
            leave_days_display = "0.5"
        elif leave_days is not None and float(leave_days) == int(float(leave_days)):
            leave_days_display = int(float(leave_days))
        else:
            leave_days_display = leave_days if leave_days is not None else ""

        jmd_display, md_display = _leave_approval_statuses_for_display(
            d, md_offline=md_offline
        )
        rows.append(
            {
                "request_id": d.get("request_id") or snap_id,
                "requested_datetime": _format_firestore_date_ist(
                    d.get("requested_datetime")
                ),
                "employee_id": d.get("employee_id") or "",
                "employee_name": d.get("employee_name") or "",
                "department": d.get("department") or "",
                "reason": d.get("reason") or "",
                "leave_from_date": d.get("leave_from_date") or "",
                "leave_to_date": d.get("leave_to_date") or "",
                "leave_days": leave_days_display,
                "jmd_status": jmd_display,
                "md_status": md_display,
            }
        )
    return rows


def fetch_security_leave_requests(
    ist_day=None,
    *,
    jmd_route_filter: str | None = None,
):
    """Load LEAVE requests overlapping one IST calendar day, newest first, cap 200."""
    db, err = _get_firestore_client()
    if err:
        return [], err
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _fetch_security_leave_requests_inner,
                ist_day,
                db,
                jmd_route_filter=jmd_route_filter,
            )
            return future.result(timeout=25), None
    except TimeoutError:
        app.logger.error("Firestore security leave fetch timed out")
        return [], (
            "Firestore request timed out. Check that the Cloud Run service account has "
            "Cloud Datastore User on whatsapp-approval-system."
        )
    except Exception as e:
        app.logger.exception("Firestore security leave fetch failed")
        return [], _firestore_user_message(e)


def _it_status_label(raw: str) -> str:
    labels = {
        "QUEUED": "Queued",
        "ASSIGNED": "Assigned",
        "CLOSED": "Closed",
        "CANCELLED": "Cancelled",
    }
    key = (raw or "").strip().upper()
    return labels.get(key, (raw or "—").strip() or "—")


def _fetch_security_it_requests_inner(
    ist_day,
    db,
    *,
    jmd_route_filter: str | None = None,
):
    buf = []
    for snap in _security_requests_snapshots(db, "IT"):
        d = snap.to_dict() or {}
        ts = d.get("requested_datetime")
        if ist_day is not None:
            if _requested_datetime_ist_date(ts) != ist_day:
                continue
        if jmd_route_filter and _request_jmd_route(d) != jmd_route_filter:
            continue
        buf.append((_firestore_ts_to_sort_key(ts), d, snap.id))

    buf.sort(key=lambda x: x[0], reverse=True)
    buf = buf[:200]

    rows = []
    for _, d, snap_id in buf:
        desc = (d.get("description") or "").strip()
        rows.append(
            {
                "request_id": d.get("request_id") or snap_id,
                "requested_datetime": _format_firestore_date_ist(
                    d.get("requested_datetime")
                ),
                "requested_time": _format_firestore_time_ist_12h(
                    d.get("requested_datetime")
                ),
                "employee_id": d.get("employee_id") or "",
                "employee_name": d.get("employee_name") or "",
                "department": d.get("department") or "",
                "it_category_label": d.get("it_category_label")
                or d.get("it_category")
                or "",
                "issue_type_label": d.get("issue_type_label")
                or d.get("issue_type")
                or "",
                "description": desc or "—",
                "issue_photo_url": (d.get("issue_photo_url") or "").strip(),
                "priority_label": d.get("priority_label") or d.get("priority") or "",
                "it_status": _it_status_label(d.get("it_status")),
                "it_status_raw": (d.get("it_status") or "").strip().upper(),
                "assigned_engineer_name": d.get("assigned_engineer_name") or "—",
                "assigned_datetime": _format_firestore_time_ist_12h(
                    d.get("assigned_datetime")
                )
                or "—",
                "closed_datetime": _format_firestore_time_ist_12h(
                    d.get("closed_datetime")
                )
                or "—",
            }
        )
    return rows


def fetch_security_it_requests(
    ist_day=None,
    *,
    jmd_route_filter: str | None = None,
):
    """Load IT requests for one IST calendar day (requested_datetime), newest first."""
    db, err = _get_firestore_client()
    if err:
        return [], err
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _fetch_security_it_requests_inner,
                ist_day,
                db,
                jmd_route_filter=jmd_route_filter,
            )
            return future.result(timeout=25), None
    except TimeoutError:
        app.logger.error("Firestore security IT fetch timed out")
        return [], (
            "Firestore request timed out. Check that the Cloud Run service account has "
            "Cloud Datastore User on whatsapp-approval-system."
        )
    except Exception as e:
        app.logger.exception("Firestore security IT fetch failed")
        return [], _firestore_user_message(e)


def _vehicle_status_label(raw: str) -> str:
    labels = {
        "PENDING": "Pending",
        "ASSIGNED": "Assigned",
        "STARTED": "Started",
        "IN_PROGRESS": "In Progress",
        "COMPLETED": "Completed",
        "CANCELLED": "Cancelled",
    }
    key = (raw or "").strip().upper()
    return labels.get(key, (raw or "—").strip() or "—")


def _fetch_security_vehicle_requests_inner(
    ist_day,
    db,
    *,
    jmd_route_filter: str | None = None,
):
    buf = []
    for snap in _security_requests_snapshots(db, "VEHICLE_REQUEST"):
        d = snap.to_dict() or {}
        ts = d.get("requested_datetime")
        if ist_day is not None:
            if _requested_datetime_ist_date(ts) != ist_day:
                continue
        if jmd_route_filter and not _vehicle_matches_unit_filter(d, jmd_route_filter):
            continue
        status = (
            d.get("vehicle_request_status") or d.get("logistics_status") or ""
        ).strip().upper()
        if status == "CANCELLED":
            continue
        buf.append((_firestore_ts_to_sort_key(ts), d, snap.id))

    buf.sort(key=lambda x: x[0], reverse=True)
    buf = buf[:200]

    rows = []
    for _, d, snap_id in buf:
        status_raw = (
            d.get("vehicle_request_status") or d.get("logistics_status") or ""
        ).strip().upper()
        active = bool(d.get("is_active_trip"))
        out_at = d.get("security_out_at")
        in_at = d.get("security_in_at")
        is_internal = _is_internal_vehicle_type(d.get("vehicle_type") or "")
        show_out = False
        show_in = False
        show_external_in = False
        show_external_out = False
        if is_internal:
            show_out = (
                status_raw == "STARTED"
                and active
                and out_at is None
            )
            show_in = active and out_at is not None and in_at is None
        elif status_raw in ("ASSIGNED", "STARTED"):
            show_external_in = out_at is None
            show_external_out = out_at is not None and in_at is None
        rows.append(
            {
                "request_id": d.get("request_id") or snap_id,
                "employee_name": d.get("employee_name") or "",
                "department": d.get("department") or "",
                "from_unit_label": d.get("from_unit_label") or "—",
                "destination_category_label": d.get("destination_category_label") or "",
                "destination_label": d.get("destination_label") or "",
                "assigned_to": d.get("assigned_to") or "—",
                "fleet_vehicle_label": d.get("fleet_vehicle_label") or "—",
                "required_at": d.get("required_at") or "—",
                "vehicle_status": _vehicle_status_label(status_raw),
                "vehicle_status_raw": status_raw,
                "security_out_at": _format_firestore_time_ist_12h(out_at),
                "security_in_at": _format_firestore_time_ist_12h(in_at),
                "is_external": not is_internal,
                "show_out": show_out,
                "show_in": show_in,
                "show_external_in": show_external_in,
                "show_external_out": show_external_out,
                "trip_closed": in_at is not None,
            }
        )
    return rows


def fetch_security_vehicle_requests(
    ist_day=None,
    *,
    jmd_route_filter: str | None = None,
):
    db, err = _get_firestore_client()
    if err:
        return [], err
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _fetch_security_vehicle_requests_inner,
                ist_day,
                db,
                jmd_route_filter=jmd_route_filter,
            )
            return future.result(timeout=25), None
    except TimeoutError:
        app.logger.error("Firestore security vehicle fetch timed out")
        return [], (
            "Firestore request timed out. Check that the Cloud Run service account has "
            "Cloud Datastore User on whatsapp-approval-system."
        )
    except Exception as e:
        app.logger.exception("Firestore security vehicle fetch failed")
        return [], _firestore_user_message(e)


def _security_record_vehicle_gate(
    request_id: str,
    action: str,
    *,
    vehicle_number: str | None = None,
):
    """Record OUT / IN for vehicle requests (internal vs external rules differ)."""
    db, err = _get_firestore_client()
    if err:
        return False, err
    action = (action or "").strip().lower()
    if action not in ("out", "in"):
        return False, "Invalid action"
    rid = (request_id or "").strip()
    if not rid:
        return False, "Missing request id"
    try:
        ref = db.collection("requests").document(rid)
        snap = ref.get()
        if not snap.exists:
            return False, "Request not found"
        d = snap.to_dict() or {}
        if (d.get("type") or "").strip().upper() not in ("VEHICLE_REQUEST", "LOGISTICS"):
            return False, "Not a vehicle request"
        status = (
            d.get("vehicle_request_status") or d.get("logistics_status") or ""
        ).strip().upper()
        out_at = d.get("security_out_at")
        in_at = d.get("security_in_at")
        now = datetime.now(timezone.utc)
        is_internal = _is_internal_vehicle_type(d.get("vehicle_type") or "")

        if is_internal:
            if status != "STARTED" or not d.get("is_active_trip"):
                return False, "Trip has not been started by assignee"
            if action == "out":
                if out_at is not None:
                    return False, "Out time is already recorded"
                ref.update({"security_out_at": now})
                return True, None
            if in_at is not None:
                return False, "This trip is already closed"
            if out_at is None:
                return False, "Record OUT before IN"
            ref.update({
                "security_in_at": now,
                "is_active_trip": False,
                "vehicle_request_status": "COMPLETED",
            })
            return True, None

        if status not in ("ASSIGNED", "STARTED"):
            return False, "Request is not ready for security gate"
        if action == "in":
            if out_at is not None:
                return False, "Vehicle OUT is already recorded"
            digits = "".join(c for c in str(vehicle_number or "").strip() if c.isdigit())
            if len(digits) != 4:
                return False, "Enter a valid 4-digit vehicle number"
            ref.update({
                "security_out_at": now,
                "external_vehicle_number": digits,
                "vehicle_request_status": "STARTED",
                "is_active_trip": True,
            })
            return True, None
        if out_at is None:
            return False, "Record vehicle IN (with number) before OUT"
        if in_at is not None:
            return False, "This trip is already closed"
        ref.update({
            "security_in_at": now,
            "is_active_trip": False,
            "vehicle_request_status": "COMPLETED",
        })
        return True, None
    except Exception as e:
        app.logger.exception("security vehicle gate update failed")
        return False, str(e)


LOGISTICS_EXTERNAL_VENDORS: tuple[tuple[str, str], ...] = (
    ("annai_transport", "Annai Transport"),
    ("challa_transport", "Challa Transport"),
    ("sridhar_transport", "Sridhar Transport"),
    ("chella_transport", "Chella Transport"),
)

LOGISTICS_INTERNAL_FLEET: tuple[tuple[str, str], ...] = (
    ("dost_3371", "Dost-3371"),
    ("dost_2568", "Dost-2568"),
    ("santro_2004", "Santro-2004"),
    ("santa_fe_1666", "Santa FE-1666"),
)


def _logistics_normalize_code(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def _logistics_department_name() -> str:
    return (
        os.getenv("VEHICLE_INTERNAL_ASSIGN_DEPARTMENT")
        or os.getenv("LOGISTICS_DEPARTMENT_NAME")
        or "LOGISTICS"
    ).strip().upper()


def _logistics_vehicle_status_raw(rd: dict) -> str:
    return (
        rd.get("vehicle_request_status") or rd.get("logistics_status") or "PENDING"
    ).strip().upper()


def _logistics_vehicle_trip_started(rd: dict) -> bool:
    return _logistics_vehicle_status_raw(rd) == "STARTED"


def _logistics_staff_options(db) -> list[dict]:
    dept = _logistics_department_name()
    rows: list[dict] = []
    try:
        snaps = db.collection("users").where("department", "==", dept).stream()
    except Exception:
        app.logger.exception("logistics staff query failed dept=%s", dept)
        return rows
    for snap in snaps:
        ud = snap.to_dict() or {}
        emp_id = (ud.get("employee_id") or "").strip()
        if not emp_id:
            continue
        rows.append({
            "code": _logistics_normalize_code(emp_id),
            "label": (ud.get("name") or emp_id).strip(),
            "wa_id": snap.id,
        })
    rows.sort(key=lambda item: item["label"].lower())
    return rows


def _logistics_assignee_options(db, vehicle_type: str) -> list[dict]:
    vtype = (vehicle_type or "").strip().lower()
    if vtype in ("in_house", "internal", "company_vehicle"):
        return _logistics_staff_options(db)
    return [{"code": code, "label": label, "wa_id": ""} for code, label in LOGISTICS_EXTERNAL_VENDORS]


def _logistics_staff_wa_for_code(db, assignee_code: str) -> str:
    code = _logistics_normalize_code(assignee_code)
    if not code:
        return ""
    for item in _logistics_staff_options(db):
        if item["code"] == code:
            return (item.get("wa_id") or "").strip()
    return ""


def _logistics_assignee_label(options: list[dict], code: str) -> str:
    norm = _logistics_normalize_code(code)
    for item in options:
        if item["code"] == norm:
            return item["label"]
    return ""


def _logistics_fleet_vehicle_label(code: str) -> str:
    norm = _logistics_normalize_code(code)
    for item_code, label in LOGISTICS_INTERNAL_FLEET:
        if item_code == norm:
            return label
    return ""


def _is_internal_vehicle_type(raw: str) -> bool:
    return _logistics_normalize_vehicle_type(raw) == "in_house"


def _user_can_access_logistics() -> bool:
    if not current_user.is_authenticated:
        return False
    role = (getattr(current_user, "role", "") or "").strip().lower()
    if role in ("admin", "editor"):
        return True
    pages = getattr(current_user, "allowed_pages", None) or []
    return "logistics" in pages


def _logistics_vehicle_row(d, snap_id):
    status_raw = _logistics_vehicle_status_raw(d)
    trip_started = _logistics_vehicle_trip_started(d)
    return {
        "request_id": d.get("request_id") or snap_id,
        "requested_date": _format_firestore_date_ist(d.get("requested_datetime")),
        "requested_time": _format_firestore_time_ist_12h(d.get("requested_datetime")),
        "employee_id": d.get("employee_id") or "",
        "employee_name": d.get("employee_name") or "",
        "department": d.get("department") or "",
        "from_unit_label": d.get("from_unit_label") or "—",
        "request_type_label": d.get("request_type_label") or "",
        "destination_category_label": d.get("destination_category_label") or "",
        "destination_label": d.get("destination_label") or "",
        "location_details": d.get("location_details") or "",
        "vehicle_type_label": d.get("vehicle_type_label") or "—",
        "fleet_vehicle_label": d.get("fleet_vehicle_label") or "—",
        "hire_vehicle_type_label": d.get("hire_vehicle_type_label") or "—",
        "load_size_label": d.get("load_size_label") or "",
        "estimated_distance_display": d.get("estimated_distance_display") or "—",
        "required_at": d.get("required_at") or "—",
        "required_time": d.get("required_time") or "",
        "assigned_to": d.get("assigned_to") or "—",
        "assigned_to_code": d.get("assigned_to_code") or "",
        "vehicle_status": _vehicle_status_label(status_raw),
        "vehicle_status_raw": status_raw,
        "security_out_at": _format_firestore_time_ist_12h(d.get("security_out_at")) or "—",
        "security_in_at": _format_firestore_time_ist_12h(d.get("security_in_at")) or "—",
        "assigned_at": _format_firestore_time_ist_12h(d.get("assigned_at")) or "—",
        "can_assign": status_raw == "PENDING",
        "can_reassign": status_raw == "ASSIGNED" and not trip_started,
        "can_cancel": status_raw in ("PENDING", "ASSIGNED") and not trip_started,
    }


def _fetch_logistics_vehicle_requests_inner(
    db,
    *,
    ist_day=None,
    ist_day_from=None,
    ist_day_to=None,
    jmd_route_filter: str | None = None,
    limit=300,
):
    buf = []
    use_range = ist_day_from is not None or ist_day_to is not None
    day_from = day_to = None
    if use_range:
        day_from = ist_day_from or ist_day_to
        day_to = ist_day_to or ist_day_from
        if day_from > day_to:
            day_from, day_to = day_to, day_from
    for snap in _security_requests_snapshots(db, "VEHICLE_REQUEST"):
        d = snap.to_dict() or {}
        ts = d.get("requested_datetime")
        req_day = _requested_datetime_ist_date(ts)
        if ist_day is not None:
            if req_day != ist_day:
                continue
        elif use_range:
            if req_day is None:
                continue
            if req_day < day_from or req_day > day_to:
                continue
        if jmd_route_filter and not _vehicle_matches_unit_filter(d, jmd_route_filter):
            continue
        buf.append((_firestore_ts_to_sort_key(ts), d, snap.id))

    buf.sort(key=lambda x: x[0], reverse=True)
    if limit is not None:
        buf = buf[:limit]
    return [_logistics_vehicle_row(d, snap_id) for _, d, snap_id in buf]


_LOGISTICS_CSV_HEADERS = (
    "Employee",
    "From",
    "Type",
    "Category",
    "Destination",
    "Location",
    "Time",
    "Status",
    "Assignee",
    "Vehicle",
    "Department",
    "Load",
    "Distance",
    "Requested",
    "Vehicle Type",
    "Assigned At",
    "Security Out",
    "Security In",
)


def _csv_cell(value):
    """Plain ASCII-safe CSV cell; empty instead of UI em-dash placeholders."""
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s == "—":
        return ""
    return s


def _logistics_row_to_csv_values(row):
    requested = " ".join(
        p for p in (row.get("requested_date") or "", row.get("requested_time") or "") if p
    ).strip()
    return [
        _csv_cell(row.get("employee_name")),
        _csv_cell(row.get("from_unit_label")),
        _csv_cell(row.get("request_type_label")),
        _csv_cell(row.get("destination_category_label")),
        _csv_cell(row.get("destination_label")),
        _csv_cell(row.get("location_details")),
        _csv_cell(row.get("required_at")),
        _csv_cell(row.get("vehicle_status")),
        _csv_cell(row.get("assigned_to")),
        _csv_cell(row.get("fleet_vehicle_label")),
        _csv_cell((row.get("department") or "").upper()),
        _csv_cell(row.get("load_size_label")),
        _csv_cell(row.get("estimated_distance_display")),
        _csv_cell(requested),
        _csv_cell(row.get("vehicle_type_label")),
        _csv_cell(row.get("assigned_at")),
        _csv_cell(row.get("security_out_at")),
        _csv_cell(row.get("security_in_at")),
    ]


def _fetch_logistics_vehicle_requests_with_timeout(**kwargs):
    db, err = _get_firestore_client()
    if err:
        return [], err
    timeout = 60 if kwargs.get("ist_day_from") or kwargs.get("ist_day_to") else 25
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_fetch_logistics_vehicle_requests_inner, db, **kwargs)
            return future.result(timeout=timeout), None
    except TimeoutError:
        app.logger.error("Firestore logistics vehicle fetch timed out")
        return [], (
            "Firestore request timed out. Check that the Cloud Run service account has "
            "Cloud Datastore User on whatsapp-approval-system."
        )
    except Exception as e:
        app.logger.exception("Firestore logistics vehicle fetch failed")
        return [], _firestore_user_message(e)


def fetch_logistics_vehicle_requests(
    ist_day=None,
    *,
    jmd_route_filter: str | None = None,
):
    return _fetch_logistics_vehicle_requests_with_timeout(
        ist_day=ist_day,
        jmd_route_filter=jmd_route_filter,
    )


def fetch_logistics_vehicle_requests_range(
    ist_day_from,
    ist_day_to,
    *,
    jmd_route_filter: str | None = None,
):
    return _fetch_logistics_vehicle_requests_with_timeout(
        ist_day_from=ist_day_from,
        ist_day_to=ist_day_to,
        jmd_route_filter=jmd_route_filter,
        limit=10000,
    )


def _logistics_load_vehicle_request(db, request_id: str):
    rid = (request_id or "").strip()
    if not rid:
        return None, None, "Missing request id"
    ref = db.collection("requests").document(rid)
    snap = ref.get()
    if not snap.exists:
        return None, None, "Request not found"
    d = snap.to_dict() or {}
    if (d.get("type") or "").strip().upper() != "VEHICLE_REQUEST":
        return None, None, "Not a vehicle request"
    return ref, d, None


def _logistics_normalize_vehicle_type(raw: str) -> str:
    key = (raw or "").strip().lower().replace(" ", "_")
    if key in ("internal", "in_house", "company_vehicle"):
        return "in_house"
    if key in ("external", "external_hire", "hire", "external_vehicle"):
        return "external_hire"
    return key


def _logistics_send_assign_notifications(
    db,
    rd: dict,
    *,
    request_id: str,
    assignee_code: str,
    assignee_label: str,
    reassign: bool = False,
    old_assignee_wa: str = "",
    employee_wa: str = "",
) -> None:
    """Best-effort WhatsApp messages after portal assign / re-assign."""
    try:
        vehicle_notify.notify_vehicle_assignee(
            db,
            rd,
            request_id=request_id,
            assignee_code=assignee_code,
            assignee_label=assignee_label,
        )
        display = vehicle_notify.sentence_case_name(assignee_label)
        if reassign and old_assignee_wa:
            vehicle_notify.send_text(
                old_assignee_wa,
                f"The request has been re-assigned to {display}. Thanks.",
            )
        if employee_wa:
            if reassign:
                msg = f"Your vehicle request has been re-assigned to {display}."
            else:
                msg = f"Your vehicle request has been assigned to {display}."
            vehicle_notify.send_text(employee_wa, msg)
    except Exception:
        app.logger.exception(
            "logistics WhatsApp notify failed request_id=%s", request_id
        )


def _logistics_assign_vehicle(
    request_id: str,
    vehicle_type: str,
    assignee_code: str,
    *,
    fleet_vehicle_code: str = "",
    reassign: bool = False,
) -> tuple[bool, str | None]:
    db, err = _get_firestore_client()
    if err:
        return False, err
    ref, rd, err = _logistics_load_vehicle_request(db, request_id)
    if err:
        return False, err

    status = _logistics_vehicle_status_raw(rd)
    if reassign:
        if status != "ASSIGNED":
            return False, "Only assigned requests can be re-assigned"
        if _logistics_vehicle_trip_started(rd):
            return False, "Trip has already started"
    elif status != "PENDING":
        return False, "Request is not pending"

    vtype = _logistics_normalize_vehicle_type(vehicle_type)
    if vtype not in ("in_house", "external_hire"):
        return False, "Invalid vehicle type"

    code = _logistics_normalize_code(assignee_code)
    options = _logistics_assignee_options(db, vtype)
    label = _logistics_assignee_label(options, code)
    if not label:
        return False, "Invalid assignee"

    if reassign and code == _logistics_normalize_code(rd.get("assigned_to_code") or ""):
        return False, "Choose a different assignee"

    is_internal = vtype == "in_house"
    fleet_code = _logistics_normalize_code(fleet_vehicle_code) if is_internal else ""
    fleet_label = _logistics_fleet_vehicle_label(fleet_code) if fleet_code else ""
    if is_internal and not fleet_label:
        return False, "Please select a vehicle"

    staff_wa = _logistics_staff_wa_for_code(db, code) if is_internal else ""
    now = datetime.now(timezone.utc)
    actor = (getattr(current_user, "email", None) or "logistics_portal").strip()
    old_wa = (rd.get("assigned_to_wa") or "").strip() if reassign else ""
    employee_wa = (rd.get("employee") or "").strip()

    update = {
        "vehicle_request_status": "ASSIGNED",
        "vehicle_type": vtype,
        "vehicle_type_label": "Internal" if is_internal else "External",
        "assigned_to": label,
        "assigned_to_code": code,
        "assigned_to_wa": staff_wa,
        "assigned_by": actor,
        "assigned_at": now,
        "assignee_can_start": is_internal,
        "is_active_trip": False,
        "fleet_vehicle_code": fleet_code if is_internal else "",
        "fleet_vehicle_label": fleet_label if is_internal else "",
    }
    if reassign:
        update["previous_assignee"] = rd.get("assigned_to") or ""
        update["previous_assignee_code"] = rd.get("assigned_to_code") or ""
        update["previous_assignee_wa"] = rd.get("assigned_to_wa") or ""
        update["reassigned_at"] = now
    try:
        ref.update(update)
    except Exception as e:
        app.logger.exception("logistics assign failed request_id=%s", request_id)
        return False, str(e)

    updated = ref.get().to_dict() or {}
    _logistics_send_assign_notifications(
        db,
        updated,
        request_id=request_id,
        assignee_code=code,
        assignee_label=label,
        reassign=reassign,
        old_assignee_wa=old_wa,
        employee_wa=employee_wa,
    )
    app.logger.info(
        "logistics %s request_id=%s assignee=%s by=%s",
        "reassign" if reassign else "assign",
        request_id,
        label,
        actor,
    )
    return True, None


def _logistics_cancel_vehicle(request_id: str) -> tuple[bool, str | None]:
    db, err = _get_firestore_client()
    if err:
        return False, err
    ref, rd, err = _logistics_load_vehicle_request(db, request_id)
    if err:
        return False, err

    status = _logistics_vehicle_status_raw(rd)
    if _logistics_vehicle_trip_started(rd):
        return False, "Trip has already started"
    if status not in ("PENDING", "ASSIGNED"):
        return False, "Only pending or assigned requests can be cancelled"

    actor = (getattr(current_user, "email", None) or "logistics_portal").strip()
    now = datetime.now(timezone.utc)
    assignee_wa = (rd.get("assigned_to_wa") or "").strip()
    employee_wa = (rd.get("employee") or "").strip()
    try:
        ref.update({
            "vehicle_request_status": "CANCELLED",
            "cancelled_by": actor,
            "cancelled_at": now,
            "assignee_can_start": False,
            "is_active_trip": False,
        })
    except Exception as e:
        app.logger.exception("logistics cancel failed request_id=%s", request_id)
        return False, str(e)
    try:
        if assignee_wa:
            vehicle_notify.send_text(
                assignee_wa,
                "Your assigned vehicle request has been cancelled by logistics.",
            )
        if employee_wa:
            vehicle_notify.send_text(
                employee_wa,
                "Your vehicle request has been cancelled by logistics.",
            )
    except Exception:
        app.logger.exception(
            "logistics cancel WhatsApp notify failed request_id=%s", request_id
        )
    app.logger.info("logistics cancel request_id=%s by=%s", request_id, actor)
    return True, None


def _parse_permission_ddmmy(raw: str):
    s = (raw or "").strip()
    if not s:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def _parse_shift_hhmm(raw: str):
    s = (raw or "").strip()
    if not s:
        return None
    parts = s.split(":")
    try:
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return None


def _shift_bounds_on_date(perm_date, login: str, logout: str):
    """IST datetimes for shift start/end; logout next day if overnight."""
    li = _parse_shift_hhmm(login)
    lo = _parse_shift_hhmm(logout)
    if not li or not lo or not perm_date:
        return None, None
    tz = _ist_tzinfo()
    start = datetime(perm_date.year, perm_date.month, perm_date.day, li[0], li[1], tzinfo=tz)
    end = datetime(perm_date.year, perm_date.month, perm_date.day, lo[0], lo[1], tzinfo=tz)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def _resolve_shift_times(user: dict | None, permission_shift: str):
    """Regular login/logout HH:MM from user doc (GS = shift_login/logout)."""
    if not user:
        return None
    st = (user.get("shift_type") or "GS").strip().upper()
    shift = (permission_shift or "I").strip().upper()
    if st == "GS":
        login = user.get("shift_login")
        logout = user.get("shift_logout")
    elif shift in ("II", "2"):
        login = user.get("shift2_login")
        logout = user.get("shift2_logout")
    else:
        login = user.get("shift1_login")
        logout = user.get("shift1_logout")
    if not login or not logout:
        return None
    return str(login).strip(), str(logout).strip()


def _permission_shift_display(user: dict | None, d: dict) -> str:
    ps = (d.get("permission_shift") or "").strip().upper()
    if ps in ("I", "1"):
        return "I"
    if ps in ("II", "2"):
        return "II"
    if user and (user.get("shift_type") or "").strip().upper() == "GS":
        return "I"
    return "—"


def _format_permission_duration(minutes: int) -> str:
    if minutes <= 0:
        return "—"
    hours, mins = divmod(minutes, 60)
    if hours and mins:
        return f"{hours}H {mins}M"
    if hours:
        return f"{hours}H"
    return f"{mins}M"


def _permission_hour_approved(d: dict) -> bool:
    if d.get("cancelled_by_employee"):
        return False
    return _permission_jmd_approved_for_gate(d)


def _compute_permission_hour(d: dict, user: dict | None) -> str:
    if not _permission_hour_approved(d):
        return "—"
    perm_date = _parse_permission_ddmmy(
        (d.get("permission_work_date") or d.get("permission_date") or "")
    )
    if not perm_date:
        return "—"
    shift_code = (d.get("permission_shift") or "").strip().upper()
    if user and (user.get("shift_type") or "").strip().upper() == "GS":
        shift_code = "I"
    bounds = _resolve_shift_times(user, shift_code)
    if not bounds:
        return "—"
    reg_in, reg_out = _shift_bounds_on_date(perm_date, bounds[0], bounds[1])
    if not reg_in or not reg_out:
        return "—"

    in_at = _firestore_value_to_utc_datetime(d.get("security_in_at"))
    out_at = _firestore_value_to_utc_datetime(d.get("security_out_at"))
    if in_at:
        in_at = in_at.astimezone(_ist_tzinfo())
    if out_at:
        out_at = out_at.astimezone(_ist_tzinfo())

    kind = _permission_type_kind(d)
    if kind == "other":
        if not in_at or not out_at:
            return "—"
        mins = int((in_at - out_at).total_seconds() // 60)
        return _format_permission_duration(mins)
    if kind == "early_out":
        if not out_at:
            return "—"
        mins = abs(int((out_at - reg_out).total_seconds() // 60))
        return _format_permission_duration(mins)
    if kind == "late_in":
        if not in_at:
            return "—"
        mins = int((in_at - reg_in).total_seconds() // 60)
        return _format_permission_duration(mins)
    return "—"


def _load_users_by_wa(db, wa_ids: set[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for wa in wa_ids:
        key = (wa or "").strip()
        if not key:
            continue
        try:
            snap = db.collection("users").document(key).get()
            if snap.exists:
                out[key] = snap.to_dict() or {}
        except Exception:
            app.logger.warning("users lookup failed wa=%s", key)
    return out


def _permission_type_kind(d: dict) -> str:
    """late_in | early_out | other — from bot permission_type / permission_type_code."""
    code = (d.get("permission_type_code") or "").strip().upper()
    label = (d.get("permission_type") or "").strip().lower()
    if code == "PERMISSION_LATE_IN" or label in ("late in", "latein"):
        return "late_in"
    if code == "PERMISSION_EARLY_OUT" or label in ("early out", "earlyout"):
        return "early_out"
    return "other"


def _permission_jmd_approved_for_gate(d: dict) -> bool:
    if d.get("cancelled_by_employee"):
        return False
    jmd = (d.get("jmd_status") or "").strip().upper()
    if jmd != "APPROVED":
        return False
    md = (d.get("md_status") or "").strip().upper()
    if md in ("", "N/A"):
        return True
    if md == "OFFLINE" and d.get("md_offline_bypass"):
        return True
    if md in ("AWAITING_JMD", "PENDING"):
        return False
    return md == "APPROVED"


def _permission_security_gate_flags(d: dict) -> dict:
    """UI flags for Security permission Action column."""
    kind = _permission_type_kind(d)
    approved = _permission_jmd_approved_for_gate(d)
    out_at = d.get("security_out_at")
    in_at = d.get("security_in_at")

    gate_closed = False
    show_out_btn = False
    show_in_btn = False
    out_enabled = False
    in_enabled = False

    if kind == "late_in":
        gate_closed = in_at is not None
        show_in_btn = not gate_closed
        in_enabled = approved and show_in_btn
    elif kind == "early_out":
        gate_closed = out_at is not None
        show_out_btn = not gate_closed
        out_enabled = approved and show_out_btn
    else:
        gate_closed = out_at is not None and in_at is not None
        if gate_closed:
            pass
        elif out_at is None:
            show_out_btn = True
            out_enabled = approved
        else:
            show_in_btn = True
            in_enabled = approved

    return {
        "gate_closed": gate_closed,
        "show_out_btn": show_out_btn,
        "show_in_btn": show_in_btn,
        "out_enabled": out_enabled,
        "in_enabled": in_enabled,
        "permission_kind": kind,
        "fully_approved": approved,
    }


def _permission_row_matches_ist_day(d: dict, date_str: str) -> bool:
    if (d.get("permission_date") or "").strip() == date_str:
        return True
    if (d.get("permission_work_date") or "").strip() == date_str:
        return True
    return False


def _permission_snapshots_for_ist_day(db, ist_day, *, limit: int = 200):
    """PERMISSION rows for one IST calendar day (work date or request date)."""
    if ist_day is None:
        return _security_requests_by_type(db, "PERMISSION", limit=limit)
    date_str = ist_day.strftime("%d-%m-%Y")
    coll = db.collection("requests")
    try:
        q = (
            coll.where("type", "==", "PERMISSION")
            .where("permission_date", "==", date_str)
            .limit(limit)
        )
        snaps = {snap.id: snap for snap in q.stream()}
        q2 = (
            coll.where("type", "==", "PERMISSION")
            .where("permission_work_date", "==", date_str)
            .limit(limit)
        )
        for snap in q2.stream():
            snaps[snap.id] = snap
        return list(snaps.values())[:limit]
    except Exception as e:
        app.logger.warning(
            "Firestore permission date query failed, using type filter: %s", e
        )
    snaps = _security_requests_by_type(db, "PERMISSION", limit=limit * 2)
    out = []
    for snap in snaps:
        d = snap.to_dict() or {}
        if _permission_row_matches_ist_day(d, date_str):
            out.append(snap)
        if len(out) >= limit:
            break
    return out


def _fetch_security_permission_requests_inner(
    ist_day,
    db,
    *,
    jmd_route_filter: str | None = None,
):
    buf = []
    for snap in _permission_snapshots_for_ist_day(db, ist_day, limit=400):
        d = snap.to_dict() or {}
        if jmd_route_filter and _request_jmd_route(d) != jmd_route_filter:
            continue
        ts = d.get("requested_datetime")
        buf.append((_firestore_ts_to_sort_key(ts), d, snap.id))

    buf.sort(key=lambda x: x[0], reverse=True)
    buf = buf[:200]

    wa_ids = {(d.get("employee") or "").strip() for _, d, _ in buf if (d.get("employee") or "").strip()}
    users_by_wa = _load_users_by_wa(db, wa_ids)
    md_wa = _md_whatsapp_for_security(db)
    md_offline = _approver_is_offline(db, md_wa)

    emp_rows = []
    cl_rows = []
    for _, d, snap_id in buf:
        gate = _permission_security_gate_flags(d)
        wa = (d.get("employee") or "").strip()
        user = users_by_wa.get(wa)
        is_cl = (d.get("permission_for") or "").strip().lower() == "cl"
        jmd_display, md_display = _permission_approval_statuses_for_display(
            d,
            md_offline=False if is_cl else md_offline,
        )
        row = {
            "request_id": d.get("request_id") or snap_id,
            "employee_id": d.get("employee_id") or "",
            "employee_name": d.get("employee_name") or "",
            "cl_employee_name": d.get("cl_employee_name") or "",
            "raised_by_name": d.get("raised_by_name") or d.get("employee_name") or "",
            "department": d.get("department") or "",
            "reason": d.get("reason") or "",
            "permission_type": d.get("permission_type") or "",
            "permission_expected_in": (d.get("permission_expected_in") or "").strip(),
            "permission_expected_out": (d.get("permission_expected_out") or "").strip(),
            "permission_shift": _permission_shift_display(user, d),
            "jmd_status": jmd_display,
            "md_status": md_display,
            "security_out_at": _format_firestore_time_ist_12h(
                d.get("security_out_at")
            ),
            "security_in_at": _format_firestore_time_ist_12h(
                d.get("security_in_at")
            ),
            "permission_hour": _compute_permission_hour(d, user),
            **gate,
        }
        if is_cl:
            cl_rows.append(row)
        else:
            emp_rows.append(row)
    return emp_rows, cl_rows


def _security_record_permission_gate(request_id: str, action: str):
    """Record OUT / IN for approved permission requests. Returns (ok, error_message)."""
    db, err = _get_firestore_client()
    if err:
        return False, err
    action = (action or "").strip().lower()
    if action not in ("out", "in"):
        return False, "Invalid action"
    rid = (request_id or "").strip()
    if not rid:
        return False, "Missing request id"
    try:
        ref = db.collection("requests").document(rid)
        snap = ref.get()
        if not snap.exists:
            return False, "Request not found"
        d = snap.to_dict() or {}
        if (d.get("type") or "").strip().upper() != "PERMISSION":
            return False, "Not a permission request"
        if not _permission_jmd_approved_for_gate(d):
            return False, "Permission is not approved yet"
        kind = _permission_type_kind(d)
        out_at = d.get("security_out_at")
        in_at = d.get("security_in_at")
        now = datetime.now(timezone.utc)
        if action == "out":
            if kind == "late_in":
                return False, "OUT is not required for Late IN permission"
            if out_at is not None:
                return False, "Out time is already recorded"
            ref.update({"security_out_at": now})
            return True, None
        if kind == "early_out":
            return False, "IN is not required for Early OUT permission"
        if in_at is not None:
            return False, "In time is already recorded"
        if kind == "other" and out_at is None:
            return False, "Record OUT before IN"
        ref.update({"security_in_at": now})
        return True, None
    except Exception as e:
        app.logger.exception("security permission gate update failed")
        return False, str(e)


def fetch_security_permission_requests(
    ist_day=None,
    *,
    jmd_route_filter: str | None = None,
):
    """Load PERMISSION requests for one IST day; returns (emp_rows, cl_rows, error)."""
    db, err = _get_firestore_client()
    if err:
        return [], [], err
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _fetch_security_permission_requests_inner,
                ist_day,
                db,
                jmd_route_filter=jmd_route_filter,
            )
            emp_rows, cl_rows = future.result(timeout=25)
            return emp_rows, cl_rows, None
    except TimeoutError:
        app.logger.error("Firestore security permission fetch timed out")
        return [], [], (
            "Firestore request timed out. Check that the Cloud Run service account has "
            "Cloud Datastore User on whatsapp-approval-system."
        )
    except Exception as e:
        app.logger.exception("Firestore security permission fetch failed")
        return [], [], _firestore_user_message(e)


def fetch_security_visitor_requests(
    ist_day=None,
    *,
    jmd_route_filter: str | None = None,
    security_tab_unit: str = "unit-i",
):
    """Load VISITOR requests for one Coming On date (IST calendar day), newest first, cap 200."""
    db, err = _get_firestore_client()
    if err:
        return [], err
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _fetch_security_visitor_requests_inner,
                ist_day,
                db,
                jmd_route_filter=jmd_route_filter,
                security_tab_unit=security_tab_unit,
            )
            return future.result(timeout=25), None
    except TimeoutError:
        app.logger.error("Firestore security visitor fetch timed out")
        return [], (
            "Firestore request timed out. Check that the Cloud Run service account has "
            "Cloud Datastore User on whatsapp-approval-system."
        )
    except Exception as e:
        app.logger.exception("Firestore security visitor fetch failed")
        return [], _firestore_user_message(e)


def _security_record_visitor_gate(
    request_id: str,
    action: str,
    otp: str,
    *,
    security_unit: str = "unit-i",
):
    """Visitor gate: IN (OTP required) then OUT. Per-unit times when visiting Both."""
    db, err = _get_firestore_client()
    if err:
        return False, err
    action = (action or "").strip().lower()
    if action not in ("in", "out"):
        return False, "Invalid action"
    rid = (request_id or "").strip()
    if not rid:
        return False, "Missing request id"
    try:
        ref = db.collection("requests").document(rid)
        snap = ref.get()
        if not snap.exists:
            return False, "Request not found"
        d = snap.to_dict() or {}
        if (d.get("type") or "").strip().upper() != "VISITOR":
            return False, "Not a visitor request"
        md_wa = _md_whatsapp_for_security(db)
        md_offline = _approver_is_offline(db, md_wa) if md_wa else False
        d = _backfill_visitor_otp_if_md_offline(ref, d)
        if not _visitor_security_fully_approved(d, md_offline):
            return False, "Visitor request is not fully approved yet (MD approval pending)"
        in_field, out_field = _visitor_gate_field_names(d, security_unit)
        in_at = d.get(in_field)
        out_at = d.get(out_field)
        now = datetime.now(timezone.utc)
        stored_otp = _normalize_visitor_otp(d.get("visitor_otp"))
        if action == "in":
            if in_at is not None:
                return False, "In time is already recorded for this unit"
            if not stored_otp:
                return False, "No entry OTP on this request yet (wait for MD approval)"
            entered = _normalize_visitor_otp(otp)
            if not entered:
                return False, "Entry OTP is required"
            if entered != stored_otp:
                return False, "Incorrect OTP"
            ref.update({in_field: now})
            return True, None
        if out_at is not None:
            return False, "Out time is already recorded for this unit"
        if in_at is None:
            return False, "Record IN before OUT for this unit"
        ref.update({out_field: now})
        return True, None
    except Exception as e:
        app.logger.exception("security visitor gate update failed")
        return False, str(e)


def require_page(page_key):
    """Abort 403 if current user is not allowed to access this page."""
    if current_user.role == "admin":
        return
    if current_user.role == "editor":
        if page_key == "admin":
            abort(403)
        return
    if current_user.role == "viewer":
        if page_key not in current_user.allowed_pages:
            abort(403)


def _fetch_history_dashboard_rows():
    """History tab date/shift/unit/dept filters plus machine idle + IoT rows (same logic as index)."""
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    selected_date = request.args.get("dateFilter") or yesterday
    selected_shift = request.args.get("shiftSlicer") or "All"
    selected_unit = request.args.get("unitSlicer") or "All"
    selected_department = request.args.get("departmentSlicer") or "All"
    machine_rows = fetch_machine_idle_rows(
        date_str=selected_date,
        shift=selected_shift,
        unit=selected_unit,
        department=selected_department,
    )
    iot_rows = fetch_iot_master_rows(
        date_str=selected_date,
        shift=selected_shift,
        unit=selected_unit,
        department=selected_department,
    )
    if not machine_rows and selected_date == yesterday:
        max_date = _get_max_date_machine_idle()
        if max_date and max_date != selected_date:
            selected_date = max_date
            machine_rows = fetch_machine_idle_rows(
                date_str=selected_date,
                shift=selected_shift,
                unit=selected_unit,
                department=selected_department,
            )
            iot_rows = fetch_iot_master_rows(
                date_str=selected_date,
                shift=selected_shift,
                unit=selected_unit,
                department=selected_department,
            )
    return selected_date, selected_shift, selected_unit, selected_department, machine_rows, iot_rows


@app.context_processor
def inject_nav_permissions():
    """Make allowed_pages and is_admin available in templates."""
    if current_user.is_authenticated:
        role = (getattr(current_user, "role", None) or "viewer").strip().lower()
        pages = getattr(current_user, "allowed_pages", None) or []
        def _show(page_key: str) -> bool:
            return role in ("admin", "editor") or (
                role == "viewer" and page_key in pages
            )

        return {
            "allowed_pages": pages,
            "user_role": role,
            "is_admin": role == "admin",
            "show_security_nav": _show("security"),
            "show_hr_nav": _show("hr"),
            "show_it_nav": _show("it"),
            "show_ppc_nav": _show("logistics") or _show("maintenance"),
            "show_logistics_subnav": _show("logistics"),
            "show_maintenance_subnav": _show("maintenance"),
            "iot_health_monitoring_enabled": IOT_HEALTH_MONITORING_ENABLED,
        }
    return {
        "allowed_pages": [],
        "user_role": "",
        "is_admin": False,
        "show_security_nav": False,
        "show_hr_nav": False,
        "show_it_nav": False,
        "show_ppc_nav": False,
        "show_logistics_subnav": False,
        "show_maintenance_subnav": False,
        "iot_health_monitoring_enabled": IOT_HEALTH_MONITORING_ENABLED,
    }


@app.route("/")
@login_required
def index():
    require_page("production")

    (
        selected_date,
        selected_shift,
        selected_unit,
        selected_department,
        machine_rows,
        iot_rows,
    ) = _fetch_history_dashboard_rows()
    iot_part_machine_map = fetch_iot_part_machine_rows(
        date_str=selected_date,
        shift=selected_shift,
        unit=selected_unit,
        department=selected_department,
    )

    # Realtime slicers must not affect History; keep them in separate query params.
    selected_realtime_unit = request.args.get("realtimeUnitSlicer") or selected_unit
    selected_realtime_department = request.args.get("realtimeDepartmentSlicer") or selected_department
    if not selected_realtime_unit or selected_realtime_unit == "All":
        selected_realtime_unit = UNIT_OPTIONS[0] if UNIT_OPTIONS else None
    if not selected_realtime_department or selected_realtime_department == "All":
        selected_realtime_department = DEPARTMENT_OPTIONS[0] if DEPARTMENT_OPTIONS else None

    realtime_refresh = request.args.get("realtime_refresh") == "1"
    realtime_rows = fetch_realtime_latest_rows(
        unit=selected_realtime_unit,
        department=selected_realtime_department,
        bypass_cache=realtime_refresh,
    )

    highlights_filter = auth.get_user_preference(current_user.id, "highlightsFilter") or "bad"

    plan_department_tabs = ("PDC", "FET", "CNC", "SEC")
    plan_tab = (request.args.get("planTab") or "PDC").strip().upper()
    if plan_tab not in plan_department_tabs:
        plan_tab = "PDC"
    plan_department_rows = []
    if _user_has_ppc_access():
        plan_department_rows = fetch_department_job_allocations(plan_tab)

    return render_template(
        "index.html",
        machine_rows=machine_rows,
        iot_rows=iot_rows,
        iot_part_machine_map=iot_part_machine_map,
        realtime_rows=realtime_rows,
        selected_date=selected_date,
        selected_shift=selected_shift,
        selected_unit=selected_unit,
        selected_department=selected_department,
        selected_realtime_unit=selected_realtime_unit,
        selected_realtime_department=selected_realtime_department,
        shift_options=SHIFT_OPTIONS,
        unit_options=UNIT_OPTIONS,
        department_options=DEPARTMENT_OPTIONS,
        active_nav="production",
        highlights_filter=highlights_filter,
        show_plan_tab=_user_has_ppc_access(),
        plan_tab=plan_tab,
        plan_department_tabs=plan_department_tabs,
        plan_department_rows=plan_department_rows,
    )


@app.route("/admin", methods=["GET", "POST"])
@login_required
def admin():
    require_page("admin")
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        role = request.form.get("role", "viewer")
        if not email:
            flash("Email is required.", "danger")
            return redirect(url_for("admin"))
        if role not in ("admin", "editor", "viewer"):
            role = "viewer"
        pages = request.form.getlist("pages") if role == "viewer" else []
        password = secrets.token_urlsafe(12)
        user_id = auth.create_user(email, password, role)
        if user_id is None:
            flash(f"User with email {email} already exists.", "danger")
            return redirect(url_for("admin"))
        if role == "viewer":
            auth.set_viewer_pages(user_id, pages)
        flash(
            f"User {email} created. Generated password: {password} (copy it now; it won't be shown again).",
            "success",
        )
        return redirect(url_for("admin"))
    users = auth.list_users_with_permissions()
    return render_template(
        "admin.html",
        users=users,
        page_options=auth.PAGE_KEYS,
        active_nav="admin",
        current_user_id=current_user.id,
    )


@app.route("/admin/delete/<int:user_id>", methods=["POST"])
@login_required
def admin_delete_user(user_id):
    require_page("admin")
    if int(user_id) == int(current_user.id):
        flash("You cannot delete your own account.", "danger")
        return redirect(url_for("admin"))
    ok, result = auth.delete_user(user_id)
    if not ok:
        flash(result, "danger")
    else:
        flash(f"User {result} deleted.", "success")
    return redirect(url_for("admin"))


@app.route("/realtime")
@login_required
def realtime():
    require_page("realtime")
    return render_template("under_development.html", active_nav="realtime")


PARTS_TABLE = "alubee-prod.alubee_production_marts.dim_component_mapper"
MONTHLY_PLANNER_TABLE = "alubee-prod.alubee_production_marts.dim_monthly_planner"
JOB_ALLOCATOR_TABLE = "alubee-prod.alubee_production_marts.fact_job_allocator"
PLAN_CHANGE_REQUEST_TABLE = "alubee-prod.alubee_production_marts.fact_plan_change_request"
REALTIME_LATEST_TABLE = "alubee-prod.alubee_production_marts.fact_realtime_latest"
# Production dashboard Realtime tab reads current-shift rows from staging master catalog (not fact_realtime_latest).
REALTIME_MASTER_CATALOG_SOURCE = "alubee-prod.alubee_production_staging.vw_master_catalog"
DIM_MACHINE_MAPPER_TABLE = "alubee-prod.alubee_production_marts.dim_machine_mapper"
FACTS_REALTIME_LOGS_TABLE = "alubee-prod.alubee_production_marts.facts_realtime_logs"
# IoT "Department" filter: CNC / VMC / PDC from dim_machine_mapper.Machine_no (substring); unit uses mm.Unit.
_IOT_DEPARTMENT_FROM_MACHINE_NO_SQL = """CASE
  WHEN UPPER(COALESCE(TRIM(CAST(mm.Machine_no AS STRING)), '')) LIKE '%CNC%' THEN 'CNC'
  WHEN UPPER(COALESCE(TRIM(CAST(mm.Machine_no AS STRING)), '')) LIKE '%VMC%' THEN 'VMC'
  ELSE 'PDC'
END"""
# IoT Realtime: (BigQuery field, single-token UI header). Row dicts use BQ keys; template shows labels.
IOT_REALTIME_LOG_COLUMNS_UI = (
    ("publish_time_ist", "Timestamp"),
    ("device_id", "Device"),
    ("iot_status", "Status"),
    ("wifi_mac", "MAC"),
    ("wifi_ip", "IP"),
    ("wifi_status", "Netstate"),
    ("wifi_rssi_dbm", "RSSI"),
    ("wifi_disconnect_count", "Disconnects"),
    ("wifi_reconnect_count", "Reconnects"),
    ("uptime_ms", "Uptime"),
    ("boot_count", "Boots"),
    ("reset_reason", "Reset"),
    ("scheduled_reset_morning_ok", "AMreset"),
    ("scheduled_restart_morning_ok", "AMreboot"),
    ("scheduled_reset_evening_ok", "PMreset"),
    ("scheduled_restart_evening_ok", "PMreboot"),
    ("free_heap_bytes", "Heapfree"),
    ("min_free_heap_bytes", "Heapmin"),
    ("loop_time_ms_avg", "Loopavg"),
    ("loop_time_ms_max", "Loopmax"),
    ("error_code", "Errcode"),
    ("error_source", "Errsource"),
    ("error_msg", "Errmsg"),
    ("last_error_epoch", "Errepoch"),
    ("error_count_today", "Errcount"),
    ("chip_temp_c", "Chiptemp"),
    ("i2c_lcd_0x27_present", "Lcd27"),
    ("i2c_lcd_probe_fail_count", "Lcdfails"),
    ("i2c_garbage_suspected", "I2cwarn"),
)
IOT_REALTIME_LOG_BQ_KEYS = tuple(k for k, _ in IOT_REALTIME_LOG_COLUMNS_UI)
# Template-only key: per-column severity for IoT realtime row highlighting ("error" | "warn" | "").
IOT_REALTIME_LEVELS_KEY = "__iot_cell_levels__"
# No red/yellow on these cells (identity / network / raw error text); they still count for Device row level.
IOT_REALTIME_NO_CELL_HIGHLIGHT_BQ_KEYS = frozenset(
    {
        "publish_time_ist",
        "wifi_mac",
        "wifi_ip",
        "error_code",
        "error_source",
        "error_msg",
        "last_error_epoch",
        "iot_status",
    }
)

# IoT AM/PM scheduled reset-reboot: highlight not-OK as error only after these IST cutoffs
# (before 08:00 IST, morning fields are "not yet due"; before 21:00 IST, evening fields same).
IOT_SCHED_IST_ZONE_NAME = "Asia/Kolkata"
IOT_SCHED_AM_NOT_OK_CRITICAL_FROM_HOUR = 8
IOT_SCHED_AM_NOT_OK_CRITICAL_FROM_MINUTE = 0
IOT_SCHED_PM_NOT_OK_CRITICAL_FROM_HOUR = 21
IOT_SCHED_PM_NOT_OK_CRITICAL_FROM_MINUTE = 0


def _iot_now_ist() -> datetime:
    """Wall clock in Asia/Kolkata for AM/PM schedule highlight gates."""
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(IOT_SCHED_IST_ZONE_NAME))
    from datetime import timezone, timedelta

    return datetime.now(timezone(timedelta(hours=5, minutes=30)))


def _iot_sched_past_ist_hm(now_ist: datetime, hour: int, minute: int) -> bool:
    return now_ist.hour * 60 + now_ist.minute >= hour * 60 + minute


def _iot_realtime_num(val):
    """Coerce BigQuery numeric-ish values to float, or None if not numeric."""
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        if isinstance(val, float) and val != val:  # NaN
            return None
        return float(val)
    try:
        from decimal import Decimal

        if isinstance(val, Decimal):
            return float(val)
    except ImportError:
        pass
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _iot_sched_slot_not_ok(val) -> bool:
    """Scheduled OK flag is failing: False / 0; missing or unknown → not treated as failure."""
    if val is None:
        return False
    if val is True:
        return False
    if val is False:
        return True
    n = _iot_realtime_num(val)
    if n is not None:
        return n == 0
    s = str(val).strip().lower()
    if not s or s in ("-", "none", "null", "n/a", "na"):
        return False
    if s in ("0", "false", "no", "fail", "failed", "off"):
        return True
    return False


def _iot_realtime_cell_level(key: str, val) -> str | None:
    """Classify one raw cell: 'error' (abnormal), 'warn', or None (normal)."""
    if key == "device_id":
        return None

    if key == "iot_status" and val is not None:
        if str(val).strip().lower() == "stopped":
            return "error"

    if key == "wifi_status" and val is not None:
        s = str(val).strip().lower()
        if s and s != "-":
            bad = ("disconnect", "offline", "no ap", "fail", "error", "lost", "disconn")
            if any(b in s for b in bad):
                return "warn"

    if key in ("wifi_rssi_dbm",) and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n < -82:
            return "warn"

    if key in ("wifi_disconnect_count", "wifi_reconnect_count") and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n > 5:
            return "warn"

    if key == "boot_count" and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n > 30:
            return "warn"

    if key == "reset_reason" and val is not None:
        s = str(val).strip().upper()
        if not s or s == "-":
            return None
        ok_tokens = (
            "POWERON",
            "POWER_ON",
            "ESP_RST_POWERON",
            "DEEPSLEEP_RESET",
            "SW_CPU_RESET",
            "RTCWDT_RTC_RESET",
            "ESP_RST_DEEPSLEEP",
            "SW_RESET",
            "ESP_RST_SW",
        )
        if s not in ok_tokens:
            return "warn"

    if key in (
        "scheduled_reset_morning_ok",
        "scheduled_restart_morning_ok",
        "scheduled_reset_evening_ok",
        "scheduled_restart_evening_ok",
    ):
        if not _iot_sched_slot_not_ok(val):
            return None
        now_ist = _iot_now_ist()
        if key in ("scheduled_reset_morning_ok", "scheduled_restart_morning_ok"):
            if _iot_sched_past_ist_hm(
                now_ist,
                IOT_SCHED_AM_NOT_OK_CRITICAL_FROM_HOUR,
                IOT_SCHED_AM_NOT_OK_CRITICAL_FROM_MINUTE,
            ):
                return "error"
            return None
        if _iot_sched_past_ist_hm(
            now_ist,
            IOT_SCHED_PM_NOT_OK_CRITICAL_FROM_HOUR,
            IOT_SCHED_PM_NOT_OK_CRITICAL_FROM_MINUTE,
        ):
            return "error"
        return None

    if key == "free_heap_bytes" and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n < 25_000:
            return "warn"

    if key == "min_free_heap_bytes" and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n < 8_192:
            return "error"
        if n is not None and n < 20_000:
            return "warn"

    if key == "loop_time_ms_max" and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n > 200:
            return "warn"

    if key == "loop_time_ms_avg" and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n > 100:
            return "warn"

    if key == "error_code" and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n != 0:
            return "error"
        st = str(val).strip().lower()
        if st and st not in ("0", "-", "none", "null"):
            return "error"

    if key == "error_source" and val is not None:
        st = str(val).strip()
        if st and st not in ("-", "none", "null", "n/a", "na"):
            return "warn"

    if key == "error_msg" and val is not None:
        st = str(val).strip()
        if st and st.lower() not in ("-", "none", "null", "n/a", "na"):
            return "error"

    if key == "error_count_today" and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n >= 10:
            return "error"
        if n is not None and n >= 1:
            return "warn"

    if key == "chip_temp_c" and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n >= 90:
            return "error"
        if n is not None and n >= 75:
            return "warn"

    if key == "i2c_lcd_0x27_present":
        if val is False:
            return "warn"

    if key == "i2c_lcd_probe_fail_count" and val is not None:
        n = _iot_realtime_num(val)
        if n is not None and n >= 5:
            return "error"
        if n is not None and n >= 1:
            return "warn"

    if key == "i2c_garbage_suspected":
        if val is True:
            return "error"

    return None


def _iot_realtime_levels_for_row(raw: dict) -> dict[str, str]:
    """Map each BQ column key to '', 'warn', or 'error'. device_id reflects worst severity in the row."""
    out: dict[str, str] = {}
    worst: str | None = None
    for k in IOT_REALTIME_LOG_BQ_KEYS:
        if k == "device_id":
            continue
        lv = _iot_realtime_cell_level(k, raw.get(k))
        if lv == "error":
            worst = "error"
        elif lv == "warn" and worst != "error":
            worst = "warn"
        if k in IOT_REALTIME_NO_CELL_HIGHLIGHT_BQ_KEYS:
            out[k] = ""
        else:
            out[k] = lv or ""
    out["device_id"] = worst or ""
    return out


def _iot_realtime_log_cell(val):
    """Format IoT realtime table cells; missing / placeholder strings show as '-'."""
    if val is None:
        return "-"
    if isinstance(val, bool):
        return val
    if isinstance(val, float) and val != val:  # NaN
        return "-"
    if isinstance(val, str):
        st = val.strip()
        if not st or st.lower() in ("none", "null", "n/a", "na"):
            return "-"
    if isinstance(val, datetime):
        return val.isoformat(sep=" ", timespec="seconds")
    if isinstance(val, date):
        return val.isoformat()
    return val


def fetch_iot_realtime_logs_distinct_filters():
    """Distinct unit (dim_machine_mapper.Unit), department (CNC/VMC/PDC from mapper Machine_no), device_id."""
    if get_bq_client() is None:
        return [], [], []
    cache_key = "iot_realtime_log_filters_v3"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    units_sql = f"""
        SELECT DISTINCT TRIM(CAST(mm.Unit AS STRING)) AS v
        FROM `{FACTS_REALTIME_LOGS_TABLE}` f
        INNER JOIN `{DIM_MACHINE_MAPPER_TABLE}` mm
          ON mm.Device_ID = SAFE_CAST(f.device_id AS INT64)
        WHERE mm.Unit IS NOT NULL AND TRIM(CAST(mm.Unit AS STRING)) != ''
          AND f.publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        ORDER BY 1
    """
    mt_sql = f"""
        SELECT DISTINCT d.dept AS v
        FROM (
          SELECT {_IOT_DEPARTMENT_FROM_MACHINE_NO_SQL} AS dept
          FROM `{FACTS_REALTIME_LOGS_TABLE}` f
          LEFT JOIN `{DIM_MACHINE_MAPPER_TABLE}` mm
            ON mm.Device_ID = SAFE_CAST(f.device_id AS INT64)
          WHERE f.publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        ) AS d
        ORDER BY 1
    """
    dev_sql = f"""
        SELECT DISTINCT device_id AS v
        FROM `{FACTS_REALTIME_LOGS_TABLE}` f
        WHERE device_id IS NOT NULL AND TRIM(CAST(device_id AS STRING)) != ''
          AND f.publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        ORDER BY 1
    """
    try:
        units = [str(r["v"]) for r in get_bq_client().query(units_sql).result()]
        mts = [str(r["v"]) for r in get_bq_client().query(mt_sql).result()]
        devices = [str(r["v"]) for r in get_bq_client().query(dev_sql).result()]
        out = (units, mts, devices)
        _cache_set(cache_key, out, ttl_sec=120)
        return out
    except Exception as e:
        app.logger.warning("BigQuery fetch_iot_realtime_logs_distinct_filters failed: %s", e)
        return [], [], []


def fetch_iot_realtime_logs_table(
    iot_unit: str | None,
    iot_machine_type: str | None,
    iot_device: str | None = None,
    limit: int = 500,
):
    """Latest row per device plus Status from latest vs previous publish_time."""
    if get_bq_client() is None:
        return []
    iot_unit = (iot_unit or "All").strip()
    iot_machine_type = (iot_machine_type or "All").strip()
    iot_device = (iot_device or "All").strip()
    limit = max(50, min(int(limit or 500), 2000))

    inner_where = f"""
        FROM `{FACTS_REALTIME_LOGS_TABLE}` f
        LEFT JOIN `{DIM_MACHINE_MAPPER_TABLE}` mm
          ON mm.Device_ID = SAFE_CAST(f.device_id AS INT64)
        WHERE 1 = 1
          AND f.publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    """
    params = []
    if iot_unit and iot_unit != "All":
        inner_where += " AND TRIM(CAST(mm.Unit AS STRING)) = @iot_unit"
        params.append(bigquery.ScalarQueryParameter("iot_unit", "STRING", iot_unit))
    if iot_machine_type and iot_machine_type != "All":
        inner_where += f" AND ({_IOT_DEPARTMENT_FROM_MACHINE_NO_SQL}) = @iot_machine_type"
        params.append(bigquery.ScalarQueryParameter("iot_machine_type", "STRING", iot_machine_type))
    if iot_device and iot_device != "All":
        inner_where += " AND CAST(f.device_id AS STRING) = @iot_device"
        params.append(bigquery.ScalarQueryParameter("iot_device", "STRING", iot_device))

    base_cols = """
            f.publish_time,
            f.publish_time_ist,
            f.device_id,
            f.wifi_mac,
            f.wifi_ip,
            f.wifi_status,
            f.wifi_rssi_dbm,
            f.wifi_disconnect_count,
            f.wifi_reconnect_count,
            f.uptime_ms,
            f.boot_count,
            f.reset_reason,
            f.scheduled_reset_morning_ok,
            f.scheduled_restart_morning_ok,
            f.scheduled_reset_evening_ok,
            f.scheduled_restart_evening_ok,
            f.free_heap_bytes,
            f.min_free_heap_bytes,
            f.loop_time_ms_avg,
            f.loop_time_ms_max,
            f.error_code,
            f.error_source,
            f.error_msg,
            f.last_error_epoch,
            f.error_count_today,
            f.chip_temp_c,
            f.i2c_lcd_0x27_present,
            f.i2c_lcd_probe_fail_count,
            f.i2c_garbage_suspected,
            f.measurement
    """
    query = f"""
        WITH iot_rt_base AS (
            SELECT {base_cols}
            {inner_where}
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY device_id ORDER BY publish_time DESC, measurement DESC
                ) AS iot_rn
            FROM iot_rt_base
        )
        SELECT
            l.publish_time_ist,
            l.device_id,
            CASE
                WHEN p.publish_time IS NOT NULL AND l.publish_time = p.publish_time THEN 'Stopped'
                ELSE 'Running'
            END AS iot_status,
            l.wifi_mac,
            l.wifi_ip,
            l.wifi_status,
            l.wifi_rssi_dbm,
            l.wifi_disconnect_count,
            l.wifi_reconnect_count,
            l.uptime_ms,
            l.boot_count,
            l.reset_reason,
            l.scheduled_reset_morning_ok,
            l.scheduled_restart_morning_ok,
            l.scheduled_reset_evening_ok,
            l.scheduled_restart_evening_ok,
            l.free_heap_bytes,
            l.min_free_heap_bytes,
            l.loop_time_ms_avg,
            l.loop_time_ms_max,
            l.error_code,
            l.error_source,
            l.error_msg,
            l.last_error_epoch,
            l.error_count_today,
            l.chip_temp_c,
            l.i2c_lcd_0x27_present,
            l.i2c_lcd_probe_fail_count,
            l.i2c_garbage_suspected
        FROM ranked l
        LEFT JOIN ranked p
            ON l.device_id = p.device_id AND p.iot_rn = 2
        WHERE l.iot_rn = 1
        ORDER BY l.device_id ASC
        LIMIT {int(limit)}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    try:
        result = get_bq_client().query(query, job_config=job_config).result()
        rows = []
        for row in result:
            raw = {k: row.get(k) for k in IOT_REALTIME_LOG_BQ_KEYS}
            display = {k: _iot_realtime_log_cell(raw[k]) for k in IOT_REALTIME_LOG_BQ_KEYS}
            display[IOT_REALTIME_LEVELS_KEY] = _iot_realtime_levels_for_row(raw)
            rows.append(display)
        return rows
    except Exception as e:
        app.logger.warning("BigQuery fetch_iot_realtime_logs_table failed: %s", e)
        return []


def _iot_realtime_summary_stats(rows: list) -> dict[str, int]:
    """Counts for IoT realtime cards from the same rows shown in the table."""
    total = len(rows)
    running = stopped = 0
    warning_rows = 0
    error_rows = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        st = str(row.get("iot_status", "")).strip()
        if st == "Running":
            running += 1
        elif st == "Stopped":
            stopped += 1
        levels = row.get(IOT_REALTIME_LEVELS_KEY) or {}
        if not isinstance(levels, dict):
            levels = {}
        if any(v == "warn" for v in levels.values()):
            warning_rows += 1
        if any(v == "error" for v in levels.values()):
            error_rows += 1
    return {
        "total": total,
        "running": running,
        "stopped": stopped,
        "warnings": warning_rows,
        "errors": error_rows,
    }


def fetch_monthly_planner(plan_month: str | None = None, department: str | None = None):
    """Fetch rows from monthly planner table, optionally filtered by plan_month (yyyy-mm) and department."""
    if get_bq_client() is None:
        return []

    base_query = f"""
        SELECT
            plan_id,
            plan_month,
            department,
            part_no,
            part_name,
            schedule,
            opening_qty,
            balance_to_be_produced,
            priority,
            allocated,
            IFNULL(produced, 0) AS produced
        FROM `{MONTHLY_PLANNER_TABLE}`
    """
    params = []
    where_clauses = []
    if plan_month:
        where_clauses.append("plan_month = @plan_month")
        params.append(bigquery.ScalarQueryParameter("plan_month", "STRING", plan_month))
    if department:
        where_clauses.append("department = @department")
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    base_query += " ORDER BY plan_id"

    job_config = None
    if params:
        job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        result = get_bq_client().query(base_query, job_config=job_config).result()
        return [
            {
                "plan_id": row["plan_id"],
                "month": row["plan_month"],
                "department": row["department"],
                "part_no": row["part_no"],
                "part_name": row["part_name"],
                "schedule": row["schedule"],
                "opening_qty": row["opening_qty"],
                "balance_to_be_produced": row["balance_to_be_produced"],
                "priority": row["priority"],
                "allocated": row["allocated"] if row["allocated"] is not None else 0,
                "produced": row["produced"] if row["produced"] is not None else 0,
            }
            for row in result
        ]
    except Exception as e:
        app.logger.warning("BigQuery fetch_monthly_planner failed: %s", e)
        return []


def _get_next_plan_id():
    """Return next plan_id as MAX(plan_id)+1."""
    if get_bq_client() is None:
        return None
    query = f"SELECT IFNULL(MAX(plan_id), 0) AS max_id FROM `{MONTHLY_PLANNER_TABLE}`"
    try:
        row = next(get_bq_client().query(query).result(), None)
        max_id = row["max_id"] if row and row["max_id"] is not None else 0
        return int(max_id) + 1
    except Exception as e:
        app.logger.warning("BigQuery _get_next_plan_id failed: %s", e)
        return None


def _get_part_by_part_no(part_no: str):
    """Return dict with part_no, part_name (and other fields) for part_no or None. Table uses part_no as key (no part_id)."""
    if get_bq_client() is None or not part_no:
        return None
    query = f"""SELECT part_no, part_name, department, components_in_fixture, cycle_time_sec, qty_per_hour
        FROM `{PARTS_TABLE}` WHERE part_no = @part_no LIMIT 1"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("part_no", "STRING", part_no)]
    )
    try:
        row = next(get_bq_client().query(query, job_config=job_config).result(), None)
        if not row:
            return None
        return {
            "id": row["part_no"],
            "part_no": row["part_no"],
            "part_name": row["part_name"],
            "department": (row.get("department") or "").strip() or "",
            "components_in_fixture": row.get("components_in_fixture"),
            "cycle_time_sec": row.get("cycle_time_sec"),
            "qty_per_hour": row.get("qty_per_hour"),
        }
    except Exception as e:
        app.logger.warning("BigQuery _get_part_by_part_no failed: %s", e)
        return None


def _get_part_id_by_part_no(part_no: str):
    """Return part_no for given part_no (for compatibility; table has no part_id)."""
    return part_no if part_no else None


def _get_plan_by_id(plan_id: int):
    """Return single monthly plan dict by plan_id or None."""
    if get_bq_client() is None or plan_id is None:
        return None
    query = f"""
        SELECT plan_id, plan_month, department, part_no, part_name, schedule, opening_qty,
               balance_to_be_produced, priority, allocated, IFNULL(produced, 0) AS produced
        FROM `{MONTHLY_PLANNER_TABLE}` WHERE plan_id = @plan_id LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("plan_id", "INT64", plan_id)]
    )
    try:
        row = next(get_bq_client().query(query, job_config=job_config).result(), None)
        if not row:
            return None
        return {
            "plan_id": row["plan_id"],
            "plan_month": row["plan_month"],
            "department": row["department"],
            "part_no": row["part_no"],
            "part_name": row["part_name"],
            "schedule": row["schedule"],
            "opening_qty": row["opening_qty"],
            "balance_to_be_produced": row["balance_to_be_produced"],
            "priority": row["priority"],
            "allocated": row["allocated"] if row["allocated"] is not None else 0,
            "produced": row["produced"] if row["produced"] is not None else 0,
        }
    except Exception as e:
        app.logger.warning("BigQuery _get_plan_by_id failed: %s", e)
        return None


def fetch_machines(department: str | None = None, unit: str | None = None):
    """Fetch machines from fact_job_allocator, optionally filtered by department and unit."""
    if get_bq_client() is None:
        return []
    base_query = f"""
        SELECT machine_no, unit, department
        FROM `{JOB_ALLOCATOR_TABLE}`
    """
    params = []
    where_clauses = []
    if department:
        where_clauses.append("department = @department")
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))
    if unit:
        where_clauses.append("unit = @unit")
        params.append(bigquery.ScalarQueryParameter("unit", "STRING", unit))
    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    base_query += " ORDER BY department, unit, machine_no"

    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    try:
        result = get_bq_client().query(base_query, job_config=job_config).result()
        return [
            {
                "machine_no": row["machine_no"],
                "unit": row["unit"],
                "department": row["department"],
            }
            for row in result
        ]
    except Exception as e:
        app.logger.warning("BigQuery fetch_machines failed: %s", e)
        return []


def _format_timestamp_ist(ts):
    """Format a datetime (UTC, naive or aware) as IST string for display."""
    if ts is None:
        return ""
    try:
        if ZoneInfo is not None:
            ist = ZoneInfo("Asia/Kolkata")
            if getattr(ts, "tzinfo", None) is None:
                ts = ts.replace(tzinfo=ZoneInfo("UTC"))
            local = ts.astimezone(ist)
            return local.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    if hasattr(ts, "strftime"):
        return ts.strftime("%Y-%m-%d %H:%M")
    return str(ts)


def fetch_job_allocations(department: str | None = None, unit: str | None = None):
    """Fetch latest job allocation row per machine_no (fact-style history; show only last updated).
    Filtered by department and unit. job_created_at is returned formatted in IST.
    """
    if get_bq_client() is None:
        return []
    base_query = """
        SELECT part_no, plan, produced, shift_allocated, consumed_shift, job_created_at, machine_no,
               back_up_part_no, back_up_schedule
        FROM (
            SELECT part_no, plan, produced, shift_allocated, consumed_shift, job_created_at, machine_no,
                   back_up_part_no, back_up_schedule,
                   ROW_NUMBER() OVER (PARTITION BY machine_no ORDER BY job_created_at DESC) AS rn
            FROM `{table}`
            WHERE 1=1
    """.format(table=JOB_ALLOCATOR_TABLE)
    params = []
    if department:
        base_query += " AND LOWER(TRIM(COALESCE(department, ''))) = LOWER(TRIM(@department))"
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))
    if unit:
        base_query += " AND LOWER(TRIM(COALESCE(unit, ''))) = LOWER(TRIM(@unit))"
        params.append(bigquery.ScalarQueryParameter("unit", "STRING", unit))
    base_query += """
        ) t
        WHERE rn = 1
        ORDER BY machine_no
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    try:
        result = get_bq_client().query(base_query, job_config=job_config).result()
    except Exception as e1:
        app.logger.warning("BigQuery fetch_job_allocations failed: %s", e1)
        return []

    rows = []
    for row in result:
        r = dict(row) if hasattr(row, "keys") else row
        def _v(k):
            return r.get(k) or r.get(k.lower()) or r.get(k.upper())
        jca = _v("job_created_at")
        jca_str = _format_timestamp_ist(jca) if jca else ""
        rows.append(
            {
                "part_no": _v("part_no"),
                "plan": _v("plan"),
                "produced": _v("produced"),
                "shift_allocated": _v("shift_allocated"),
                "consumed_shift": _v("consumed_shift"),
                "job_created_at": jca_str,
                "machine_no": _v("machine_no"),
                "back_up_part_no": _v("back_up_part_no"),
                "back_up_schedule": _v("back_up_schedule"),
            }
        )
    return rows


def fetch_department_job_allocations(selected_tab: str):
    """Fetch latest allocated jobs per machine for the selected department tab."""
    if get_bq_client() is None:
        return []

    tab = (selected_tab or "PDC").strip().upper()
    department_map = {
        "PDC": ["PDC"],
        "CNC": ["CNC"],
        "SEC": ["SEC"],
        "FET": ["FET", "FETTLING"],
    }
    departments = [d.lower() for d in department_map.get(tab, ["PDC"])]

    query = f"""
        SELECT machine_no, part_no, plan, produced, shift_allocated, job_created_at,
               back_up_part_no, back_up_schedule,
               EXISTS (
                   SELECT 1
                   FROM `{PLAN_CHANGE_REQUEST_TABLE}` r
                   WHERE r.machine_no = t.machine_no
                     AND r.from_part_no = t.part_no
                     AND r.to_part_no = t.back_up_part_no
                     AND r.approval_flag = 0
               ) AS has_pending_switch
        FROM (
            SELECT
                machine_no,
                part_no,
                plan,
                produced,
                shift_allocated,
                job_created_at,
                back_up_part_no,
                back_up_schedule,
                ROW_NUMBER() OVER (PARTITION BY machine_no ORDER BY job_created_at DESC) AS rn
            FROM `{JOB_ALLOCATOR_TABLE}`
            WHERE LOWER(TRIM(COALESCE(department, ''))) IN UNNEST(@departments)
              AND TRIM(COALESCE(part_no, '')) != ''
              AND COALESCE(plan, 0) > 0
        ) t
        WHERE rn = 1
        ORDER BY machine_no
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("departments", "STRING", departments),
        ]
    )
    try:
        result = get_bq_client().query(query, job_config=job_config).result()
    except Exception as e:
        app.logger.warning("BigQuery fetch_department_job_allocations failed: %s", e)
        return []

    rows = []
    for row in result:
        rows.append(
            {
                "machine_no": row.get("machine_no"),
                "part_no": row.get("part_no"),
                "plan": row.get("plan"),
                "produced": row.get("produced") if row.get("produced") is not None else 0,
                "shift_allocated": row.get("shift_allocated"),
                "job_created_at": _format_timestamp_ist(row.get("job_created_at")),
                "back_up_part_no": row.get("back_up_part_no"),
                "back_up_schedule": row.get("back_up_schedule"),
                "has_pending_switch": bool(row.get("has_pending_switch")),
            }
        )
    return rows


def fetch_switch_requests():
    """Fetch switch requests for PPC review."""
    if get_bq_client() is None:
        return []
    query = f"""
        SELECT
            machine_no,
            from_part_no,
            to_part_no,
            requested_at,
            requested_by,
            approval_flag,
            UNIX_MICROS(requested_at) AS requested_at_us
        FROM `{PLAN_CHANGE_REQUEST_TABLE}`
        ORDER BY requested_at DESC
    """
    try:
        result = get_bq_client().query(query).result()
    except Exception as e:
        app.logger.warning("BigQuery fetch_switch_requests failed: %s", e)
        return []

    rows = []
    for row in result:
        approval_flag = row.get("approval_flag")
        status = "Pending"
        if approval_flag == 1:
            status = "Approved"
        elif approval_flag == -1:
            status = "Denied"
        rows.append(
            {
                "machine_no": row.get("machine_no"),
                "from_part_no": row.get("from_part_no"),
                "to_part_no": row.get("to_part_no"),
                "requested_at": _format_timestamp_ist(row.get("requested_at")),
                "requested_by": (row.get("requested_by") or "").strip(),
                "requested_at_us": row.get("requested_at_us"),
                "approval_flag": approval_flag,
                "status": status,
            }
        )
    return rows


def fetch_parts_count(department: str | None = None) -> int:
    """Return total number of parts (for Part Manager pagination), with optional department filter."""
    if get_bq_client() is None:
        return 0
    base_query = f"SELECT COUNT(*) AS n FROM `{PARTS_TABLE}`"
    params = []
    if department:
        base_query += " WHERE LOWER(TRIM(COALESCE(department, ''))) = LOWER(TRIM(@department))"
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))
    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    try:
        row = next(get_bq_client().query(base_query, job_config=job_config).result(), None)
        return int(row["n"]) if row and row["n"] is not None else 0
    except Exception as e:
        app.logger.warning("BigQuery fetch_parts_count failed: %s", e)
        return 0


def fetch_parts(
    department: str | None = None,
    limit: int | None = None,
    offset: int = 0,
):
    """Fetch parts from BigQuery, optionally filtered by department. Optional limit/offset for pagination."""
    if get_bq_client() is None:
        return []
    base_query = f"""
        SELECT
            part_no,
            part_name,
            department,
            components_in_fixture,
            cycle_time_sec,
            qty_per_hour
        FROM `{PARTS_TABLE}`
    """
    params = []
    if department:
        base_query += " WHERE LOWER(TRIM(COALESCE(department, ''))) = LOWER(TRIM(@department))"
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))
    base_query += " ORDER BY part_no"
    if limit is not None:
        # Use literal values; BigQuery can fail with parameterized LIMIT/OFFSET
        base_query += f" LIMIT {int(limit)} OFFSET {int(offset)}"
    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    try:
        result = get_bq_client().query(base_query, job_config=job_config).result()
        return [
            {
                "id": row["part_no"],
                "part_no": row["part_no"],
                "part_name": row["part_name"],
                "department": (row.get("department") or "").strip() or "",
                "components_in_fixture": row["components_in_fixture"],
                "cycle_time_sec": row["cycle_time_sec"],
                "qty_per_hour": row["qty_per_hour"],
            }
            for row in result
        ]
    except Exception as e:
        app.logger.warning("BigQuery fetch_parts failed: %s", e)
        return []


def _part_no_exists(part_no: str, exclude_part_no: str = None) -> bool:
    """Return True if part_no is already used. If exclude_part_no is set, ignore that part (for edit)."""
    if not part_no or get_bq_client() is None:
        return False
    query = f"SELECT 1 FROM `{PARTS_TABLE}` WHERE part_no = @part_no"
    params = [bigquery.ScalarQueryParameter("part_no", "STRING", part_no)]
    if exclude_part_no:
        query += " AND part_no != @exclude_part_no"
        params.append(bigquery.ScalarQueryParameter("exclude_part_no", "STRING", exclude_part_no))
    query += " LIMIT 1"
    try:
        row = next(
            get_bq_client().query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result(),
            None,
        )
        return row is not None
    except Exception as e:
        app.logger.warning("BigQuery _part_no_exists failed: %s", e)
        return False


def _plan_exists(plan_month: str, part_no: str, exclude_plan_id: int | None = None) -> bool:
    """Return True if a plan already exists for given month and part_no."""
    if not plan_month or not part_no or get_bq_client() is None:
        return False
    query = f"SELECT 1 FROM `{MONTHLY_PLANNER_TABLE}` WHERE plan_month = @plan_month AND part_no = @part_no"
    params = [
        bigquery.ScalarQueryParameter("plan_month", "STRING", plan_month),
        bigquery.ScalarQueryParameter("part_no", "STRING", part_no),
    ]
    if exclude_plan_id is not None:
        query += " AND plan_id != @exclude_plan_id"
        params.append(bigquery.ScalarQueryParameter("exclude_plan_id", "INT64", exclude_plan_id))
    query += " LIMIT 1"
    try:
        row = next(
            get_bq_client().query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result(),
            None,
        )
        return row is not None
    except Exception as e:
        app.logger.warning("BigQuery _plan_exists failed: %s", e)
        return False


@app.route("/ppc")
@login_required
def ppc():
    require_page("ppc")
    # Defaults: current year/month for form and filter
    today = date.today()
    default_year = today.year
    default_month = today.month

    # Optional filters for monthly planner (defaults to current month, PDC).
    filter_year_raw = request.args.get("year") or ""
    filter_month_raw = request.args.get("month") or ""
    filter_department_raw = (request.args.get("department") or "PDC").strip().upper()

    filter_year = str(default_year)
    filter_month = str(default_month)
    filter_department = "PDC"
    plan_month_filter = f"{default_year:04d}-{default_month:02d}"

    try:
        if filter_year_raw and filter_month_raw:
            y = int(filter_year_raw)
            m = int(filter_month_raw)
            if y > 0 and 1 <= m <= 12:
                plan_month_filter = f"{y:04d}-{m:02d}"
                filter_year = str(y)
                filter_month = str(m)
    except (TypeError, ValueError):
        # On invalid filter, fall back to current month defaults
        plan_month_filter = f"{default_year:04d}-{default_month:02d}"
        filter_year = str(default_year)
        filter_month = str(default_month)

    # Validate/normalise department (use tabs: PDC, FETTLING, CNC)
    allowed_departments = {"PDC", "CNC", "FETTLING"}
    if filter_department_raw in allowed_departments:
        filter_department = filter_department_raw

    # Daily planner and Part Manager filter args (needed for parallel fetches)
    daily_dept_raw = (request.args.get("daily_dept") or "PDC").strip().upper()
    daily_unit_raw = request.args.get("daily_unit") or "Unit I"
    daily_filter_department = "PDC"
    if daily_dept_raw in allowed_departments:
        daily_filter_department = daily_dept_raw
    allowed_units = ("Unit I", "Unit II")
    daily_filter_unit = "Unit I"
    if daily_unit_raw in allowed_units:
        daily_filter_unit = daily_unit_raw

    part_dept_raw = (request.args.get("part_dept") or "PDC").strip().upper()
    part_filter_department = part_dept_raw if part_dept_raw in allowed_departments else "PDC"
    part_page = max(1, int(request.args.get("part_page") or 1))
    part_per_page = min(100, max(5, int(request.args.get("part_per_page") or 10)))

    # Run independent BigQuery fetches in parallel (7 calls)
    results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(fetch_parts): "parts_all",
            executor.submit(fetch_monthly_planner, plan_month_filter, filter_department): "monthly_plans",
            executor.submit(fetch_machines, daily_filter_department or None, daily_filter_unit): "daily_machines",
            executor.submit(fetch_job_allocations, daily_filter_department or None, daily_filter_unit): "job_allocations",
            executor.submit(fetch_switch_requests): "switch_requests",
            executor.submit(fetch_parts_count, part_filter_department): "parts_count",
            executor.submit(fetch_parts, filter_department, None, 0): "monthly_planner_parts_dropdown",
            executor.submit(fetch_parts, part_filter_department, None, 0): "parts_for_dropdown",
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                app.logger.warning("ppc parallel fetch %s failed: %s", name, e)
                results[name] = [] if name != "parts_count" else 0

    parts_all = results.get("parts_all") or []
    monthly_plans = results.get("monthly_plans") or []
    daily_machines = results.get("daily_machines") or []
    job_allocations = results.get("job_allocations") or []
    switch_requests = results.get("switch_requests") or []
    parts_count = results.get("parts_count") or 0
    monthly_planner_parts_dropdown = results.get("monthly_planner_parts_dropdown") or []
    parts_for_dropdown = results.get("parts_for_dropdown") or []

    part_total_pages = max(1, (parts_count + part_per_page - 1) // part_per_page)
    part_page = min(part_page, part_total_pages)
    parts = fetch_parts(
        department=part_filter_department,
        limit=part_per_page,
        offset=(part_page - 1) * part_per_page,
    )

    # Map part_no -> cycle_time_sec for shift calculations
    part_cycle_map: dict[str, int] = {}
    for part in parts_all:
        pn = (part.get("part_no") or "").strip()
        if pn:
            part_cycle_map[pn] = part.get("cycle_time_sec") or 0

    # Distinct part_no, part_name from monthly planner for Job Allocator dropdown
    seen_part = set()
    monthly_planner_parts = []
    for p in monthly_plans:
        part_no = (p.get("part_no") or "").strip()
        if not part_no or part_no in seen_part:
            continue
        seen_part.add(part_no)
        schedule = p.get("schedule") or 0
        allocated = p.get("allocated") or 0
        try:
            remaining = int(schedule) - int(allocated)
        except (TypeError, ValueError):
            remaining = 0
        if remaining < 0:
            remaining = 0
        cycle_time_sec = part_cycle_map.get(part_no, 0)
        monthly_planner_parts.append(
            {
                "part_no": part_no,
                "part_name": (p.get("part_name") or "").strip(),
                "schedule": schedule,
                "allocated": allocated,
                "remaining": remaining,
                "cycle_time_sec": cycle_time_sec,
            }
        )

    # Part numbers that already have a monthly plan for the current filter month (user can only edit those)
    existing_plan_part_nos = [p.get("part_no") for p in monthly_plans if p.get("part_no")]
    # Parts available for Add Monthly Plan (exclude already planned); for searchable dropdown
    monthly_planner_add_parts = [
        {"id": p.get("part_no"), "part_no": p.get("part_no"), "part_name": (p.get("part_name") or "").strip()}
        for p in monthly_planner_parts_dropdown
        if (p.get("part_no") or "").strip() not in existing_plan_part_nos
    ]

    return render_template(
        "ppc.html",
        active_nav="ppc",
        parts=parts,
        parts_for_dropdown=parts_for_dropdown,
        monthly_planner_parts_dropdown=monthly_planner_parts_dropdown,
        monthly_planner_add_parts=monthly_planner_add_parts,
        part_filter_department=part_filter_department,
        part_page=part_page,
        part_per_page=part_per_page,
        parts_count=parts_count,
        part_total_pages=part_total_pages,
        monthly_plans=monthly_plans,
        existing_plan_part_nos=existing_plan_part_nos,
        default_year=default_year,
        default_month=default_month,
        filter_year=filter_year,
        filter_month=filter_month,
        filter_department=filter_department,
        daily_filter_department=daily_filter_department,
        daily_filter_unit=daily_filter_unit,
        daily_machines=daily_machines,
        job_allocations=job_allocations,
        switch_requests=switch_requests,
        monthly_planner_parts=monthly_planner_parts,
    )


@app.route("/iot")
@login_required
def iot():
    if not IOT_HEALTH_MONITORING_ENABLED:
        flash("IoT Health Monitoring is temporarily disabled.", "info")
        return redirect(url_for("index"))
    if not _user_has_iot_access():
        abort(403)
    tab = (request.args.get("tab") or "realtime").strip().lower()
    if tab not in ("realtime", "history"):
        tab = "realtime"
    iot_unit = (request.args.get("iot_unit") or "All").strip() or "All"
    iot_machine_type = (request.args.get("iot_machine_type") or "All").strip() or "All"
    iot_device = (request.args.get("iot_device") or "All").strip() or "All"
    iot_unit_options, iot_machine_type_options, iot_device_options = [], [], []
    iot_realtime_rows = []
    if tab == "realtime":
        iot_unit_options, iot_machine_type_options, iot_device_options = fetch_iot_realtime_logs_distinct_filters()
        if iot_unit != "All" and iot_unit_options and iot_unit not in iot_unit_options:
            iot_unit = "All"
        if iot_machine_type != "All" and iot_machine_type_options and iot_machine_type not in iot_machine_type_options:
            iot_machine_type = "All"
        if iot_device != "All" and iot_device_options and iot_device not in iot_device_options:
            iot_device = "All"
        iot_realtime_rows = fetch_iot_realtime_logs_table(iot_unit, iot_machine_type, iot_device)
        iot_summary_stats = _iot_realtime_summary_stats(iot_realtime_rows)
    else:
        iot_summary_stats = _iot_realtime_summary_stats([])
    refresh_ist = _iot_now_ist()
    iot_last_refresh_display = refresh_ist.strftime("%d %b %Y, %H:%M:%S IST")
    return render_template(
        "iot.html",
        active_nav="iot",
        iot_tab=tab,
        bq_iot_available=get_bq_client() is not None,
        iot_last_refresh_display=iot_last_refresh_display,
        iot_unit=iot_unit,
        iot_machine_type=iot_machine_type,
        iot_device=iot_device,
        iot_unit_options=iot_unit_options,
        iot_machine_type_options=iot_machine_type_options,
        iot_device_options=iot_device_options,
        iot_realtime_rows=iot_realtime_rows,
        iot_realtime_log_columns=IOT_REALTIME_LOG_COLUMNS_UI,
        iot_summary_stats=iot_summary_stats,
    )


@app.route("/ppc/job-allocator/update-plan", methods=["POST"])
@login_required
def ppc_job_allocator_update_plan():
    """Update plan and job_created_at for a job allocation row.

    Rows are identified by machine_no (plus department/unit); job_id is no longer used.
    """
    require_page("ppc")
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#daily-tab-pane")

    machine_no = (request.form.get("machine_no") or "").strip()
    plan_raw = request.form.get("plan") or ""
    part_no = (request.form.get("part_no") or "").strip()
    daily_dept = request.form.get("daily_dept") or "PDC"
    daily_unit = request.form.get("daily_unit") or "Unit I"
    plan_year_raw = request.form.get("plan_year") or ""
    plan_month_raw = request.form.get("plan_month") or ""
    add_backup_plan_raw = (request.form.get("add_backup_plan") or "").strip()
    add_backup_plan = add_backup_plan_raw in ("1", "true", "on", "yes")
    back_up_part_no_raw = (request.form.get("back_up_part_no") or "").strip()
    back_up_schedule_raw = request.form.get("back_up_schedule") or ""

    if not machine_no:
        flash("Invalid machine.", "danger")
        return redirect(
            url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
        )

    if not part_no:
        flash("Please select a part.", "danger")
        return redirect(
            url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
        )

    try:
        plan_val = int(plan_raw)
    except (TypeError, ValueError):
        flash("Plan must be a number.", "danger")
        return redirect(
            url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
        )

    if plan_val < 0:
        flash("Plan cannot be negative.", "danger")
        return redirect(
            url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
        )

    back_up_part_no_val = None
    back_up_schedule_val = None
    if add_backup_plan:
        if not back_up_part_no_raw:
            flash("Back Up Plan part is required.", "danger")
            return redirect(
                url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
            )
        backup_part = _get_part_by_part_no(back_up_part_no_raw)
        if not backup_part:
            flash("Selected Back Up part not found.", "danger")
            return redirect(
                url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
            )
        back_up_part_no_val = backup_part["part_no"]
        if back_up_part_no_val == part_no:
            flash("Back Up Plan part cannot be the same as the primary part.", "danger")
            return redirect(
                url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit)
                + "#daily-tab-pane"
            )
        try:
            back_up_schedule_val = int(back_up_schedule_raw)
        except (TypeError, ValueError):
            flash("Back Up Plan schedule must be a number.", "danger")
            return redirect(
                url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
            )
        if back_up_schedule_val < 0:
            flash("Back Up Plan schedule cannot be negative.", "danger")
            return redirect(
                url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
            )

    # Determine plan_month (yyyy-mm) for Monthly Planner lookup (defaults to current month/year)
    today = date.today()
    try:
        y = int(plan_year_raw or today.year)
        m = int(plan_month_raw or today.month)
        if y <= 0 or not 1 <= m <= 12:
            raise ValueError
        plan_month_str = f"{y:04d}-{m:02d}"
    except (TypeError, ValueError):
        plan_month_str = f"{today.year:04d}-{today.month:02d}"

    # Enforce plan <= (schedule - allocated) from Monthly Planner for this month/part/department
    remaining_allowed = None
    if get_bq_client() is not None:
        mp_query = f"""
            SELECT schedule, allocated
            FROM `{MONTHLY_PLANNER_TABLE}`
            WHERE plan_month = @plan_month
              AND department = @department
              AND part_no = @part_no
            LIMIT 1
        """
        mp_params = [
            bigquery.ScalarQueryParameter("plan_month", "STRING", plan_month_str),
            bigquery.ScalarQueryParameter("department", "STRING", daily_dept),
            bigquery.ScalarQueryParameter("part_no", "STRING", part_no),
        ]
        mp_job_cfg = bigquery.QueryJobConfig(query_parameters=mp_params)
        try:
            mp_row = next(get_bq_client().query(mp_query, job_config=mp_job_cfg).result(), None)
            if mp_row is not None:
                schedule = mp_row.get("schedule") or 0
                allocated = mp_row.get("allocated") or 0
                try:
                    remaining_allowed = int(schedule) - int(allocated)
                except (TypeError, ValueError):
                    remaining_allowed = 0
                if remaining_allowed < 0:
                    remaining_allowed = 0
        except Exception as e:
            app.logger.warning(
                "BigQuery fetch schedule/allocated for job allocator failed: %s", e
            )

    if remaining_allowed is None:
        # No matching Monthly Planner row found; do not allow over-allocation
        flash("No Monthly Planner entry found for selected month and part.", "danger")
        return redirect(
            url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
        )

    if plan_val > remaining_allowed:
        flash(
            f"Plan cannot exceed remaining ({remaining_allowed} = schedule - allocated).",
            "danger",
        )
        return redirect(
            url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane"
        )

    # Compute shift_allocated from Part Manager qty_per_hour:
    # hours = plan / qty_per_hour, shift = hours / 12 (1 shift = 12 hours)
    shift_required = 0.0
    if get_bq_client() is not None and part_no and plan_val > 0:
        qty_query = f"""
            SELECT qty_per_hour
            FROM `{PARTS_TABLE}`
            WHERE part_no = @part_no
            LIMIT 1
        """
        qty_params = [bigquery.ScalarQueryParameter("part_no", "STRING", part_no)]
        qty_job_cfg = bigquery.QueryJobConfig(query_parameters=qty_params)
        try:
            qty_row = next(get_bq_client().query(qty_query, job_config=qty_job_cfg).result(), None)
            if qty_row is not None:
                qty_per_hour = qty_row.get("qty_per_hour") or 0
                try:
                    qty_val = float(qty_per_hour)
                except (TypeError, ValueError):
                    qty_val = 0.0
                if qty_val > 0:
                    hours = plan_val / qty_val
                    shift_required = round((hours / 11.5) * 100.0) / 100.0  # 2 decimal places
        except Exception as e:
            app.logger.warning(
                "BigQuery fetch qty_per_hour for job allocator failed: %s", e
            )

    # Fetch current plan for this (machine_no, part_no, unit, department) to compute delta for Monthly Planner
    old_plan = None
    if get_bq_client() is not None:
        old_query = f"""
            SELECT plan
            FROM `{JOB_ALLOCATOR_TABLE}`
            WHERE machine_no = @machine_no
              AND part_no = @part_no
              AND LOWER(TRIM(COALESCE(unit, ''))) = LOWER(TRIM(@unit))
              AND LOWER(TRIM(COALESCE(department, ''))) = LOWER(TRIM(@department))
            ORDER BY job_created_at DESC
            LIMIT 1
        """
        old_params = [
            bigquery.ScalarQueryParameter("machine_no", "STRING", machine_no),
            bigquery.ScalarQueryParameter("part_no", "STRING", part_no),
            bigquery.ScalarQueryParameter("unit", "STRING", daily_unit),
            bigquery.ScalarQueryParameter("department", "STRING", daily_dept),
        ]
        try:
            old_row = next(
                get_bq_client().query(
                    old_query,
                    job_config=bigquery.QueryJobConfig(query_parameters=old_params),
                ).result(),
                None,
            )
            if old_row is not None:
                p = old_row.get("plan")
                old_plan = int(p) if p is not None else 0
        except Exception as e:
            app.logger.warning("BigQuery fetch old plan for job allocator failed: %s", e)

    # Delta to add to Monthly Planner allocated: new plan - old plan (or +plan_val if insert)
    allocated_delta = plan_val - (old_plan if old_plan is not None else 0)

    # Same part_no + same machine_no (and unit/department): update existing row; otherwise insert new row
    params = [
        bigquery.ScalarQueryParameter("machine_no", "STRING", machine_no),
        bigquery.ScalarQueryParameter("unit", "STRING", daily_unit),
        bigquery.ScalarQueryParameter("department", "STRING", daily_dept),
        bigquery.ScalarQueryParameter("part_no", "STRING", part_no),
        bigquery.ScalarQueryParameter("plan", "INT64", plan_val),
        bigquery.ScalarQueryParameter("shift_allocated", "FLOAT64", float(shift_required)),
        bigquery.ScalarQueryParameter("back_up_part_no", "STRING", back_up_part_no_val),
        bigquery.ScalarQueryParameter("back_up_schedule", "INT64", back_up_schedule_val),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    update_query = f"""
        UPDATE `{JOB_ALLOCATOR_TABLE}`
        SET plan = @plan,
            shift_allocated = @shift_allocated,
            back_up_part_no = @back_up_part_no,
            back_up_schedule = @back_up_schedule,
            job_created_at = CURRENT_TIMESTAMP()
        WHERE machine_no = @machine_no
          AND part_no = @part_no
          AND LOWER(TRIM(COALESCE(unit, ''))) = LOWER(TRIM(@unit))
          AND LOWER(TRIM(COALESCE(department, ''))) = LOWER(TRIM(@department))
          AND job_created_at = (
            SELECT MAX(job_created_at)
            FROM `{JOB_ALLOCATOR_TABLE}` t2
            WHERE t2.machine_no = @machine_no
              AND t2.part_no = @part_no
              AND LOWER(TRIM(COALESCE(t2.unit, ''))) = LOWER(TRIM(@unit))
              AND LOWER(TRIM(COALESCE(t2.department, ''))) = LOWER(TRIM(@department))
          )
    """
    try:
        update_job = get_bq_client().query(update_query, job_config=job_config)
        update_job.result()
        affected = getattr(update_job, "num_dml_affected_rows", None) or 0
    except Exception as e:
        app.logger.warning("BigQuery update job allocation failed: %s", e)
        affected = 0

    if affected and affected > 0:
        flash("Job allocation updated.", "success")
    else:
        # No row with same machine_no + part_no: insert new row
        insert_query = f"""
            INSERT INTO `{JOB_ALLOCATOR_TABLE}`
            (machine_no, unit, department, part_no, plan, produced, shift_allocated, consumed_shift, job_created_at, back_up_part_no, back_up_schedule)
            VALUES (
                @machine_no,
                @unit,
                @department,
                @part_no,
                @plan,
                0,
                @shift_allocated,
                0,
                CURRENT_TIMESTAMP(),
                @back_up_part_no,
                @back_up_schedule
            )
        """
        try:
            get_bq_client().query(insert_query, job_config=job_config).result()
            flash("Job allocation saved.", "success")
        except Exception as e:
            app.logger.warning("BigQuery insert job allocation failed: %s", e)
            flash("Save failed. Please try again.", "danger")

    # Reflect in Monthly Planner: add allocated_delta to allocated for this month/department/part_no
    if get_bq_client() is not None and allocated_delta != 0:
        mp_update = f"""
            UPDATE `{MONTHLY_PLANNER_TABLE}`
            SET allocated = COALESCE(allocated, 0) + @allocated_delta
            WHERE plan_month = @plan_month
              AND department = @department
              AND part_no = @part_no
        """
        mp_params = [
            bigquery.ScalarQueryParameter("allocated_delta", "INT64", allocated_delta),
            bigquery.ScalarQueryParameter("plan_month", "STRING", plan_month_str),
            bigquery.ScalarQueryParameter("department", "STRING", daily_dept),
            bigquery.ScalarQueryParameter("part_no", "STRING", part_no),
        ]
        try:
            get_bq_client().query(mp_update, job_config=bigquery.QueryJobConfig(query_parameters=mp_params)).result()
        except Exception as e:
            app.logger.warning("BigQuery update monthly planner allocated failed: %s", e)

    return redirect(url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane")


@app.route("/ppc/monthly-planner", methods=["POST"])
@login_required
def ppc_monthly_planner_add():
    require_page("ppc")
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    part_no_raw = (request.form.get("part_id") or "").strip()
    department_raw = (request.form.get("department") or "").strip().upper()
    year_raw = request.form.get("year") or ""
    month_raw = request.form.get("month") or ""
    schedule_raw = request.form.get("schedule") or ""
    opening_qty_raw = request.form.get("opening_qty") or ""
    priority = (request.form.get("priority") or "").strip()

    errors = []
    if not part_no_raw:
        errors.append("Part is required.")

    allowed_departments = {"PDC", "CNC", "FETTLING"}
    if department_raw not in allowed_departments:
        errors.append("Department must be one of PDC, CNC, FETTLING.")
    if priority not in ("1st", "2nd", "3rd"):
        errors.append("Priority must be 1st, 2nd, or 3rd.")

    try:
        year_val = int(year_raw)
    except (TypeError, ValueError):
        errors.append("Year must be an integer.")
        year_val = 0

    try:
        month_val = int(month_raw)
    except (TypeError, ValueError):
        errors.append("Month must be an integer.")
        month_val = 0

    if year_val <= 0:
        errors.append("Year is required.")
    if month_val < 1 or month_val > 12:
        errors.append("Month must be between 1 and 12.")

    try:
        schedule_val = int(schedule_raw)
    except (TypeError, ValueError):
        errors.append("Schedule must be an integer.")
        schedule_val = 0
    try:
        opening_qty_val = int(opening_qty_raw)
    except (TypeError, ValueError):
        errors.append("Opening Qty must be an integer.")
        opening_qty_val = 0

    if errors:
        for e in errors:
            flash(e, "danger")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    part = _get_part_by_part_no(part_no_raw)
    if not part:
        flash("Selected part not found.", "danger")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    balance_to_be_produced = schedule_val - opening_qty_val
    plan_month = f"{year_val:04d}-{month_val:02d}"
    # Prevent duplicate: one plan per part per month/year; user must edit existing plan
    if _plan_exists(plan_month, part["part_no"]):
        flash("A plan already exists for this part in the selected month and year. Use Edit to change it.", "danger")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    allocated_val = 0
    next_plan_id = _get_next_plan_id()
    if next_plan_id is None:
        flash("Could not generate Plan ID.", "danger")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    produced_val = 0
    query = f"""
        INSERT INTO `{MONTHLY_PLANNER_TABLE}` (
            plan_id, plan_month, department, part_no, part_name, schedule, opening_qty,
            balance_to_be_produced, priority, allocated, produced
        )
        VALUES (
            @plan_id, @plan_month, @department, @part_no, @part_name, @schedule, @opening_qty,
            @balance_to_be_produced, @priority, @allocated, @produced
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("plan_id", "INT64", next_plan_id),
            bigquery.ScalarQueryParameter("plan_month", "STRING", plan_month),
            bigquery.ScalarQueryParameter("department", "STRING", department_raw),
            bigquery.ScalarQueryParameter("part_no", "STRING", part["part_no"]),
            bigquery.ScalarQueryParameter("part_name", "STRING", part["part_name"]),
            bigquery.ScalarQueryParameter("schedule", "INT64", schedule_val),
            bigquery.ScalarQueryParameter("opening_qty", "INT64", opening_qty_val),
            bigquery.ScalarQueryParameter("balance_to_be_produced", "INT64", balance_to_be_produced),
            bigquery.ScalarQueryParameter("priority", "STRING", priority),
            bigquery.ScalarQueryParameter("allocated", "INT64", allocated_val),
            bigquery.ScalarQueryParameter("produced", "INT64", produced_val),
        ]
    )
    try:
        get_bq_client().query(query, job_config=job_config).result()
        flash("Monthly plan added successfully.", "success")
    except Exception as exc:
        app.logger.warning("BigQuery monthly planner insert failed: %s", exc)
        flash("Failed to add monthly plan.", "danger")
        flash(f"BigQuery error: {exc}", "danger")

    return redirect(url_for("ppc") + "#monthly-tab-pane")


@app.route("/ppc/monthly-planner/<int:plan_id>/edit", methods=["GET", "POST"])
@login_required
def ppc_edit_monthly_plan(plan_id):
    require_page("ppc")
    plan = _get_plan_by_id(plan_id)
    if not plan:
        flash("Plan not found.", "danger")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    if request.method == "GET":
        parts = fetch_parts()
        current_part_id = _get_part_id_by_part_no(plan["part_no"])
        return render_template(
            "ppc_edit_monthly_plan.html",
            active_nav="ppc",
            plan=plan,
            parts=parts,
            current_part_id=current_part_id,
        )

    # POST: update plan
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))

    part_no_raw = (request.form.get("part_id") or "").strip()
    department_raw = (request.form.get("department") or "").strip().upper()
    year_raw = request.form.get("year") or ""
    month_raw = request.form.get("month") or ""
    schedule_raw = request.form.get("schedule") or ""
    opening_qty_raw = request.form.get("opening_qty") or ""
    priority = (request.form.get("priority") or "").strip()

    if not part_no_raw:
        flash("Invalid part.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))

    part = _get_part_by_part_no(part_no_raw)
    if not part:
        flash("Part not found.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))

    allowed_departments = {"PDC", "CNC", "FETTLING"}
    if department_raw not in allowed_departments:
        flash("Department must be one of PDC, CNC, FETTLING.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))

    try:
        schedule_val = int(schedule_raw)
        opening_qty_val = int(opening_qty_raw)
    except (TypeError, ValueError):
        flash("Schedule and Opening Qty must be whole numbers.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))

    try:
        year_val = int(year_raw)
        month_val = int(month_raw)
    except (TypeError, ValueError):
        flash("Year and Month must be whole numbers.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))

    if schedule_val < 0 or opening_qty_val < 0:
        flash("Schedule and Opening Qty must be non-negative.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))

    if year_val <= 0 or month_val < 1 or month_val > 12:
        flash("Year must be positive and Month must be between 1 and 12.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))

    balance_to_be_produced = max(0, schedule_val - opening_qty_val)
    plan_month = f"{year_val:04d}-{month_val:02d}"
    # Prevent duplicate (year, month, part_no) with another plan
    if _plan_exists(plan_month, part["part_no"], exclude_plan_id=plan_id):
        flash("Another plan already exists for this Year, Month, and Part No.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))
    if priority not in ("1st", "2nd", "3rd"):
        flash("Invalid priority.", "danger")
        return redirect(url_for("ppc_edit_monthly_plan", plan_id=plan_id))

    query = f"""
        UPDATE `{MONTHLY_PLANNER_TABLE}`
        SET part_no = @part_no, part_name = @part_name,
            plan_month = @plan_month,
            department = @department,
            schedule = @schedule, opening_qty = @opening_qty,
            balance_to_be_produced = @balance_to_be_produced, priority = @priority
        WHERE plan_id = @plan_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("plan_id", "INT64", plan_id),
            bigquery.ScalarQueryParameter("part_no", "STRING", part["part_no"]),
            bigquery.ScalarQueryParameter("part_name", "STRING", part["part_name"]),
            bigquery.ScalarQueryParameter("plan_month", "STRING", plan_month),
            bigquery.ScalarQueryParameter("department", "STRING", department_raw),
            bigquery.ScalarQueryParameter("schedule", "INT64", schedule_val),
            bigquery.ScalarQueryParameter("opening_qty", "INT64", opening_qty_val),
            bigquery.ScalarQueryParameter("balance_to_be_produced", "INT64", balance_to_be_produced),
            bigquery.ScalarQueryParameter("priority", "STRING", priority),
        ]
    )
    try:
        get_bq_client().query(query, job_config=job_config).result()
        flash("Monthly plan updated successfully.", "success")
    except Exception as exc:
        app.logger.warning("BigQuery monthly planner update failed: %s", exc)
        flash("Failed to update monthly plan.", "danger")

    return redirect(url_for("ppc") + "#monthly-tab-pane")


@app.route("/ppc/monthly-planner/<int:plan_id>/delete", methods=["POST"])
@login_required
def ppc_delete_monthly_plan(plan_id):
    require_page("ppc")
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    query = f"DELETE FROM `{MONTHLY_PLANNER_TABLE}` WHERE plan_id = @plan_id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("plan_id", "INT64", plan_id)]
    )
    try:
        get_bq_client().query(query, job_config=job_config).result()
        flash("Monthly plan deleted.", "success")
    except Exception as exc:
        app.logger.warning("BigQuery monthly planner delete failed: %s", exc)
        flash("Failed to delete monthly plan.", "danger")

    return redirect(url_for("ppc") + "#monthly-tab-pane")


@app.route("/ppc/monthly-planner/delete", methods=["POST"])
@login_required
def ppc_delete_monthly_plans_bulk():
    """Delete one or more monthly plans by plan_id. Form: plan_ids (list)."""
    require_page("ppc")
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    raw = request.form.getlist("plan_ids") or request.form.get("plan_ids", "").split(",")
    plan_ids = []
    for x in raw:
        x = (x or "").strip()
        if not x:
            continue
        try:
            plan_ids.append(int(x))
        except ValueError:
            continue
    if not plan_ids:
        flash("No plans selected to delete.", "warning")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    # DELETE FROM ... WHERE plan_id IN UNNEST(@plan_ids)
    query = f"DELETE FROM `{MONTHLY_PLANNER_TABLE}` WHERE plan_id IN (SELECT id FROM UNNEST(@plan_ids) AS id)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("plan_ids", "INT64", plan_ids)]
    )
    try:
        get_bq_client().query(query, job_config=job_config).result()
        n = len(plan_ids)
        flash(f"{n} monthly plan(s) deleted." if n > 1 else "Monthly plan deleted.", "success")
    except Exception as exc:
        app.logger.warning("BigQuery monthly planner bulk delete failed: %s", exc)
        flash("Failed to delete monthly plan(s).", "danger")

    return redirect(url_for("ppc") + "#monthly-tab-pane")


def _normalize_part_name(name: str) -> str:
    """Uppercase part name and enforce only A-Z and hyphen."""
    raw = (name or "").strip().upper()
    return raw


@app.route("/ppc/parts", methods=["POST"])
@login_required
def ppc_create_part():
    require_page("ppc")
    allowed_departments = {"PDC", "CNC", "FETTLING"}
    part_no = (request.form.get("part_no") or "").strip()
    part_name_raw = request.form.get("part_name") or ""
    department_raw = (request.form.get("department") or "").strip().upper()
    components = request.form.get("components_in_fixture") or ""
    cycle_time = request.form.get("cycle_time_sec") or ""

    part_name = _normalize_part_name(part_name_raw)

    # Basic validation
    errors = []
    if not part_no:
        errors.append("Part No is required.")
    if not part_name:
        errors.append("Part Name is required.")
    if " " in part_name:
        errors.append("Part Name cannot contain spaces; use hyphen (-) instead.")

    if not re.fullmatch(r"[A-Z0-9-]+", part_name):
        errors.append("Part Name must contain only capital letters, numbers, and hyphens.")

    if department_raw not in allowed_departments:
        errors.append("Department must be one of: PDC, CNC, FETTLING.")

    try:
        components_val = int(components)
        if components_val <= 0:
            errors.append("Component in Fixture must be a positive integer.")
    except (TypeError, ValueError):
        errors.append("Component in Fixture must be a positive integer.")

    try:
        cycle_val = int(cycle_time)
        if cycle_val <= 0:
            errors.append("Cycle Time must be a positive integer.")
    except (TypeError, ValueError):
        errors.append("Cycle Time must be a positive integer.")

    qty_per_hour_raw = request.form.get("qty_per_hour") or ""
    try:
        qty_per_hour = int(qty_per_hour_raw)
        if qty_per_hour < 0:
            errors.append("Qty/Hour must be 0 or more (fill Cycle Time and Component in Fixture to auto-calculate).")
    except (TypeError, ValueError):
        errors.append("Qty/Hour is required (fill Cycle Time and Component in Fixture to auto-calculate).")

    if errors:
        for e in errors:
            flash(e, "danger")
        return redirect(url_for("ppc") + "#part-tab-pane")

    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#part-tab-pane")

    if _part_no_exists(part_no):
        flash("Part No already exists. No duplicate Part No allowed.", "danger")
        return redirect(url_for("ppc") + "#part-tab-pane")

    query = f"""
        INSERT INTO `{PARTS_TABLE}` (
            part_no,
            part_name,
            department,
            components_in_fixture,
            cycle_time_sec,
            qty_per_hour
        )
        VALUES (
            @part_no,
            @part_name,
            @department,
            @components_in_fixture,
            @cycle_time_sec,
            @qty_per_hour
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("part_no", "STRING", part_no),
            bigquery.ScalarQueryParameter("part_name", "STRING", part_name),
            bigquery.ScalarQueryParameter("department", "STRING", department_raw),
            bigquery.ScalarQueryParameter(
                "components_in_fixture", "INT64", components_val
            ),
            bigquery.ScalarQueryParameter("cycle_time_sec", "INT64", cycle_val),
            bigquery.ScalarQueryParameter("qty_per_hour", "INT64", qty_per_hour),
        ]
    )

    try:
        get_bq_client().query(query, job_config=job_config).result()
        flash("Part created successfully.", "success")
    except Exception as exc:
        app.logger.warning("BigQuery insert part failed: %s", exc)
        flash("Failed to create part. Ensure Part Name is unique.", "danger")
        flash(f"BigQuery error: {exc}", "danger")

    return redirect(url_for("ppc") + "#part-tab-pane")


@app.route("/ppc/parts/<part_no>/delete", methods=["POST"])
@login_required
def ppc_delete_part(part_no: str):
    require_page("ppc")
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#part-tab-pane")

    query = f"DELETE FROM `{PARTS_TABLE}` WHERE part_no = @part_no"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("part_no", "STRING", part_no),
        ]
    )
    try:
        get_bq_client().query(query, job_config=job_config).result()
        flash("Part deleted successfully.", "success")
    except Exception as exc:
        app.logger.warning("BigQuery delete part %s failed: %s", part_no, exc)
        flash("Failed to delete part.", "danger")
        flash(f"BigQuery error: {exc}", "danger")
    return redirect(url_for("ppc") + "#part-tab-pane")


@app.route("/ppc/parts/<part_no>/edit", methods=["GET", "POST"])
@login_required
def ppc_edit_part(part_no: str):
    require_page("ppc")
    allowed_departments = {"PDC", "CNC", "FETTLING"}
    if request.method == "POST":
        part_no_new = (request.form.get("part_no") or "").strip()
        part_name_raw = request.form.get("part_name") or ""
        department_raw = (request.form.get("department") or "").strip().upper()
        components = request.form.get("components_in_fixture") or ""
        cycle_time = request.form.get("cycle_time_sec") or ""

        part_name = _normalize_part_name(part_name_raw)

        errors = []
        if not part_no_new:
            errors.append("Part No is required.")
        if not part_name:
            errors.append("Part Name is required.")
        if " " in part_name:
            errors.append("Part Name cannot contain spaces; use hyphen (-) instead.")

        if not re.fullmatch(r"[A-Z0-9-]+", part_name):
            errors.append("Part Name must contain only capital letters, numbers, and hyphens.")

        if department_raw not in allowed_departments:
            errors.append("Department must be one of: PDC, CNC, FETTLING.")

        try:
            components_val = int(components)
            if components_val <= 0:
                errors.append("Component in Fixture must be a positive integer.")
        except (TypeError, ValueError):
            errors.append("Component in Fixture must be a positive integer.")

        try:
            cycle_val = int(cycle_time)
            if cycle_val <= 0:
                errors.append("Cycle Time must be a positive integer.")
        except (TypeError, ValueError):
            errors.append("Cycle Time must be a positive integer.")

        qty_per_hour_raw = request.form.get("qty_per_hour") or ""
        try:
            qty_per_hour = int(qty_per_hour_raw)
            if qty_per_hour < 0:
                errors.append("Qty/Hour must be 0 or more (fill Cycle Time and Component in Fixture to auto-calculate).")
        except (TypeError, ValueError):
            errors.append("Qty/Hour is required (fill Cycle Time and Component in Fixture to auto-calculate).")

        if errors:
            for e in errors:
                flash(e, "danger")
            return redirect(url_for("ppc_edit_part", part_no=part_no))

        if get_bq_client() is None:
            flash("BigQuery is not configured.", "danger")
            return redirect(url_for("ppc_edit_part", part_no=part_no))

        if _part_no_exists(part_no_new, exclude_part_no=part_no):
            flash("Part No already exists. No duplicate Part No allowed.", "danger")
            return redirect(url_for("ppc_edit_part", part_no=part_no))

        query = f"""
            UPDATE `{PARTS_TABLE}`
            SET
                part_no = @part_no,
                part_name = @part_name,
                department = @department,
                components_in_fixture = @components_in_fixture,
                cycle_time_sec = @cycle_time_sec,
                qty_per_hour = @qty_per_hour
            WHERE part_no = @current_part_no
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("part_no", "STRING", part_no_new),
                bigquery.ScalarQueryParameter("part_name", "STRING", part_name),
                bigquery.ScalarQueryParameter("department", "STRING", department_raw),
                bigquery.ScalarQueryParameter(
                    "components_in_fixture", "INT64", components_val
                ),
                bigquery.ScalarQueryParameter("cycle_time_sec", "INT64", cycle_val),
                bigquery.ScalarQueryParameter("qty_per_hour", "INT64", qty_per_hour),
                bigquery.ScalarQueryParameter("current_part_no", "STRING", part_no),
            ]
        )

        try:
            get_bq_client().query(query, job_config=job_config).result()
            flash("Part updated successfully.", "success")
            return redirect(url_for("ppc") + "#part-tab-pane")
        except Exception as exc:
            app.logger.warning("BigQuery update part %s failed: %s", part_no, exc)
            flash("Failed to update part. Ensure Part Name is unique.", "danger")
            flash(f"BigQuery error: {exc}", "danger")
            return redirect(url_for("ppc_edit_part", part_no=part_no))

    # GET
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#part-tab-pane")

    part = _get_part_by_part_no(part_no)
    if not part:
        flash("Part not found.", "danger")
        return redirect(url_for("ppc") + "#part-tab-pane")
    part["department"] = part.get("department") or None
    return render_template("ppc_edit_part.html", active_nav="ppc", part=part)


@app.route("/consumables")
@login_required
def consumables():
    require_page("consumables")
    return render_template("under_development.html", active_nav="consumables")


@app.route("/department")
@login_required
def department():
    """Legacy URL: Department view now lives under Production → Plan."""
    require_page("ppc")
    selected_tab = (request.args.get("tab") or "PDC").strip().upper()
    allowed_tabs = ("PDC", "FET", "CNC", "SEC")
    if selected_tab not in allowed_tabs:
        selected_tab = "PDC"
    return redirect(url_for("index", planTab=selected_tab) + "#plan-tab-pane")


@app.route("/department/switch-request", methods=["POST"])
@login_required
def department_switch_request():
    # Reuse PPC permission so existing PPC users can access this action.
    require_page("ppc")
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("index", planTab="PDC") + "#plan-tab-pane")

    machine_no = (request.form.get("machine_no") or "").strip()
    from_part_no = (request.form.get("from_part_no") or "").strip()
    to_part_no = (request.form.get("to_part_no") or "").strip()
    selected_tab = (request.form.get("tab") or "PDC").strip().upper()
    allowed_tabs = {"PDC", "FET", "CNC", "SEC"}
    if selected_tab not in allowed_tabs:
        selected_tab = "PDC"

    if not machine_no or not from_part_no or not to_part_no:
        flash("Invalid switch request data.", "danger")
        return redirect(url_for("index", planTab=selected_tab) + "#plan-tab-pane")

    if from_part_no == to_part_no:
        flash("Back Up plan must be different from primary plan.", "danger")
        return redirect(url_for("index", planTab=selected_tab) + "#plan-tab-pane")

    duplicate_query = f"""
        SELECT 1
        FROM `{PLAN_CHANGE_REQUEST_TABLE}`
        WHERE machine_no = @machine_no
          AND from_part_no = @from_part_no
          AND to_part_no = @to_part_no
          AND approval_flag = 0
        LIMIT 1
    """
    duplicate_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("machine_no", "STRING", machine_no),
            bigquery.ScalarQueryParameter("from_part_no", "STRING", from_part_no),
            bigquery.ScalarQueryParameter("to_part_no", "STRING", to_part_no),
        ]
    )
    try:
        duplicate_row = next(
            get_bq_client().query(duplicate_query, job_config=duplicate_cfg).result(),
            None,
        )
        if duplicate_row is not None:
            flash("A pending switch request already exists for this machine and parts.", "warning")
            return redirect(url_for("index", planTab=selected_tab) + "#plan-tab-pane")
    except Exception as e:
        app.logger.warning("BigQuery duplicate switch request check failed: %s", e)

    query = f"""
        INSERT INTO `{PLAN_CHANGE_REQUEST_TABLE}`
        (machine_no, from_part_no, to_part_no, requested_at, requested_by, approval_flag)
        VALUES (@machine_no, @from_part_no, @to_part_no, CURRENT_TIMESTAMP(), @requested_by, 0)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("machine_no", "STRING", machine_no),
            bigquery.ScalarQueryParameter("from_part_no", "STRING", from_part_no),
            bigquery.ScalarQueryParameter("to_part_no", "STRING", to_part_no),
            bigquery.ScalarQueryParameter("requested_by", "STRING", (current_user.email or "").strip()),
        ]
    )
    try:
        get_bq_client().query(query, job_config=job_config).result()
        flash("Switch request raised.", "success")
    except Exception as e:
        app.logger.warning("BigQuery insert switch request failed: %s", e)
        flash("Failed to raise switch request.", "danger")

    return redirect(url_for("index", planTab=selected_tab) + "#plan-tab-pane")


@app.route("/ppc/switch-request/approve", methods=["POST"])
@login_required
def ppc_approve_switch_request():
    require_page("ppc")
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#switch-request-tab-pane")

    machine_no = (request.form.get("machine_no") or "").strip()
    from_part_no = (request.form.get("from_part_no") or "").strip()
    to_part_no = (request.form.get("to_part_no") or "").strip()
    requested_at_us_raw = (request.form.get("requested_at_us") or "").strip()
    try:
        requested_at_us = int(requested_at_us_raw)
    except (TypeError, ValueError):
        requested_at_us = None

    if not machine_no or not from_part_no or not to_part_no or requested_at_us is None:
        flash("Invalid switch request.", "danger")
        return redirect(url_for("ppc") + "#switch-request-tab-pane")

    latest_query = f"""
        SELECT machine_no, unit, department, part_no, back_up_part_no, back_up_schedule
        FROM (
            SELECT
                machine_no, unit, department, part_no, back_up_part_no, back_up_schedule,
                ROW_NUMBER() OVER (PARTITION BY machine_no ORDER BY job_created_at DESC) AS rn
            FROM `{JOB_ALLOCATOR_TABLE}`
            WHERE machine_no = @machine_no
        ) t
        WHERE rn = 1
        LIMIT 1
    """
    latest_cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("machine_no", "STRING", machine_no)]
    )
    latest_row = None
    try:
        latest_row = next(get_bq_client().query(latest_query, job_config=latest_cfg).result(), None)
    except Exception as e:
        app.logger.warning("BigQuery fetch latest allocation for approve failed: %s", e)

    if not latest_row:
        flash("Latest machine allocation not found.", "danger")
        return redirect(url_for("ppc") + "#switch-request-tab-pane")

    current_part_no = (latest_row.get("part_no") or "").strip()
    backup_part_no = (latest_row.get("back_up_part_no") or "").strip()
    backup_schedule = latest_row.get("back_up_schedule")
    machine_unit = latest_row.get("unit")
    machine_department = latest_row.get("department")

    if current_part_no != from_part_no or backup_part_no != to_part_no or backup_schedule is None:
        flash("Switch request no longer matches current backup plan.", "danger")
        return redirect(url_for("ppc") + "#switch-request-tab-pane")

    try:
        new_plan = int(backup_schedule)
    except (TypeError, ValueError):
        flash("Invalid backup schedule for switch.", "danger")
        return redirect(url_for("ppc") + "#switch-request-tab-pane")
    if new_plan <= 0:
        flash("Backup schedule must be greater than zero.", "danger")
        return redirect(url_for("ppc") + "#switch-request-tab-pane")

    qty_query = f"""
        SELECT qty_per_hour
        FROM `{PARTS_TABLE}`
        WHERE part_no = @part_no
        LIMIT 1
    """
    qty_cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("part_no", "STRING", to_part_no)]
    )
    qty_row = None
    try:
        qty_row = next(get_bq_client().query(qty_query, job_config=qty_cfg).result(), None)
    except Exception as e:
        app.logger.warning("BigQuery fetch qty_per_hour for approve failed: %s", e)
    qty_per_hour = float((qty_row or {}).get("qty_per_hour") or 0)
    shift_allocated = 0.0
    if qty_per_hour > 0:
        shift_allocated = round(((new_plan / qty_per_hour) / 11.5) * 100.0) / 100.0

    insert_query = f"""
        INSERT INTO `{JOB_ALLOCATOR_TABLE}`
        (machine_no, unit, department, part_no, plan, produced, shift_allocated, consumed_shift, job_created_at, back_up_part_no, back_up_schedule)
        VALUES (
            @machine_no,
            @unit,
            @department,
            @part_no,
            @plan,
            0,
            @shift_allocated,
            0,
            CURRENT_TIMESTAMP(),
            NULL,
            NULL
        )
    """
    insert_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("machine_no", "STRING", machine_no),
            bigquery.ScalarQueryParameter("unit", "STRING", machine_unit),
            bigquery.ScalarQueryParameter("department", "STRING", machine_department),
            bigquery.ScalarQueryParameter("part_no", "STRING", to_part_no),
            bigquery.ScalarQueryParameter("plan", "INT64", new_plan),
            bigquery.ScalarQueryParameter("shift_allocated", "FLOAT64", shift_allocated),
        ]
    )
    try:
        get_bq_client().query(insert_query, job_config=insert_cfg).result()
    except Exception as e:
        app.logger.warning("BigQuery insert switched allocation failed: %s", e)
        flash("Failed to apply switch.", "danger")
        return redirect(url_for("ppc") + "#switch-request-tab-pane")

    update_query = f"""
        UPDATE `{PLAN_CHANGE_REQUEST_TABLE}`
        SET approval_flag = 1
        WHERE machine_no = @machine_no
          AND from_part_no = @from_part_no
          AND to_part_no = @to_part_no
          AND UNIX_MICROS(requested_at) = @requested_at_us
          AND approval_flag = 0
    """
    update_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("machine_no", "STRING", machine_no),
            bigquery.ScalarQueryParameter("from_part_no", "STRING", from_part_no),
            bigquery.ScalarQueryParameter("to_part_no", "STRING", to_part_no),
            bigquery.ScalarQueryParameter("requested_at_us", "INT64", requested_at_us),
        ]
    )
    try:
        get_bq_client().query(update_query, job_config=update_cfg).result()
        flash("Switch request approved and job card updated.", "success")
    except Exception as e:
        app.logger.warning("BigQuery approve switch request update failed: %s", e)
        flash("Approved switch but failed to update request status.", "warning")

    return redirect(url_for("ppc") + "#switch-request-tab-pane")


@app.route("/ppc/switch-request/deny", methods=["POST"])
@login_required
def ppc_deny_switch_request():
    require_page("ppc")
    if get_bq_client() is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#switch-request-tab-pane")

    machine_no = (request.form.get("machine_no") or "").strip()
    from_part_no = (request.form.get("from_part_no") or "").strip()
    to_part_no = (request.form.get("to_part_no") or "").strip()
    requested_at_us_raw = (request.form.get("requested_at_us") or "").strip()
    try:
        requested_at_us = int(requested_at_us_raw)
    except (TypeError, ValueError):
        requested_at_us = None

    if not machine_no or not from_part_no or not to_part_no or requested_at_us is None:
        flash("Invalid switch request.", "danger")
        return redirect(url_for("ppc") + "#switch-request-tab-pane")

    query = f"""
        UPDATE `{PLAN_CHANGE_REQUEST_TABLE}`
        SET approval_flag = -1
        WHERE machine_no = @machine_no
          AND from_part_no = @from_part_no
          AND to_part_no = @to_part_no
          AND UNIX_MICROS(requested_at) = @requested_at_us
          AND approval_flag = 0
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("machine_no", "STRING", machine_no),
            bigquery.ScalarQueryParameter("from_part_no", "STRING", from_part_no),
            bigquery.ScalarQueryParameter("to_part_no", "STRING", to_part_no),
            bigquery.ScalarQueryParameter("requested_at_us", "INT64", requested_at_us),
        ]
    )
    try:
        get_bq_client().query(query, job_config=job_config).result()
        flash("Switch request denied.", "success")
    except Exception as e:
        app.logger.warning("BigQuery deny switch request failed: %s", e)
        flash("Failed to deny switch request.", "danger")

    return redirect(url_for("ppc") + "#switch-request-tab-pane")


@app.route("/maintenance")
@login_required
def maintenance():
    require_page("maintenance")
    return redirect(url_for("logistics", tab="maintenance"))


PPC_SECTION_TABS = (
    ("logistics", "Logistics"),
    ("maintenance", "Maintenance"),
)


def _ppc_tabs_for_user() -> tuple[tuple[str, str], ...]:
    return tuple(
        (key, label)
        for key, label in PPC_SECTION_TABS
        if _user_can_access_page(key)
    )


def _user_can_access_page(page_key: str) -> bool:
    if not current_user.is_authenticated:
        return False
    role = (getattr(current_user, "role", "") or "").strip().lower()
    if role in ("admin", "editor"):
        return True
    pages = getattr(current_user, "allowed_pages", None) or []
    return page_key in pages


def _render_ppc_page(tab: str):
    tabs = _ppc_tabs_for_user()
    allowed = {k for k, _ in tabs}
    if not allowed:
        abort(403)
    tab = (tab or "logistics").strip().lower()
    if tab not in allowed:
        tab = tabs[0][0]
    require_page(tab)
    ist_today = _ist_today_date()
    selected_day = _parse_security_table_date(request.args.get("date"), ist_today)
    vehicle_requests = []
    firestore_error = None
    if tab == "logistics":
        vehicle_requests, firestore_error = fetch_logistics_vehicle_requests(
            ist_day=selected_day,
            jmd_route_filter=None,
        )
    return render_template(
        "ppc.html",
        active_nav="ppc_hub",
        ppc_tabs=tabs,
        selected_tab=tab,
        vehicle_requests=vehicle_requests,
        firestore_error=firestore_error,
        selected_date_iso=selected_day.strftime("%Y-%m-%d"),
        can_delete_requests=_user_is_admin(),
    )


@app.route("/documents")
@login_required
def documents():
    require_page("documents")
    return render_template("under_development.html", active_nav="documents")


SECURITY_TABS = (
    ("on-duty", "OD Request"),
    ("visitor-request", "Visitor Request"),
    ("vehicle-request", "Vehicle Request"),
)

HR_TABS = (
    ("leave-request", "Leave Request"),
)

IT_TABS = (
    ("it-request", "IT Request"),
)


def _render_requests_page(
    page_key: str,
    active_nav: str,
    tabs: tuple[tuple[str, str], ...],
    default_tab: str,
):
    require_page(page_key)
    tab = (request.args.get("tab") or default_tab).strip().lower()
    allowed_tabs = {k for k, _ in tabs}
    if tab not in allowed_tabs:
        tab = default_tab
    ist_today = _ist_today_date()
    selected_day = _parse_security_table_date(request.args.get("date"), ist_today)
    selected_unit, unit_filter_locked = _security_unit_for_session(request.args.get("unit"))
    jmd_route_filter = _parse_security_unit_filter(selected_unit)
    od_requests = []
    visitor_requests = []
    leave_requests = []
    it_requests = []
    vehicle_requests = []
    permission_emp_requests = []
    permission_cl_requests = []
    permission_view = (request.args.get("permission_view") or "emp").strip().lower()
    if permission_view not in ("emp", "cl"):
        permission_view = "emp"
    approver_status_rows = []
    firestore_error = None
    if tab == "on-duty":
        od_requests, firestore_error = fetch_security_od_requests(
            ist_day=selected_day,
            jmd_route_filter=jmd_route_filter,
        )
    elif tab == "visitor-request":
        visitor_requests, firestore_error = fetch_security_visitor_requests(
            ist_day=selected_day,
            jmd_route_filter=jmd_route_filter,
            security_tab_unit=selected_unit,
        )
    elif tab == "leave-request":
        leave_requests, firestore_error = fetch_security_leave_requests(
            ist_day=selected_day,
            jmd_route_filter=jmd_route_filter,
        )
    elif tab == "vehicle-request":
        vehicle_requests, firestore_error = fetch_security_vehicle_requests(
            ist_day=selected_day,
            jmd_route_filter=jmd_route_filter,
        )
    elif tab == "it-request":
        it_requests, firestore_error = fetch_security_it_requests(
            ist_day=selected_day,
            jmd_route_filter=jmd_route_filter,
        )
    elif tab == "permission-request":
        permission_emp_requests, permission_cl_requests, firestore_error = (
            fetch_security_permission_requests(
                ist_day=selected_day,
                jmd_route_filter=jmd_route_filter,
            )
        )
    elif tab == "approver-status":
        db, err = _get_firestore_client()
        if err:
            firestore_error = err
        else:
            try:
                approver_status_rows = _fetch_security_approver_status(db)
            except Exception as e:
                app.logger.exception("Firestore approver status fetch failed")
                firestore_error = _firestore_user_message(e)
    return render_template(
        "security.html",
        active_nav=active_nav,
        section_endpoint=active_nav,
        security_tabs=tabs,
        selected_tab=tab,
        od_requests=od_requests,
        visitor_requests=visitor_requests,
        leave_requests=leave_requests,
        it_requests=it_requests,
        vehicle_requests=vehicle_requests,
        permission_emp_requests=permission_emp_requests,
        permission_cl_requests=permission_cl_requests,
        permission_view=permission_view,
        approver_status_rows=approver_status_rows,
        firestore_error=firestore_error,
        selected_date_iso=selected_day.strftime("%Y-%m-%d"),
        selected_unit=selected_unit,
        unit_filter_locked=unit_filter_locked,
        can_delete_requests=_user_is_admin(),
    )


@app.route("/security")
@login_required
def security():
    return _render_requests_page("security", "security", SECURITY_TABS, "on-duty")


@app.route("/hr")
@login_required
def hr():
    return _render_requests_page("hr", "hr", HR_TABS, "leave-request")


@app.route("/it")
@login_required
def it():
    return _render_requests_page("it", "it", IT_TABS, "it-request")


@app.route("/logistics")
@login_required
def logistics():
    tab = (request.args.get("tab") or "logistics").strip().lower()
    return _render_ppc_page(tab)


@app.route("/logistics/export")
@login_required
def logistics_export():
    require_page("logistics")
    ist_today = _ist_today_date()
    from_day = _parse_security_table_date(request.args.get("from"), ist_today)
    to_day = _parse_security_table_date(request.args.get("to"), ist_today)
    if from_day > to_day:
        from_day, to_day = to_day, from_day
    rows, err = fetch_logistics_vehicle_requests_range(from_day, to_day)
    if err:
        abort(400, err)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(_LOGISTICS_CSV_HEADERS)
    for row in rows:
        writer.writerow(_logistics_row_to_csv_values(row))
    filename = f"vehicle_requests_{from_day.isoformat()}_{to_day.isoformat()}.csv"
    return Response(
        "\ufeff" + output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/logistics/api/fleet-vehicles", methods=["GET"])
@login_required
def logistics_fleet_vehicles():
    if not _user_can_access_logistics():
        abort(403)
    return jsonify({
        "ok": True,
        "vehicles": [
            {"code": code, "label": label}
            for code, label in LOGISTICS_INTERNAL_FLEET
        ],
    })


@app.route("/logistics/api/assignees", methods=["GET"])
@login_required
def logistics_assignees():
    if not _user_can_access_logistics():
        abort(403)
    vehicle_type = (request.args.get("vehicle_type") or "").strip()
    request_id = (request.args.get("request_id") or "").strip()
    db, err = _get_firestore_client()
    if err:
        return jsonify({"ok": False, "error": err}), 500
    options = _logistics_assignee_options(db, vehicle_type)
    if request_id:
        _ref, rd, load_err = _logistics_load_vehicle_request(db, request_id)
        if not load_err and rd:
            current = _logistics_normalize_code(rd.get("assigned_to_code") or "")
            options = [o for o in options if o["code"] != current]
    return jsonify({
        "ok": True,
        "assignees": [{"code": o["code"], "label": o["label"]} for o in options],
    })


@app.route("/logistics/api/assign", methods=["POST"])
@login_required
def logistics_assign():
    if not _user_can_access_logistics():
        abort(403)
    payload = request.get_json(silent=True) or {}
    ok, err = _logistics_assign_vehicle(
        payload.get("request_id") or "",
        payload.get("vehicle_type") or "",
        payload.get("assignee_code") or "",
        fleet_vehicle_code=payload.get("fleet_vehicle_code") or "",
        reassign=False,
    )
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not assign"}), 400
    return jsonify({"ok": True})


@app.route("/logistics/api/reassign", methods=["POST"])
@login_required
def logistics_reassign():
    if not _user_can_access_logistics():
        abort(403)
    payload = request.get_json(silent=True) or {}
    ok, err = _logistics_assign_vehicle(
        payload.get("request_id") or "",
        payload.get("vehicle_type") or "",
        payload.get("assignee_code") or "",
        fleet_vehicle_code=payload.get("fleet_vehicle_code") or "",
        reassign=True,
    )
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not re-assign"}), 400
    return jsonify({"ok": True})


@app.route("/logistics/api/cancel", methods=["POST"])
@login_required
def logistics_cancel():
    if not _user_can_access_logistics():
        abort(403)
    payload = request.get_json(silent=True) or {}
    ok, err = _logistics_cancel_vehicle(payload.get("request_id") or "")
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not cancel"}), 400
    return jsonify({"ok": True})


def _digits_only(value: str) -> str:
    return "".join(c for c in str(value or "").strip() if c.isdigit())


def _whatsapp_doc_id_from_mobile(mobile: str) -> str:
    digits = _digits_only(mobile)
    if len(digits) == 10:
        return f"whatsapp:+91{digits}"
    if len(digits) == 12 and digits.startswith("91"):
        return f"whatsapp:+{digits}"
    raise ValueError("Mobile must be a 10-digit Indian number (or 91 + 10 digits).")


def _create_firestore_whatsapp_user(data: dict) -> tuple[bool, str | None, bool]:
    """Save one employee to Firestore ``users`` (upsert by mobile). Returns (ok, error, created)."""
    ok, err, payload, doc_id = _validate_whatsapp_user_payload(data)
    if not ok:
        return False, err, False

    db, err = _get_firestore_client()
    if err:
        return False, err, False

    ref = db.collection("users").document(doc_id)
    created = not ref.get().exists
    try:
        ref.set(payload)
    except Exception as e:
        app.logger.exception("Firestore save whatsapp user failed employee_id=%s", payload.get("employee_id"))
        return False, _firestore_user_message(e), False

    action = "added" if created else "updated"
    app.logger.info(
        "whatsapp user %s doc_id=%s employee_id=%s by=%s",
        action,
        doc_id,
        payload.get("employee_id"),
        getattr(current_user, "email", ""),
    )
    return True, None, created


def _validate_whatsapp_user_payload(data: dict) -> tuple[bool, str | None, dict | None, str | None]:
    employee_id = (data.get("employee_id") or "").strip().upper()
    name = (data.get("name") or "").strip()
    department = (data.get("department") or "").strip()
    mobile_raw = (data.get("employee_mobile") or data.get("mobile") or "").strip()
    jmd_route = (data.get("jmd_route") or "JMD1").strip().upper()
    shift_type = (data.get("shift_type") or "GS").strip().upper()
    is_supervisor = bool(data.get("is_supervisor"))

    if not employee_id:
        return False, "Employee ID is required.", None, None
    if not name:
        return False, "Name is required.", None, None
    if not department:
        return False, "Department is required.", None, None
    if jmd_route not in ("JMD1", "JMD2"):
        return False, "JMD route must be JMD1 or JMD2.", None, None
    if shift_type not in ("GS", "RS"):
        return False, "Shift type must be GS or RS.", None, None

    digits = _digits_only(mobile_raw)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    if len(digits) != 10:
        return False, "Mobile must be a 10-digit Indian number.", None, None

    try:
        doc_id = _whatsapp_doc_id_from_mobile(digits)
    except ValueError as e:
        return False, str(e), None, None

    payload: dict = {
        "employee_id": employee_id,
        "name": name,
        "department": department,
        "employee_mobile": digits,
        "jmd_route": jmd_route,
        "role": "employee",
        "shift_type": shift_type,
    }

    if shift_type == "GS":
        shift_login = (data.get("shift_login") or "").strip()
        shift_logout = (data.get("shift_logout") or "").strip()
        if not shift_login or not shift_logout:
            return False, "GS shift requires login and logout times.", None, None
        payload["shift_login"] = shift_login
        payload["shift_logout"] = shift_logout
    else:
        s1_in = (data.get("shift1_login") or "").strip()
        s1_out = (data.get("shift1_logout") or "").strip()
        s2_in = (data.get("shift2_login") or "").strip()
        s2_out = (data.get("shift2_logout") or "").strip()
        if not s1_in or not s1_out:
            return False, "RS shift requires shift 1 login and logout times.", None, None
        payload["shift1_login"] = s1_in
        payload["shift1_logout"] = s1_out
        payload["shift2_login"] = s2_in
        payload["shift2_logout"] = s2_out

    if is_supervisor:
        payload["is_supervisor"] = True

    return True, None, payload, doc_id


def _normalize_whatsapp_user_mobile(raw: str) -> str:
    digits = _digits_only(raw or "")
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    return digits if len(digits) == 10 else ""


def _whatsapp_user_api_dict(doc_id: str, data: dict) -> dict:
    d = data or {}
    return {
        "doc_id": doc_id,
        "employee_id": d.get("employee_id") or "",
        "name": d.get("name") or "",
        "department": d.get("department") or "",
        "employee_mobile": d.get("employee_mobile") or _normalize_whatsapp_user_mobile(doc_id),
        "jmd_route": d.get("jmd_route") or "JMD1",
        "shift_type": d.get("shift_type") or "GS",
        "shift_login": d.get("shift_login") or "",
        "shift_logout": d.get("shift_logout") or "",
        "shift1_login": d.get("shift1_login") or "",
        "shift1_logout": d.get("shift1_logout") or "",
        "shift2_login": d.get("shift2_login") or "",
        "shift2_logout": d.get("shift2_logout") or "",
        "is_supervisor": bool(d.get("is_supervisor")),
    }


def _get_firestore_whatsapp_user_by_mobile(mobile: str) -> tuple[dict | None, str | None]:
    digits = _normalize_whatsapp_user_mobile(mobile)
    if not digits:
        return None, "Enter a valid 10-digit mobile number."
    db, err = _get_firestore_client()
    if err:
        return None, err
    try:
        doc_id = _whatsapp_doc_id_from_mobile(digits)
    except ValueError as e:
        return None, str(e)
    snap = db.collection("users").document(doc_id).get()
    if not snap.exists:
        return None, "No WhatsApp user found for this mobile number."
    return _whatsapp_user_api_dict(doc_id, snap.to_dict() or {}), None


def _delete_firestore_whatsapp_user_by_mobile(mobile: str) -> tuple[bool, str | None]:
    digits = _normalize_whatsapp_user_mobile(mobile)
    if not digits:
        return False, "Enter a valid 10-digit mobile number."
    db, err = _get_firestore_client()
    if err:
        return False, err
    try:
        doc_id = _whatsapp_doc_id_from_mobile(digits)
    except ValueError as e:
        return False, str(e)
    ref = db.collection("users").document(doc_id)
    if not ref.get().exists:
        return False, "No WhatsApp user found for this mobile number."
    try:
        ref.delete()
    except Exception as e:
        app.logger.exception("Firestore delete whatsapp user failed mobile=%s", digits)
        return False, _firestore_user_message(e)
    app.logger.info(
        "whatsapp user deleted doc_id=%s by=%s",
        doc_id,
        getattr(current_user, "email", ""),
    )
    return True, None


def _delete_firestore_request(request_id: str) -> tuple[bool, str | None]:
    """Permanently remove one document from Firestore ``requests``."""
    rid = (request_id or "").strip()
    if not rid:
        return False, "Missing request id"
    db, err = _get_firestore_client()
    if err:
        return False, err
    ref = db.collection("requests").document(rid)
    if not ref.get().exists:
        return False, "Request not found"
    ref.delete()
    return True, None


@app.route("/security/api/od-gate", methods=["POST"])
@login_required
def security_od_gate():
    if not _user_can_access_security():
        abort(403)
    payload = request.get_json(silent=True) or {}
    request_id = (payload.get("request_id") or "").strip()
    action = (payload.get("action") or "").strip()
    odo_reading = payload.get("odo_reading")
    ok, err = _security_record_od_gate(request_id, action, odo_reading)
    if not ok:
        return jsonify({"ok": False, "error": err or "Update failed"}), 400
    return jsonify({"ok": True})


@app.route("/security/api/permission-gate", methods=["POST"])
@login_required
def security_permission_gate():
    if not _user_can_access_security():
        abort(403)
    payload = request.get_json(silent=True) or {}
    request_id = (payload.get("request_id") or "").strip()
    action = (payload.get("action") or "").strip()
    ok, err = _security_record_permission_gate(request_id, action)
    if not ok:
        return jsonify({"ok": False, "error": err or "Update failed"}), 400
    return jsonify({"ok": True})


@app.route("/security/api/vehicle-gate", methods=["POST"])
@login_required
def security_vehicle_gate():
    if not _user_can_access_security():
        abort(403)
    payload = request.get_json(silent=True) or {}
    request_id = (payload.get("request_id") or "").strip()
    action = (payload.get("action") or "").strip()
    vehicle_number = payload.get("vehicle_number")
    ok, err = _security_record_vehicle_gate(
        request_id,
        action,
        vehicle_number=vehicle_number,
    )
    if not ok:
        return jsonify({"ok": False, "error": err or "Update failed"}), 400
    return jsonify({"ok": True})


@app.route("/security/api/visitor-gate", methods=["POST"])
@login_required
def security_visitor_gate():
    if not _user_can_access_security():
        abort(403)
    payload = request.get_json(silent=True) or {}
    request_id = (payload.get("request_id") or "").strip()
    action = (payload.get("action") or "").strip()
    otp = payload.get("otp")
    security_unit = (payload.get("unit") or "unit-i").strip().lower()
    ok, err = _security_record_visitor_gate(
        request_id, action, otp, security_unit=security_unit
    )
    if not ok:
        return jsonify({"ok": False, "error": err or "Update failed"}), 400
    return jsonify({"ok": True})


@app.route("/security/api/add-whatsapp-user", methods=["POST"])
@login_required
def security_add_whatsapp_user():
    """Admin only: save employee in Firestore ``users`` (upsert by mobile)."""
    if not _user_is_admin():
        abort(403)
    payload = request.get_json(silent=True) or {}
    ok, err, created = _create_firestore_whatsapp_user(payload)
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not save user"}), 400
    return jsonify({"ok": True, "created": created, "updated": not created})


@app.route("/security/api/whatsapp-user", methods=["GET"])
@login_required
def security_lookup_whatsapp_user():
    """Admin only: load one WhatsApp user by mobile number."""
    if not _user_is_admin():
        abort(403)
    mobile = (request.args.get("mobile") or "").strip()
    user, err = _get_firestore_whatsapp_user_by_mobile(mobile)
    if err:
        return jsonify({"ok": False, "error": err}), 404
    return jsonify({"ok": True, "user": user})


@app.route("/security/api/whatsapp-user", methods=["DELETE"])
@login_required
def security_delete_whatsapp_user():
    """Admin only: remove one WhatsApp user by mobile number."""
    if not _user_is_admin():
        abort(403)
    payload = request.get_json(silent=True) or {}
    mobile = (payload.get("employee_mobile") or payload.get("mobile") or "").strip()
    ok, err = _delete_firestore_whatsapp_user_by_mobile(mobile)
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not delete user"}), 400
    return jsonify({"ok": True})


@app.route("/security/api/update-whatsapp-user", methods=["POST"])
@login_required
def security_update_whatsapp_user():
    """Admin only: update existing WhatsApp user (same as add — keyed by mobile)."""
    if not _user_is_admin():
        abort(403)
    payload = request.get_json(silent=True) or {}
    ok, err, created = _create_firestore_whatsapp_user(payload)
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not update user"}), 400
    return jsonify({"ok": True, "created": created, "updated": not created})


@app.route("/security/api/delete-request", methods=["POST"])
@login_required
def security_delete_request():
    """Admin only: delete one request (OD, visitor, permission, etc.) from Firestore."""
    if not _user_is_admin():
        abort(403)
    payload = request.get_json(silent=True) or {}
    request_id = (payload.get("request_id") or "").strip()
    ok, err = _delete_firestore_request(request_id)
    if not ok:
        return jsonify({"ok": False, "error": err or "Delete failed"}), 400
    app.logger.info(
        "security request deleted request_id=%s by=%s",
        request_id,
        getattr(current_user, "email", ""),
    )
    return jsonify({"ok": True})


@app.route("/help")
@login_required
def help():
    require_page("help")
    return render_template("under_development.html", active_nav="help")


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(_login_landing_url(current_user))
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        if not email or not password:
            flash("Email and password are required.", "danger")
            return render_template("login.html")
        user_row = auth.get_user_by_email(email)
        if not user_row or not auth.check_password(user_row, password):
            flash("Invalid email or password.", "danger")
            return render_template("login.html")
        role = (user_row.get("role") or "viewer").strip().lower()
        allowed = auth.get_viewer_pages(user_row["id"]) if role == "viewer" else []
        user = User(
            id_=user_row["id"],
            email=user_row["email"],
            role=role,
            allowed_pages=allowed,
        )
        login_user(user, remember=bool(request.form.get("remember")))
        next_url = request.args.get("next")
        if not next_url or not str(next_url).startswith("/"):
            next_url = _login_landing_url(user)
        return redirect(next_url)
    return render_template("login.html")


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    reset_url = None
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        if not email:
            flash("Please enter your email address.", "danger")
            return redirect(url_for("forgot_password"))
        token = auth.create_reset_token(email)
        if token:
            reset_url = url_for("reset_password", token=token, _external=True)
            flash("Use the link below to set a new password. It expires in 1 hour.", "success")
        else:
            flash("No account found with that email address. Try again or contact your admin.", "danger")
        return render_template("forgot_password.html", reset_url=reset_url)
    return render_template("forgot_password.html", reset_url=None)


@app.route("/reset-password", methods=["GET", "POST"])
def reset_password():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    token = request.args.get("token") or request.form.get("token")
    user_id = auth.get_user_id_from_reset_token(token) if token else None
    if not user_id:
        flash("Invalid or expired reset link. Please request a new one.", "danger")
        return redirect(url_for("forgot_password"))
    if request.method == "POST":
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")
        if len(password) < 8:
            flash("Password must be at least 8 characters.", "danger")
            return render_template("reset_password.html", token=token)
        if password != confirm:
            flash("Passwords do not match.", "danger")
            return render_template("reset_password.html", token=token)
        auth.set_password(user_id, password)
        auth.clear_reset_token(token)
        flash("Your password has been reset. You can log in now.", "success")
        return redirect(url_for("login"))
    return render_template("reset_password.html", token=token)


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


@app.route("/settings", methods=["GET"])
@app.route("/settings.html", methods=["GET"])
@login_required
def settings():
    highlights_filter = auth.get_user_preference(current_user.id, "highlightsFilter") or "bad"
    return render_template(
        "settings.html",
        active_nav="settings",
        highlights_filter=highlights_filter,
    )


@app.route("/settings/highlights", methods=["POST"])
@login_required
def settings_highlights():
    """Save highlights filter preference for the current user."""
    data = request.get_json(silent=True) or {}
    value = (data.get("highlightsFilter") or request.form.get("highlightsFilter") or "bad").strip().lower()
    if value not in ("bad", "good"):
        value = "bad"
    auth.set_user_preference(current_user.id, "highlightsFilter", value)
    return {"ok": True, "highlightsFilter": value}


if __name__ == "__main__":
    _ensure_auth_database()
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_DEBUG", "").strip().lower() in ("1", "true", "yes")
    app.run(host="0.0.0.0", port=port, debug=debug)
