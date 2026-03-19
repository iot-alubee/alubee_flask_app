from flask import Flask, render_template, redirect, url_for, request, flash, abort
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    logout_user,
    login_required,
    current_user,
)
from datetime import date, timedelta, datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # Python < 3.9
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import re
import secrets
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import auth

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

app = Flask(__name__)


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
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY") or "change-this-to-a-random-secret-key-in-production"

login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message = "Please log in to access this page."

# Page key for each route (used for permission checks)
PAGE_KEYS = [p[0] for p in auth.PAGE_KEYS]

# Ensure DB and tables exist when run under gunicorn (if __name__ == "__main__" is not run)
auth.init_db()


def _init_bigquery_client():
    """Initialise BigQuery client. No service account file in this folder.

    - If BQ_CREDENTIALS_PATH is set and the file exists, use it (e.g. local dev with a key elsewhere).
    - Otherwise use Application Default Credentials (ADC). On Cloud Run this uses the
      service account attached to the Cloud Run service; no key file required.
    Returns None if credentials are missing or invalid so the app can still run.
    """
    creds_path = os.environ.get("BQ_CREDENTIALS_PATH")
    if creds_path and os.path.isfile(creds_path):
        try:
            credentials = service_account.Credentials.from_service_account_file(creds_path)
            return bigquery.Client(credentials=credentials, project=credentials.project_id)
        except Exception:
            pass
    # Application Default Credentials only (no key file in app folder)
    try:
        project = os.environ.get("GOOGLE_CLOUD_PROJECT")
        return bigquery.Client(project=project) if project else bigquery.Client()
    except Exception:
        return None


bq_client = _init_bigquery_client()


def _get_max_date_machine_idle():
    """Return the latest Date in fact_machine_idle, or None on error. Cached briefly."""
    if bq_client is None:
        return None
    cache_key = ("max_date_machine_idle",)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        job = bq_client.query(
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
    if bq_client is None:
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
        result = bq_client.query(query, job_config=job_config).result()
        rows = [dict(row) for row in result]
        app.logger.info("Machine idle: date=%s unit=%s shift=%s dept=%s -> %d rows", date_str, unit, shift, department, len(rows))
        _cache_set(cache_key, rows)
        return rows
    except Exception as e:
        app.logger.warning("BigQuery machine idle query failed: %s", e)
        return []


IOT_MASTER_TABLE = "alubee_production_marts.fact_iot_master"


def fetch_iot_master_rows(date_str=None, shift=None, unit=None, department=None):
    """Fetch production/IoT rows from fact_iot_master. Same filters as machine idle (date, shift, unit, department)."""
    if bq_client is None:
        return []
    cache_key = ("iot_master", date_str, shift, unit, department)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    query = """
        SELECT
            shift_date,
            shift_id,
            unit,
            department,
            item_code,
            partNo,
            cycle_time_sec,
            components_in_fixture,
            shot,
            quantity,
            plan
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

    query += " ORDER BY partNo, shift_id"

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        result = bq_client.query(query, job_config=job_config).result()
        rows = [dict(row) for row in result]
        app.logger.info("IoT master: date=%s unit=%s shift=%s dept=%s -> %d rows", date_str, unit, shift, department, len(rows))
        _cache_set(cache_key, rows)
        return rows
    except Exception as e:
        app.logger.warning("BigQuery IoT master query failed: %s", e)
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
        role = row["role"] or "viewer"
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


@app.context_processor
def inject_nav_permissions():
    """Make allowed_pages and is_admin available in templates."""
    if current_user.is_authenticated:
        return {
            "allowed_pages": getattr(current_user, "allowed_pages", []),
            "user_role": getattr(current_user, "role", "viewer"),
            "is_admin": getattr(current_user, "role", None) == "admin",
        }
    return {"allowed_pages": [], "user_role": "", "is_admin": False}


@app.route("/")
@login_required
def index():
    require_page("production")

    # Default date to yesterday when not provided
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

    # If no rows for chosen date (e.g. data is in 2026, yesterday is 2025), use latest date in table
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

    highlights_filter = auth.get_user_preference(current_user.id, "highlightsFilter") or "bad"
    return render_template(
        "index.html",
        machine_rows=machine_rows,
        iot_rows=iot_rows,
        selected_date=selected_date,
        selected_shift=selected_shift,
        selected_unit=selected_unit,
        selected_department=selected_department,
        shift_options=SHIFT_OPTIONS,
        unit_options=UNIT_OPTIONS,
        department_options=DEPARTMENT_OPTIONS,
        active_nav="production",
        highlights_filter=highlights_filter,
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
    )


@app.route("/realtime")
@login_required
def realtime():
    require_page("realtime")
    return render_template("under_development.html", active_nav="realtime")


PARTS_TABLE = "alubee-prod.alubee_production_marts.dim_component_mapper"
MONTHLY_PLANNER_TABLE = "alubee-prod.alubee_production_marts.dim_monthly_planner"
JOB_ALLOCATOR_TABLE = "alubee-prod.alubee_production_marts.fact_job_allocator"
PLAN_CHANGE_REQUEST_TABLE = "alubee-prod.alubee_production_marts.fact_plan_change_request"


def fetch_monthly_planner(plan_month: str | None = None, department: str | None = None):
    """Fetch rows from monthly planner table, optionally filtered by plan_month (yyyy-mm) and department."""
    if bq_client is None:
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
        result = bq_client.query(base_query, job_config=job_config).result()
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
    if bq_client is None:
        return None
    query = f"SELECT IFNULL(MAX(plan_id), 0) AS max_id FROM `{MONTHLY_PLANNER_TABLE}`"
    try:
        row = next(bq_client.query(query).result(), None)
        max_id = row["max_id"] if row and row["max_id"] is not None else 0
        return int(max_id) + 1
    except Exception as e:
        app.logger.warning("BigQuery _get_next_plan_id failed: %s", e)
        return None


def _get_part_by_part_no(part_no: str):
    """Return dict with part_no, part_name (and other fields) for part_no or None. Table uses part_no as key (no part_id)."""
    if bq_client is None or not part_no:
        return None
    query = f"""SELECT part_no, part_name, department, components_in_fixture, cycle_time_sec, qty_per_hour
        FROM `{PARTS_TABLE}` WHERE part_no = @part_no LIMIT 1"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("part_no", "STRING", part_no)]
    )
    try:
        row = next(bq_client.query(query, job_config=job_config).result(), None)
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
    if bq_client is None or plan_id is None:
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
        row = next(bq_client.query(query, job_config=job_config).result(), None)
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
    if bq_client is None:
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
        result = bq_client.query(base_query, job_config=job_config).result()
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
    if bq_client is None:
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
        result = bq_client.query(base_query, job_config=job_config).result()
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
    if bq_client is None:
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
        result = bq_client.query(query, job_config=job_config).result()
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
    if bq_client is None:
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
        result = bq_client.query(query).result()
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
    if bq_client is None:
        return 0
    base_query = f"SELECT COUNT(*) AS n FROM `{PARTS_TABLE}`"
    params = []
    if department:
        base_query += " WHERE LOWER(TRIM(COALESCE(department, ''))) = LOWER(TRIM(@department))"
        params.append(bigquery.ScalarQueryParameter("department", "STRING", department))
    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    try:
        row = next(bq_client.query(base_query, job_config=job_config).result(), None)
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
    if bq_client is None:
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
        result = bq_client.query(base_query, job_config=job_config).result()
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
    if not part_no or bq_client is None:
        return False
    query = f"SELECT 1 FROM `{PARTS_TABLE}` WHERE part_no = @part_no"
    params = [bigquery.ScalarQueryParameter("part_no", "STRING", part_no)]
    if exclude_part_no:
        query += " AND part_no != @exclude_part_no"
        params.append(bigquery.ScalarQueryParameter("exclude_part_no", "STRING", exclude_part_no))
    query += " LIMIT 1"
    try:
        row = next(
            bq_client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result(),
            None,
        )
        return row is not None
    except Exception as e:
        app.logger.warning("BigQuery _part_no_exists failed: %s", e)
        return False


def _plan_exists(plan_month: str, part_no: str, exclude_plan_id: int | None = None) -> bool:
    """Return True if a plan already exists for given month and part_no."""
    if not plan_month or not part_no or bq_client is None:
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
            bq_client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result(),
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


@app.route("/ppc/job-allocator/update-plan", methods=["POST"])
@login_required
def ppc_job_allocator_update_plan():
    """Update plan and job_created_at for a job allocation row.

    Rows are identified by machine_no (plus department/unit); job_id is no longer used.
    """
    require_page("ppc")
    if bq_client is None:
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
    if bq_client is not None:
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
            mp_row = next(bq_client.query(mp_query, job_config=mp_job_cfg).result(), None)
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
    if bq_client is not None and part_no and plan_val > 0:
        qty_query = f"""
            SELECT qty_per_hour
            FROM `{PARTS_TABLE}`
            WHERE part_no = @part_no
            LIMIT 1
        """
        qty_params = [bigquery.ScalarQueryParameter("part_no", "STRING", part_no)]
        qty_job_cfg = bigquery.QueryJobConfig(query_parameters=qty_params)
        try:
            qty_row = next(bq_client.query(qty_query, job_config=qty_job_cfg).result(), None)
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
    if bq_client is not None:
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
                bq_client.query(
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
        update_job = bq_client.query(update_query, job_config=job_config)
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
            bq_client.query(insert_query, job_config=job_config).result()
            flash("Job allocation saved.", "success")
        except Exception as e:
            app.logger.warning("BigQuery insert job allocation failed: %s", e)
            flash("Save failed. Please try again.", "danger")

    # Reflect in Monthly Planner: add allocated_delta to allocated for this month/department/part_no
    if bq_client is not None and allocated_delta != 0:
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
            bq_client.query(mp_update, job_config=bigquery.QueryJobConfig(query_parameters=mp_params)).result()
        except Exception as e:
            app.logger.warning("BigQuery update monthly planner allocated failed: %s", e)

    return redirect(url_for("ppc", daily_dept=daily_dept, daily_unit=daily_unit) + "#daily-tab-pane")


@app.route("/ppc/monthly-planner", methods=["POST"])
@login_required
def ppc_monthly_planner_add():
    require_page("ppc")
    if bq_client is None:
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
        bq_client.query(query, job_config=job_config).result()
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
    if bq_client is None:
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
        bq_client.query(query, job_config=job_config).result()
        flash("Monthly plan updated successfully.", "success")
    except Exception as exc:
        app.logger.warning("BigQuery monthly planner update failed: %s", exc)
        flash("Failed to update monthly plan.", "danger")

    return redirect(url_for("ppc") + "#monthly-tab-pane")


@app.route("/ppc/monthly-planner/<int:plan_id>/delete", methods=["POST"])
@login_required
def ppc_delete_monthly_plan(plan_id):
    require_page("ppc")
    if bq_client is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#monthly-tab-pane")

    query = f"DELETE FROM `{MONTHLY_PLANNER_TABLE}` WHERE plan_id = @plan_id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("plan_id", "INT64", plan_id)]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
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
    if bq_client is None:
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
        bq_client.query(query, job_config=job_config).result()
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

    if bq_client is None:
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
        bq_client.query(query, job_config=job_config).result()
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
    if bq_client is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("ppc") + "#part-tab-pane")

    query = f"DELETE FROM `{PARTS_TABLE}` WHERE part_no = @part_no"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("part_no", "STRING", part_no),
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
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

        if bq_client is None:
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
            bq_client.query(query, job_config=job_config).result()
            flash("Part updated successfully.", "success")
            return redirect(url_for("ppc") + "#part-tab-pane")
        except Exception as exc:
            app.logger.warning("BigQuery update part %s failed: %s", part_no, exc)
            flash("Failed to update part. Ensure Part Name is unique.", "danger")
            flash(f"BigQuery error: {exc}", "danger")
            return redirect(url_for("ppc_edit_part", part_no=part_no))

    # GET
    if bq_client is None:
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
    # Reuse PPC permission so existing PPC users can access this page.
    require_page("ppc")
    selected_tab = (request.args.get("tab") or "PDC").strip().upper()
    allowed_tabs = ("PDC", "FET", "CNC", "SEC")
    if selected_tab not in allowed_tabs:
        selected_tab = "PDC"
    department_rows = fetch_department_job_allocations(selected_tab)
    return render_template(
        "department.html",
        active_nav="department",
        selected_tab=selected_tab,
        department_tabs=allowed_tabs,
        department_rows=department_rows,
    )


@app.route("/department/switch-request", methods=["POST"])
@login_required
def department_switch_request():
    # Reuse PPC permission so existing PPC users can access this action.
    require_page("ppc")
    if bq_client is None:
        flash("BigQuery is not configured.", "danger")
        return redirect(url_for("department"))

    machine_no = (request.form.get("machine_no") or "").strip()
    from_part_no = (request.form.get("from_part_no") or "").strip()
    to_part_no = (request.form.get("to_part_no") or "").strip()
    selected_tab = (request.form.get("tab") or "PDC").strip().upper()
    allowed_tabs = {"PDC", "FET", "CNC", "SEC"}
    if selected_tab not in allowed_tabs:
        selected_tab = "PDC"

    if not machine_no or not from_part_no or not to_part_no:
        flash("Invalid switch request data.", "danger")
        return redirect(url_for("department", tab=selected_tab))

    if from_part_no == to_part_no:
        flash("Back Up plan must be different from primary plan.", "danger")
        return redirect(url_for("department", tab=selected_tab))

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
            bq_client.query(duplicate_query, job_config=duplicate_cfg).result(),
            None,
        )
        if duplicate_row is not None:
            flash("A pending switch request already exists for this machine and parts.", "warning")
            return redirect(url_for("department", tab=selected_tab))
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
        bq_client.query(query, job_config=job_config).result()
        flash("Switch request raised.", "success")
    except Exception as e:
        app.logger.warning("BigQuery insert switch request failed: %s", e)
        flash("Failed to raise switch request.", "danger")

    return redirect(url_for("department", tab=selected_tab))


@app.route("/ppc/switch-request/approve", methods=["POST"])
@login_required
def ppc_approve_switch_request():
    require_page("ppc")
    if bq_client is None:
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
        latest_row = next(bq_client.query(latest_query, job_config=latest_cfg).result(), None)
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
        qty_row = next(bq_client.query(qty_query, job_config=qty_cfg).result(), None)
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
        bq_client.query(insert_query, job_config=insert_cfg).result()
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
        bq_client.query(update_query, job_config=update_cfg).result()
        flash("Switch request approved and job card updated.", "success")
    except Exception as e:
        app.logger.warning("BigQuery approve switch request update failed: %s", e)
        flash("Approved switch but failed to update request status.", "warning")

    return redirect(url_for("ppc") + "#switch-request-tab-pane")


@app.route("/ppc/switch-request/deny", methods=["POST"])
@login_required
def ppc_deny_switch_request():
    require_page("ppc")
    if bq_client is None:
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
        bq_client.query(query, job_config=job_config).result()
        flash("Switch request denied.", "success")
    except Exception as e:
        app.logger.warning("BigQuery deny switch request failed: %s", e)
        flash("Failed to deny switch request.", "danger")

    return redirect(url_for("ppc") + "#switch-request-tab-pane")


@app.route("/maintenance")
@login_required
def maintenance():
    require_page("maintenance")
    return render_template("under_development.html", active_nav="maintenance")


@app.route("/documents")
@login_required
def documents():
    require_page("documents")
    return render_template("under_development.html", active_nav="documents")


@app.route("/help")
@login_required
def help():
    require_page("help")
    return render_template("under_development.html", active_nav="help")


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
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
        role = user_row.get("role") or "viewer"
        allowed = auth.get_viewer_pages(user_row["id"]) if role == "viewer" else []
        user = User(
            id_=user_row["id"],
            email=user_row["email"],
            role=role,
            allowed_pages=allowed,
        )
        login_user(user, remember=bool(request.form.get("remember")))
        next_url = request.args.get("next") or url_for("index")
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
    auth.init_db()
    existing = auth.get_user_by_email("admin@alubee.com")
    if existing is None:
        auth.create_user("admin@alubee.com", "admin123", "admin")
    else:
        conn = auth.get_db()
        conn.execute(
            "UPDATE users SET role = 'admin' WHERE email = ?",
            ("admin@alubee.com",),
        )
        conn.commit()
        conn.close()
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_DEBUG", "").lower() in ("1", "true", "yes")
    app.run(host="0.0.0.0", port=port, debug=debug)
