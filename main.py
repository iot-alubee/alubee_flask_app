from flask import Flask, render_template, redirect, url_for, request, flash, abort
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    logout_user,
    login_required,
    current_user,
)
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import secrets

import auth

app = Flask(__name__)
app.config["SECRET_KEY"] = "change-this-to-a-random-secret-key-in-production"

login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message = "Please log in to access this page."

# Page key for each route (used for permission checks)
PAGE_KEYS = [p[0] for p in auth.PAGE_KEYS]


def _init_bigquery_client():
    """Initialise BigQuery client using the service account JSON in the project root.

    Returns None if credentials are missing or invalid so the app can still run.
    """
    sa_path = os.path.join(os.path.dirname(__file__), "bq_service_acc.json")
    if not os.path.exists(sa_path):
        return None
    try:
        credentials = service_account.Credentials.from_service_account_file(sa_path)
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception:
        return None


bq_client = _init_bigquery_client()


def fetch_machine_idle_rows(date_str=None, shift=None, unit=None, department=None):
    """Fetch machine idle rows from BigQuery with optional filters."""
    if bq_client is None:
        return []

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
    except Exception:
        return []

    return [dict(row) for row in result]


def fetch_machine_idle_filter_values():
    """Return distinct Shift, Unit, department values from BigQuery for filters."""
    if bq_client is None:
        return {"shifts": [], "units": [], "departments": []}

    def _simple_list(sql, field):
        try:
            rows = bq_client.query(sql).result()
            return [r[field] for r in rows if r[field] is not None]
        except Exception:
            return []

    shifts = _simple_list(
        "SELECT DISTINCT Shift FROM `alubee_production_marts.fact_machine_idle` "
        "WHERE Shift IS NOT NULL ORDER BY Shift",
        "Shift",
    )
    units = _simple_list(
        "SELECT DISTINCT Unit FROM `alubee_production_marts.fact_machine_idle` "
        "WHERE Unit IS NOT NULL ORDER BY Unit",
        "Unit",
    )
    departments = _simple_list(
        "SELECT DISTINCT department FROM `alubee_production_marts.fact_machine_idle` "
        "WHERE department IS NOT NULL ORDER BY department",
        "department",
    )

    return {"shifts": shifts, "units": units, "departments": departments}


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

    selected_date = request.args.get("dateFilter") or None
    selected_shift = request.args.get("shiftSlicer") or "All"
    selected_unit = request.args.get("unitSlicer") or "All"
    selected_department = request.args.get("departmentSlicer") or "All"

    filter_values = fetch_machine_idle_filter_values()

    machine_rows = fetch_machine_idle_rows(
        date_str=selected_date,
        shift=selected_shift,
        unit=selected_unit,
        department=selected_department,
    )

    return render_template(
        "index.html",
        machine_rows=machine_rows,
        selected_date=selected_date,
        selected_shift=selected_shift,
        selected_unit=selected_unit,
        selected_department=selected_department,
        shift_options=filter_values["shifts"],
        unit_options=filter_values["units"],
        department_options=filter_values["departments"],
        active_nav="production",
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


@app.route("/consumables")
@login_required
def consumables():
    require_page("consumables")
    return render_template("under_development.html", active_nav="consumables")


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


@app.route("/settings")
@login_required
def settings():
    return render_template("settings.html")


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
    app.run(debug=True)
