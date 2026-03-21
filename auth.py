"""
Authentication: user storage and password hashing.
Uses SQLite for users (not BigQuery). Roles: admin, editor, viewer.
"""
import sqlite3
import os
import secrets
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash

DB_PATH = os.path.join(os.path.dirname(__file__), "instance", "app.db")

# Page keys used for viewer permissions (must match route names / active_nav)
PAGE_KEYS = [
    ("production", "Production"),
    ("ppc", "PPC"),
    ("realtime", "Realtime"),
    ("consumables", "Consumables"),
    ("maintenance", "Maintenance"),
    ("documents", "Documents"),
    ("help", "Help"),
]


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'viewer',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    try:
        conn.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'viewer'")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS viewer_permissions (
            user_id INTEGER NOT NULL,
            page TEXT NOT NULL,
            PRIMARY KEY (user_id, page),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (user_id, key),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()
    conn.close()


def get_user_preference(user_id, key):
    """Return preference value for user and key, or None if not set."""
    if not user_id:
        return None
    conn = get_db()
    row = conn.execute(
        "SELECT value FROM user_preferences WHERE user_id = ? AND key = ?",
        (int(user_id), key),
    ).fetchone()
    conn.close()
    return row["value"] if row else None


def set_user_preference(user_id, key, value):
    """Save preference for user and key."""
    if not user_id:
        return
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO user_preferences (user_id, key, value) VALUES (?, ?, ?)",
        (int(user_id), key, str(value)),
    )
    conn.commit()
    conn.close()


def get_user_by_email(email):
    conn = get_db()
    row = conn.execute(
        "SELECT id, email, password_hash, role FROM users WHERE email = ?",
        (email.strip().lower(),),
    ).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row["id"],
        "email": row["email"],
        "password_hash": row["password_hash"],
        "role": row["role"] or "viewer",
    }


def check_password(user, password):
    return check_password_hash(user["password_hash"], password)


def create_user(email, password, role="viewer"):
    email = email.strip().lower()
    if role not in ("admin", "editor", "viewer"):
        role = "viewer"
    password_hash = generate_password_hash(password, method="scrypt")
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO users (email, password_hash, role) VALUES (?, ?, ?)",
            (email, password_hash, role),
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return None
    finally:
        conn.close()


def get_viewer_pages(user_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT page FROM viewer_permissions WHERE user_id = ?", (int(user_id),)
    ).fetchall()
    conn.close()
    return [r["page"] for r in rows]


def set_viewer_pages(user_id, pages):
    conn = get_db()
    conn.execute("DELETE FROM viewer_permissions WHERE user_id = ?", (int(user_id),))
    for page in pages or []:
        if page in [p[0] for p in PAGE_KEYS]:
            conn.execute(
                "INSERT INTO viewer_permissions (user_id, page) VALUES (?, ?)",
                (int(user_id), page),
            )
    conn.commit()
    conn.close()


def list_users_with_permissions():
    conn = get_db()
    users = conn.execute(
        "SELECT id, email, role, created_at FROM users ORDER BY created_at DESC"
    ).fetchall()
    result = []
    for u in users:
        uid = u["id"]
        pages = []
        if u["role"] == "viewer":
            rows = conn.execute(
                "SELECT page FROM viewer_permissions WHERE user_id = ?", (uid,)
            ).fetchall()
            pages = [r["page"] for r in rows]
        result.append({
            "id": uid,
            "email": u["email"],
            "role": u["role"] or "viewer",
            "created_at": u["created_at"],
            "allowed_pages": pages,
        })
    conn.close()
    return result


def get_user_role(user_id):
    conn = get_db()
    row = conn.execute(
        "SELECT role FROM users WHERE id = ?", (int(user_id),)
    ).fetchone()
    conn.close()
    return (row["role"] or "viewer") if row else "viewer"


def create_reset_token(email):
    """Create a password reset token for the given email. Returns (token, expires_at) or None if email not found."""
    user = get_user_by_email(email)
    if not user:
        return None
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.utcnow() + timedelta(hours=1)).isoformat()
    conn = get_db()
    conn.execute(
        "INSERT INTO password_reset_tokens (token, user_id, expires_at) VALUES (?, ?, ?)",
        (token, user["id"], expires_at),
    )
    conn.commit()
    conn.close()
    return token


def get_user_id_from_reset_token(token):
    """Return user_id if token is valid and not expired, else None."""
    if not token:
        return None
    conn = get_db()
    row = conn.execute(
        "SELECT user_id, expires_at FROM password_reset_tokens WHERE token = ?",
        (token,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    try:
        if datetime.fromisoformat(row["expires_at"]) < datetime.utcnow():
            return None
    except (ValueError, TypeError):
        return None
    return row["user_id"]


def clear_reset_token(token):
    conn = get_db()
    conn.execute("DELETE FROM password_reset_tokens WHERE token = ?", (token,))
    conn.commit()
    conn.close()


def set_password(user_id, new_password):
    password_hash = generate_password_hash(new_password, method="scrypt")
    conn = get_db()
    conn.execute(
        "UPDATE users SET password_hash = ? WHERE id = ?",
        (password_hash, int(user_id)),
    )
    conn.commit()
    conn.close()
