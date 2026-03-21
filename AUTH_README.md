# Authentication

## Do I need a table in BigQuery for users?

**No.** User accounts are stored in **SQLite**, not BigQuery.

- **BigQuery** – Use it for your **analytics and dashboard data** (production stats, machine data, etc.). It’s built for large queries and reporting, not for checking a password on every request.
- **SQLite** – Used for **users and login** only. It’s fast, has no extra setup, and keeps auth simple. The file is `instance/app.db`.

So: **no users table in BigQuery**. Keep BigQuery for business data; keep auth in SQLite.

## How it works

- **Flask-Login** handles sessions and “remember me”.
- **SQLite** stores the `users` table (id, email, password_hash, created_at).
- **First run** creates a default user you can use to log in:
  - Email: `admin@alubee.com`
  - Password: `admin123`  
  Change this password (or add a proper “change password” flow) before going to production.

## Production checklist

1. Set a strong **SECRET_KEY** in `main.py` (e.g. from env: `os.environ.get("SECRET_KEY")`).
2. Change or remove the default admin password.
3. Add more users in code with `auth.create_user("email@example.com", "securepassword")`, or add a signup/account page later.

## Adding more users (e.g. from Python)

```python
import auth
auth.create_user("user@example.com", "their-password")
```
