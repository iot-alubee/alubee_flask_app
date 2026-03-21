# Deploy Flask app to Google Cloud Run

This folder is set up to deploy **without** a service account JSON file in the image. The app uses **Application Default Credentials** — the **Cloud Run service identity** (grant it BigQuery IAM roles).

## Prerequisites

- Google Cloud project with Cloud Run and BigQuery APIs enabled
- `gcloud` CLI installed and logged in (`gcloud auth login`)
- Docker (optional; you can use Cloud Build to build the image)

## 1. Set up the Cloud Run service account

1. In Google Cloud Console, go to **IAM & Admin** → **Service Accounts**.
2. Find the **default compute service account** used by Cloud Run (e.g. `PROJECT_NUMBER-compute@developer.gserviceaccount.com`) or create a dedicated service account for this app.
3. Grant that service account **BigQuery** permissions (e.g. **BigQuery Data Editor** and **BigQuery Job User**) so it can read/write your BigQuery datasets.

## 2. Deploy from the `Production` folder

**Important:** You must build and deploy **from inside this `Production` folder** so that `main.py`, `auth.py`, `templates/`, and `static/` are at the image root. Building from the repo root will cause "No module named 'main'" on Cloud Run.

Keep the deployable app files in this folder aligned with the **parent repo** (local) app before each build.

From the **`Production`** directory (this folder):

```bash
# Set your project and region
export PROJECT_ID=your-gcp-project-id
export REGION=us-central1

# Build and deploy (Cloud Build will build the Docker image; no key file is uploaded)
gcloud run deploy alubee-app \
  --source . \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID \
  --allow-unauthenticated
```

Or build with Docker and push to Artifact Registry, then deploy:

```bash
cd Production   # must be in Production folder (Windows path: same name as this directory)
# Build (context is current dir so main.py is in /app)
docker build -t gcr.io/$PROJECT_ID/alubee-app:latest .

# Push (configure docker for gcr first: gcloud auth configure-docker)
docker push gcr.io/$PROJECT_ID/alubee-app:latest

# Deploy
gcloud run deploy alubee-app \
  --image gcr.io/$PROJECT_ID/alubee-app:latest \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID
```

## 3. Environment variables

- **SECRET_KEY** – **Required for stable sessions in production.** Set a long random string in Cloud Run → Variables (or Secrets). If unset, the app uses a default and logs a warning; users may be logged out on each deploy.
- **GOOGLE_CLOUD_PROJECT** – Usually set automatically by Cloud Run. Override only if needed.
- **BQ_CREDENTIALS_PATH** – Do **not** set on Cloud Run. Only for local dev with a key file outside the repo.

### SQLite auth database on Cloud Run

Users are stored in `instance/app.db` on the container filesystem. That storage is **ephemeral**: new revisions or extra instances each get an empty DB, and the default **admin@alubee.com** user is recreated on startup. For durable accounts across deploys or multiple instances, move auth to Cloud SQL, Firestore, or another managed store (future change).

## 4. What the container runs

- **`requirements.txt`** includes **gunicorn** (the Dockerfile starts `gunicorn main:app`).
- **`main.py`** initializes the auth database **on import**, so login works under gunicorn (not only when running `python main.py`).

## 5. Local development (from this folder)

Use one of:

```bash
gcloud auth application-default login
python main.py
```

Or a key file **outside** the repo:

```bash
export BQ_CREDENTIALS_PATH=/path/to/your/service-account-key.json
python main.py
```

On **Cloud Run**, do not set `BQ_CREDENTIALS_PATH`; the runtime service account is used automatically.
