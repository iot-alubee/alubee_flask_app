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

## 3. Environment variables (optional)

- **GOOGLE_CLOUD_PROJECT** – Set automatically by Cloud Run. If you need to override, set it in Cloud Run → Edit & deploy new revision → Variables.
- **SECRET_KEY** – Set in Cloud Run **Variables** or **Secrets** for Flask session security (override the default in code).
- **BQ_CREDENTIALS_PATH** – Do **not** set in production. Only for local dev when using a key file outside the repo.

## 4. Local development (run from repo root, not this folder)

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
