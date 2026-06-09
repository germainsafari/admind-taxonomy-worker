# admind-taxonomy-worker

Cloud Run Job for Admind's data organisation pipeline. It syncs projects from Scoro, discovers documents from Gemini Enterprise (Google Drive), classifies document–project matches with an LLM, and generates internal wiki pages stored in Firestore for the web app.

## Pipeline overview

```
Scoro API
    │
    ▼
BigQuery.projects          ← scoro-sync

Gemini Enterprise (Discovery Engine / Google Drive)
    │
    ▼
BigQuery.documents         ← document-discovery

BigQuery.projects + documents
    │
    ▼
LLM classifier             ← taxonomy-sync
    │
    ▼
BigQuery.project_document_map

project_document_map + documents
    │
    ▼
LLM wiki writer            ← wiki-generate
    │
    ▼
Firestore.wiki/{project_id}   ← web app reads from here
```

## Job types

Set `JOB_TYPE` to select which step runs:

| Job type | Description |
|----------|-------------|
| `scoro-sync` | Fetch active projects from Scoro API v2 and upsert into `BigQuery.projects` |
| `document-discovery` | Search Gemini Enterprise per project and upsert results into `BigQuery.documents` |
| `taxonomy-sync` | LLM classifies each project's **own** candidate documents → `project_document_map` |
| `wiki-generate` | Generate Markdown wiki **only for projects with mapped documents** → Firestore `wiki` |
| `wiki-generate-one` | Generate wiki for a single project (requires `PROJECT_ID_TO_GENERATE`) |
| `full-sync` | Runs all four steps in order over **active** projects (use for nightly scheduling) |
| `historical-full-sync` | Same pipeline but over **all** projects incl. completed (backfill; sets `PROJECT_MODE=all`, `SCORO_PROJECT_MODE=all`) |

## Requirements

- Python 3.11
- GCP project: `admind-data-organisation`
- BigQuery dataset: `admind_data_organisation`
- Firestore database: `admindfirestore`
- Service account: `project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com`

### IAM roles (service account)

- `roles/bigquery.dataEditor`
- `roles/bigquery.jobUser`
- `roles/datastore.user`
- `roles/aiplatform.user`
- `roles/discoveryengine.viewer`
- `roles/iam.serviceAccountTokenCreator` (on itself — for Workspace impersonation)
- Secret Manager access for `openai-api-key`, `scoro-api-token`, `gemini-api-key`

### Scoro

- API v2 with `SCORO_BASE_URL=https://admindagency.scoro.com` (do **not** include `/api/v2` in the URL)
- `SCORO_COMPANY_ACCOUNT_ID=admindagency`
- `SCORO_API_KEY` from Secret Manager (`scoro-api-token`)

### Gemini Enterprise / Google Drive search

Google Drive data stores **do not allow service-account search**. The worker uses **domain-wide delegation** to impersonate a licensed Workspace user (`DISCOVERY_IMPERSONATE_USER`).

One-time setup (Workspace Super Admin):

1. [admin.google.com](https://admin.google.com) → Security → API controls → Domain-wide delegation → Add new
2. Client ID: service account `uniqueId` (from `gcloud iam service-accounts describe ...`)
3. OAuth scope: `https://www.googleapis.com/auth/cloud-platform`

The impersonated user must have a **Gemini Enterprise** license assigned.

### BigQuery table (create once)

```sql
CREATE TABLE IF NOT EXISTS `admind-data-organisation.admind_data_organisation.pipeline_runs` (
  run_id STRING,
  job_name STRING,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  status STRING,
  records_read INT64,
  records_written INT64,
  error_message STRING
);
```

See [SCHEMA.md](SCHEMA.md) for all table schemas.

## LLM configuration

Priority when `LLM_PROVIDER=auto`:

1. Gemini Developer API (`GEMINI_API_KEY`)
2. Vertex AI Gemini (service account — no key needed)
3. OpenAI (`OPENAI_API_KEY`) as fallback

| `LLM_PROVIDER` | Behavior |
|----------------|----------|
| `auto` | Gemini → Vertex → OpenAI fallback |
| `gemini` | Gemini only, no OpenAI fallback |
| `openai` | OpenAI only |

## Environment variables

Copy [.env.example](.env.example) for local development. Cloud Run uses env vars and Secret Manager — not `.env` files in the image.

| Variable | Default | Description |
|----------|---------|-------------|
| `PROJECT_ID` | `admind-data-organisation` | GCP project |
| `DATASET` | `admind_data_organisation` | BigQuery dataset |
| `LOCATION` | `europe-west4` | Vertex AI region |
| `FIRESTORE_DATABASE` | `admindfirestore` | Firestore database for wiki pages |
| `JOB_TYPE` | `taxonomy-sync` | Job to run (see table above) |
| `LLM_PROVIDER` | `auto` | LLM routing |
| `MODEL_NAME` | `gemini-2.5-flash` | Gemini model |
| `GEMINI_API_KEY` | — | Secret: `gemini-api-key` |
| `OPENAI_API_KEY` | — | Secret: `openai-api-key` |
| `OPENAI_MODEL` | `gpt-4o-mini` | OpenAI model |
| `SCORO_BASE_URL` | — | `https://admindagency.scoro.com` |
| `SCORO_API_KEY` | — | Secret: `scoro-api-token` |
| `SCORO_COMPANY_ACCOUNT_ID` | — | `admindagency` |
| `DISCOVERY_ENGINE_PROJECT_NUMBER` | — | `493121771508` |
| `DISCOVERY_ENGINE_ENGINE_ID` | — | `gemini-enterprise-admind` |
| `DISCOVERY_ENGINE_LOCATION` | `global` | Discovery Engine location |
| `DISCOVERY_ENGINE_COLLECTION` | `default_collection` | Collection name |
| `DISCOVERY_ENGINE_SERVING_CONFIG` | `default_search` | Serving config |
| `DISCOVERY_IMPERSONATE_USER` | — | Licensed Workspace user for Drive search |
| `PROJECT_LIMIT` | `20` | Max projects per run |
| `DOCUMENT_LIMIT` | `25` | Discovery Engine page size per search query |
| `CANDIDATE_LIMIT` | `50` | Max per-project candidate docs the classifier sees |
| `PROJECT_MODE` | `active` | Which projects to process: `active` or `all` |
| `SCORO_PROJECT_MODE` | `active` | Which projects to fetch from Scoro: `active` (inprogress) or `all` |
| `PROJECT_ID_TO_GENERATE` | — | Required for `wiki-generate-one` |

## Local development

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # fill in values
gcloud auth application-default login
```

```bash
# Run a specific job
JOB_TYPE=scoro-sync python main.py
JOB_TYPE=document-discovery python main.py
JOB_TYPE=taxonomy-sync python main.py
JOB_TYPE=wiki-generate python main.py
```

Windows PowerShell:

```powershell
$env:JOB_TYPE = "taxonomy-sync"
python main.py
```

## Deployment

Deployed as a **Cloud Run Job** (not a Service). The container runs `python main.py`
and exits — no HTTP server is involved.

> **Do NOT use `gcloud run jobs deploy --source .`**  
> That flag triggers Google Cloud Buildpacks which tries to wrap the app in gunicorn
> and fails because this is a job, not a web service. Always build via Docker as shown
> below.

### Recommended: Cloud Build (CI/CD, one command)

Builds the Docker image, pushes it to Artifact Registry, and updates the Cloud Run
Job in one step:

```bash
gcloud builds submit --config cloudbuild.yaml
```

The first time you run this, the Artifact Registry repository
`europe-west1-docker.pkg.dev/admind-data-organisation/admind` must exist:

```bash
gcloud artifacts repositories create admind \
  --repository-format=docker \
  --location=europe-west1 \
  --project=admind-data-organisation
```

### Alternative: build Docker image then deploy

Use this when you want full control, or if Cloud Build isn't set up yet.

```bash
IMAGE=europe-west1-docker.pkg.dev/admind-data-organisation/admind/admind-taxonomy-worker:latest

# Build and push (run from the repo root)
docker build -t $IMAGE .
docker push $IMAGE

# Create the job (first time only)
gcloud run jobs create admind-taxonomy-worker \
  --image=$IMAGE \
  --region=europe-west1 \
  --service-account=project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com \
  --set-env-vars "PROJECT_ID=admind-data-organisation,DATASET=admind_data_organisation,LOCATION=europe-west4,FIRESTORE_DATABASE=admindfirestore,MODEL_NAME=gemini-2.5-flash,LLM_PROVIDER=auto,JOB_TYPE=full-sync,PROJECT_MODE=active,SCORO_PROJECT_MODE=active,PROJECT_LIMIT=20,DOCUMENT_LIMIT=25,CANDIDATE_LIMIT=50,SCORO_BASE_URL=https://admindagency.scoro.com,SCORO_COMPANY_ACCOUNT_ID=admindagency,DISCOVERY_ENGINE_PROJECT_NUMBER=493121771508,DISCOVERY_ENGINE_LOCATION=global,DISCOVERY_ENGINE_COLLECTION=default_collection,DISCOVERY_ENGINE_ENGINE_ID=gemini-enterprise-admind,DISCOVERY_ENGINE_SERVING_CONFIG=default_search,DISCOVERY_IMPERSONATE_USER=germain.safari@admindagency.com" \
  --set-secrets "GEMINI_API_KEY=gemini-api-key:latest,OPENAI_API_KEY=openai-api-key:latest,SCORO_API_KEY=scoro-api-token:latest" \
  --task-timeout=3600 \
  --max-retries=0

# Subsequent deploys — update image only (env vars/secrets already set)
gcloud run jobs update admind-taxonomy-worker \
  --image=$IMAGE \
  --region=europe-west1
```

## Running and testing

Test each step individually before `full-sync`:

```bash
# 1. Scoro → projects
gcloud run jobs update admind-taxonomy-worker --region europe-west1 \
  --update-env-vars JOB_TYPE=scoro-sync
gcloud run jobs execute admind-taxonomy-worker --region europe-west1 --wait

# 2. Discovery Engine → documents
gcloud run jobs update admind-taxonomy-worker --region europe-west1 \
  --update-env-vars JOB_TYPE=document-discovery
gcloud run jobs execute admind-taxonomy-worker --region europe-west1 --wait

# 3. LLM classification
gcloud run jobs update admind-taxonomy-worker --region europe-west1 \
  --update-env-vars JOB_TYPE=taxonomy-sync
gcloud run jobs execute admind-taxonomy-worker --region europe-west1 --wait

# 4. Wiki → Firestore
gcloud run jobs update admind-taxonomy-worker --region europe-west1 \
  --update-env-vars JOB_TYPE=wiki-generate
gcloud run jobs execute admind-taxonomy-worker --region europe-west1 --wait

# 5. Full pipeline
gcloud run jobs update admind-taxonomy-worker --region europe-west1 \
  --update-env-vars JOB_TYPE=full-sync
gcloud run jobs execute admind-taxonomy-worker --region europe-west1 --wait
```

### Verify in BigQuery

```sql
-- Pipeline runs
SELECT job_name, status, records_read, records_written, started_at
FROM `admind-data-organisation.admind_data_organisation.pipeline_runs`
ORDER BY started_at DESC LIMIT 10;

-- Projects from Scoro
SELECT project_id, project_no, project_name, status
FROM `admind-data-organisation.admind_data_organisation.projects`
ORDER BY imported_at DESC LIMIT 20;

-- Discovered documents
SELECT document_id, title, LEFT(text_preview, 100) AS preview
FROM `admind-data-organisation.admind_data_organisation.documents`
ORDER BY imported_at DESC LIMIT 20;

-- Document–project mappings
SELECT project_id, document_id, confidence_score, classifier_reason
FROM `admind-data-organisation.admind_data_organisation.project_document_map`
ORDER BY classified_at DESC LIMIT 20;
```

### Verify wiki in Firestore

Console: **Firestore → database `admindfirestore` → collection `wiki`**

Each document ID is a `project_id`. Fields include `markdown`, `project_name`, `generated_at`, `source_document_ids`.

Web app read pattern (handles chunked large wikis):

```python
from google.cloud import firestore

db = firestore.Client(project="admind-data-organisation", database="admindfirestore")

def load_wiki_markdown(project_id: str) -> str | None:
    ref = db.collection("wiki").document(project_id)
    snap = ref.get()
    if not snap.exists:
        return None
    wiki = snap.to_dict()
    if not wiki.get("markdown_chunked"):
        return wiki.get("markdown", "")
    parts = []
    for chunk in ref.collection("markdown_chunks").order_by("index").stream():
        parts.append(chunk.to_dict().get("content", ""))
    return "".join(parts)

markdown = load_wiki_markdown("scoro_566170")
```

Large wikis (> ~900 KB) are split into subcollection `wiki/{project_id}/markdown_chunks/{0,1,2,...}`.
The main doc still has chunk 0 in `markdown` for backward compatibility.

## Firestore wiki document shape

| Field | Description |
|-------|-------------|
| `project_id` | BigQuery project ID (e.g. `scoro_566170`) |
| `project_name` | Project display name |
| `project_no` | Scoro project number |
| `client_company` | Client name |
| `project_members` | Team members |
| `markdown` | Wiki Markdown (full page, or chunk 0 when chunked) |
| `markdown_chunked` | `true` when additional chunks exist in subcollection |
| `markdown_chunk_count` | Total number of chunks |
| `markdown_total_bytes` | Full wiki size in bytes |
| `wiki_status` | `generated`, `generated_chunked`, or `no_sources` |
| `generated_at` | Server timestamp |
| `generated_by_model` | LLM used (e.g. `gemini-api:gemini-2.5-flash`) |
| `source_document_ids` | Document IDs used as sources |

## BigQuery tables

| Table | Role |
|-------|------|
| `projects` | Clean project records (from Scoro) |
| `documents` | Discovered documents (from Discovery Engine) |
| `project_document_candidates` | Per-project discovery candidates (preserves project → document link) |
| `project_document_map` | LLM-classified project ↔ document links |
| `pipeline_runs` | Append-only job execution log |
| `project_raw` | Legacy raw Scoro export (not written by this worker) |
| `source_items` | Project-level source links (manual, not written by this worker) |

Full schemas: [SCHEMA.md](SCHEMA.md)

Classification matches with confidence below **0.75** are excluded from `project_document_map`.

## Project structure

```
.
├── main.py              # Job entrypoint and pipeline logic
├── requirements.txt     # Python dependencies
├── Dockerfile           # Cloud Run Job container
├── Dockerfile.job       # Alias of Dockerfile
├── cloudbuild.yaml      # CI/CD pipeline
├── SCHEMA.md            # BigQuery table schemas
├── .env.example         # Local env template
└── README.md
```

## Scheduling

Cloud Run Jobs are triggered on a schedule via **Cloud Scheduler**. Set the job's
default env vars first, grant the scheduler identity `run.invoker`, then create the
schedule.

```bash
# 1. Set nightly defaults on the job
gcloud run jobs update admind-taxonomy-worker \
  --region europe-west1 \
  --update-env-vars JOB_TYPE=full-sync,PROJECT_MODE=active,SCORO_PROJECT_MODE=active,PROJECT_LIMIT=20,DOCUMENT_LIMIT=25,CANDIDATE_LIMIT=50

# 2. Allow the service account to run the job
gcloud run jobs add-iam-policy-binding admind-taxonomy-worker \
  --region europe-west1 \
  --member="serviceAccount:project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

# 3. Nightly active sync at 02:00 Europe/Warsaw
gcloud scheduler jobs create http admind-taxonomy-worker-nightly \
  --location europe-west1 \
  --schedule "0 2 * * *" \
  --time-zone "Europe/Warsaw" \
  --uri "https://run.googleapis.com/v2/projects/admind-data-organisation/locations/europe-west1/jobs/admind-taxonomy-worker:run" \
  --http-method POST \
  --oauth-service-account-email project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com
```

Optional **weekly historical backfill** (Sundays 03:00). Because the scheduler
overrides the container args, pass the job type via an override:

```bash
gcloud scheduler jobs create http admind-taxonomy-worker-weekly-backfill \
  --location europe-west1 \
  --schedule "0 3 * * 0" \
  --time-zone "Europe/Warsaw" \
  --uri "https://run.googleapis.com/v2/projects/admind-data-organisation/locations/europe-west1/jobs/admind-taxonomy-worker:run" \
  --http-method POST \
  --oauth-service-account-email project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com \
  --headers "Content-Type=application/json" \
  --message-body '{"overrides":{"containerOverrides":[{"env":[{"name":"JOB_TYPE","value":"historical-full-sync"}]}]}}'
```

Test and verify:

```bash
gcloud scheduler jobs run admind-taxonomy-worker-nightly --location europe-west1
gcloud run jobs executions list --job admind-taxonomy-worker --region europe-west1
```
