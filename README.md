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
| `taxonomy-sync` | LLM classifies which documents belong to which project → `project_document_map` |
| `wiki-generate` | Generate Markdown wiki for all active projects → Firestore `wiki` collection |
| `wiki-generate-one` | Generate wiki for a single project (requires `PROJECT_ID_TO_GENERATE`) |
| `full-sync` | Runs all four steps in order (use for nightly scheduling) |

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
| `DOCUMENT_LIMIT` | `200` | Max documents per search / load |
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

Deployed as a **Cloud Run Job** (not a Service). The container runs `python main.py` and exits — no HTTP server required.

### Via Cloud Build

```bash
gcloud builds submit --config cloudbuild.yaml
```

Pipeline: build image → push to Artifact Registry → create/update Cloud Run Job.

### Manual deploy (with all env vars and secrets)

```bash
gcloud run jobs deploy admind-taxonomy-worker \
  --source . \
  --region europe-west1 \
  --service-account project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com \
  --set-env-vars "PROJECT_ID=admind-data-organisation,DATASET=admind_data_organisation,LOCATION=europe-west4,FIRESTORE_DATABASE=admindfirestore,MODEL_NAME=gemini-2.5-flash,OPENAI_MODEL=gpt-4o-mini,LLM_PROVIDER=auto,JOB_TYPE=full-sync,PROJECT_LIMIT=20,DOCUMENT_LIMIT=10,SCORO_BASE_URL=https://admindagency.scoro.com,SCORO_COMPANY_ACCOUNT_ID=admindagency,DISCOVERY_ENGINE_PROJECT_NUMBER=493121771508,DISCOVERY_ENGINE_LOCATION=global,DISCOVERY_ENGINE_COLLECTION=default_collection,DISCOVERY_ENGINE_ENGINE_ID=gemini-enterprise-admind,DISCOVERY_ENGINE_SERVING_CONFIG=default_search,DISCOVERY_IMPERSONATE_USER=your-user@admindagency.com" \
  --set-secrets "GEMINI_API_KEY=gemini-api-key:latest,OPENAI_API_KEY=openai-api-key:latest,SCORO_API_KEY=scoro-api-token:latest" \
  --task-timeout 3600 \
  --max-retries 0
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

Web app read pattern:

```python
from google.cloud import firestore

db = firestore.Client(project="admind-data-organisation", database="admindfirestore")
doc = db.collection("wiki").document("scoro_566170").get()
if doc.exists:
    wiki = doc.to_dict()
    markdown = wiki["markdown"]
```

## Firestore wiki document shape

| Field | Description |
|-------|-------------|
| `project_id` | BigQuery project ID (e.g. `scoro_566170`) |
| `project_name` | Project display name |
| `project_no` | Scoro project number |
| `client_company` | Client name |
| `project_members` | Team members |
| `markdown` | Generated wiki page (Markdown) |
| `generated_at` | Server timestamp |
| `generated_by_model` | LLM used (e.g. `gemini-api:gemini-2.5-flash`) |
| `source_document_ids` | Document IDs used as sources |

## BigQuery tables

| Table | Role |
|-------|------|
| `projects` | Clean project records (from Scoro) |
| `documents` | Discovered documents (from Discovery Engine) |
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

## Scheduling (optional)

After `full-sync` passes reliably, schedule nightly:

```bash
gcloud scheduler jobs create http taxonomy-full-sync \
  --schedule "0 2 * * *" \
  --uri "https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/admind-data-organisation/jobs/admind-taxonomy-worker:run" \
  --http-method POST \
  --oauth-service-account-email project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com \
  --location europe-west1
```

Ensure the job's default `JOB_TYPE` is `full-sync` before scheduling.
