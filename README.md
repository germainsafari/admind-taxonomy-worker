# admind-taxonomy-worker

Cloud Run Job worker for Admind's data organisation pipeline. It uses LLMs to classify documents against active projects and generate internal project wiki pages from matched source documents.

## What it does

The worker supports three job types, selected via the `JOB_TYPE` environment variable:

| Job type | Description |
|----------|-------------|
| `taxonomy-sync` | Fetches active projects and candidate documents from BigQuery, classifies document–project matches with an LLM, and writes results to `project_document_map` and `taxonomy_runs`. |
| `wiki-generate` | Generates wiki pages for all active projects and stores them in Firestore (`wiki` collection). |
| `wiki-generate-one` | Generates a wiki page for a single project. Requires `PROJECT_ID_TO_GENERATE`. |

### Data flow

```
BigQuery (projects, documents)
        │
        ▼
   LLM classifier (Gemini or OpenAI)
        │
        ▼
BigQuery (project_document_map, taxonomy_runs)
        │
        ▼
   LLM wiki writer
        │
        ▼
Firestore (wiki) + BigQuery (wiki_generation_runs)
```

## Requirements

- Python 3.11
- Google Cloud project with BigQuery, Firestore, and Vertex AI enabled
- Service account with access to those services
- (Optional) OpenAI API key for fallback or primary LLM usage

## Local development

1. Create a virtual environment and install dependencies:

   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Copy the example environment file and fill in your values:

   ```bash
   cp .env.example .env
   ```

3. Authenticate with Google Cloud (Application Default Credentials):

   ```bash
   gcloud auth application-default login
   ```

4. Run a job locally:

   ```bash
   # Taxonomy sync (default)
   python main.py

   # Generate wikis for all active projects
   JOB_TYPE=wiki-generate python main.py

   # Generate wiki for one project
   JOB_TYPE=wiki-generate-one PROJECT_ID_TO_GENERATE=<project-id> python main.py
   ```

   On Windows PowerShell, set env vars before running:

   ```powershell
   $env:JOB_TYPE = "taxonomy-sync"
   python main.py
   ```

> **Note:** `.env` is for local development only. Cloud Run does not read `.env` from the container image — configure environment variables on the service or use Secret Manager.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROJECT_ID` | `admind-data-organisation` | GCP project ID |
| `DATASET` | `admind_data_organisation` | BigQuery dataset |
| `LOCATION` | `europe-west4` | Vertex AI region |
| `LLM_PROVIDER` | `auto` | `gemini`, `openai`, or `auto` (Gemini first, OpenAI fallback) |
| `MODEL_NAME` | `gemini-2.5-flash` | Gemini model name |
| `OPENAI_API_KEY` | — | Required when using OpenAI |
| `OPENAI_MODEL` | `gpt-4o-mini` | OpenAI model name |
| `JOB_TYPE` | `taxonomy-sync` | Job to run (`taxonomy-sync`, `wiki-generate`, `wiki-generate-one`) |
| `PROJECT_LIMIT` | `20` | Max active projects to process |
| `DOCUMENT_LIMIT` | `200` | Max candidate documents to load |
| `PROJECT_ID_TO_GENERATE` | — | Required for `wiki-generate-one` |

## Deployment

The project is deployed as a **Cloud Run Job** named `admind-taxonomy-worker` via Cloud Build.

```bash
gcloud builds submit --config cloudbuild.yaml
```

The build pipeline:

1. Builds a Docker image from `Dockerfile`
2. Pushes to Artifact Registry (`europe-west4-docker.pkg.dev/...`)
3. Creates or updates the Cloud Run Job in `europe-west4`

Default substitutions in `cloudbuild.yaml`:

- **Region:** `europe-west4`
- **Service account:** `project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com`
- **Task timeout:** 3600s, single task, no retries

Trigger a job execution after deploy:

```bash
gcloud run jobs execute admind-taxonomy-worker --region=europe-west4
```

Pass overrides at execution time if needed:

```bash
gcloud run jobs execute admind-taxonomy-worker \
  --region=europe-west4 \
  --update-env-vars=JOB_TYPE=wiki-generate
```

## Project structure

```
.
├── main.py              # Job entrypoint and business logic
├── requirements.txt     # Python dependencies
├── Dockerfile           # Container image for Cloud Run Job
├── Dockerfile.job       # Alias of Dockerfile
├── cloudbuild.yaml      # CI/CD pipeline
├── .env.example         # Local env template
└── README.md
```

## BigQuery tables

The worker reads from and writes to tables in the configured dataset:

- **Reads:** `projects`, `documents`, `project_document_map`
- **Writes:** `project_document_map`, `taxonomy_runs`, `wiki_generation_runs`

Classification matches with confidence below `0.75` are excluded from `project_document_map`.
