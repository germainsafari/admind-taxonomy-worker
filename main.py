"""
admind-taxonomy-worker  —  Cloud Run Job
Pipeline: Scoro → BigQuery.projects
          Discovery Engine → BigQuery.documents
          LLM classifier → BigQuery.project_document_map
          LLM wiki writer → Firestore.wiki
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import requests
import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import bigquery
from google.cloud import firestore
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_ID = os.getenv("PROJECT_ID", "admind-data-organisation")
DATASET = os.getenv("DATASET", "admind_data_organisation")
LOCATION = os.getenv("LOCATION", "europe-west4")
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.5-flash")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
# auto  → Gemini API key → Vertex AI Gemini → OpenAI
# gemini → Gemini API key → Vertex AI Gemini (no OpenAI fallback)
# openai → OpenAI only
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "auto").lower()

SCORO_BASE_URL = os.getenv("SCORO_BASE_URL", "").rstrip("/")
SCORO_API_KEY = os.getenv("SCORO_API_KEY", "")
SCORO_COMPANY_ACCOUNT_ID = os.getenv("SCORO_COMPANY_ACCOUNT_ID", "")

DE_PROJECT = (
    os.getenv("DISCOVERY_ENGINE_PROJECT_NUMBER")
    or os.getenv("DISCOVERY_ENGINE_PROJECT")
    or PROJECT_ID
)
DE_LOCATION = os.getenv("DISCOVERY_ENGINE_LOCATION", "global")
DE_COLLECTION = os.getenv("DISCOVERY_ENGINE_COLLECTION", "default_collection")
DE_ENGINE_ID = os.getenv("DISCOVERY_ENGINE_ENGINE_ID", "")
DE_SERVING_CONFIG = os.getenv("DISCOVERY_ENGINE_SERVING_CONFIG", "default_search")
# Workspace (Google Drive) data stores reject service-account search. When set,
# the worker impersonates this licensed Workspace user via domain-wide delegation.
DISCOVERY_IMPERSONATE_USER = os.getenv("DISCOVERY_IMPERSONATE_USER", "").strip()

# ---------------------------------------------------------------------------
# GCP clients
# ---------------------------------------------------------------------------

bq = bigquery.Client(project=PROJECT_ID)
db = firestore.Client(project=PROJECT_ID, database=os.getenv("FIRESTORE_DATABASE", "(default)"))

# ---------------------------------------------------------------------------
# LLM layer
# Priority: Gemini Developer API (GEMINI_API_KEY) → Vertex AI Gemini → OpenAI
# ---------------------------------------------------------------------------

_vertex_model = None
_vertex_unavailable = False
_openai_client = None
_active_llm_label = None


def _gemini_api_key() -> str:
    return os.getenv("GEMINI_API_KEY", "").strip()


def _openai_api_key() -> str:
    return os.getenv("OPENAI_API_KEY", "").strip()


# ── Gemini Developer API (AI Studio / google-generativeai) ──────────────────

def _generate_with_gemini_api_key(prompt: str, *, temperature: float, json_mode: bool) -> str:
    import google.generativeai as genai  # lazy import

    genai.configure(api_key=_gemini_api_key())
    config_kwargs = {"temperature": temperature}
    if json_mode:
        config_kwargs["response_mime_type"] = "application/json"
    model = genai.GenerativeModel(MODEL_NAME)
    response = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(**config_kwargs),
    )
    return response.text


# ── Vertex AI Gemini (service account, no API key needed) ───────────────────

def _get_vertex_model():
    global _vertex_model, _vertex_unavailable
    if _vertex_unavailable:
        return None
    if _vertex_model is not None:
        return _vertex_model
    try:
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        _vertex_model = GenerativeModel(MODEL_NAME)
        return _vertex_model
    except Exception as e:
        _vertex_unavailable = True
        logger.warning("Vertex AI Gemini unavailable: %s", e)
        return None


def _generate_with_vertex_gemini(prompt: str, *, temperature: float, json_mode: bool) -> str:
    model = _get_vertex_model()
    if model is None:
        raise RuntimeError("Vertex AI Gemini is not available")
    config_kwargs = {"temperature": temperature}
    if json_mode:
        config_kwargs["response_mime_type"] = "application/json"
    response = model.generate_content(
        prompt,
        generation_config=GenerationConfig(**config_kwargs),
    )
    return response.text


# ── OpenAI ──────────────────────────────────────────────────────────────────

def _get_openai_client():
    global _openai_client
    if _openai_client is not None:
        return _openai_client
    api_key = _openai_api_key()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    from openai import OpenAI
    _openai_client = OpenAI(api_key=api_key)
    return _openai_client


def _generate_with_openai(prompt: str, *, temperature: float, json_mode: bool) -> str:
    client = _get_openai_client()
    kwargs = {
        "model": OPENAI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    response = client.chat.completions.create(**kwargs)
    return response.choices[0].message.content or ""


# ── Router ───────────────────────────────────────────────────────────────────

def generate_text(prompt: str, *, temperature: float = 0, json_mode: bool = False) -> str:
    """
    LLM_PROVIDER=auto   → Gemini API key → Vertex AI Gemini → OpenAI
    LLM_PROVIDER=gemini → Gemini API key → Vertex AI Gemini  (no OpenAI fallback)
    LLM_PROVIDER=openai → OpenAI only
    """
    global _active_llm_label

    if LLM_PROVIDER == "openai":
        _active_llm_label = f"openai:{OPENAI_MODEL}"
        return _generate_with_openai(prompt, temperature=temperature, json_mode=json_mode)

    gemini_errors: list[str] = []

    # 1. Gemini Developer API (requires GEMINI_API_KEY)
    if _gemini_api_key():
        try:
            _active_llm_label = f"gemini-api:{MODEL_NAME}"
            return _generate_with_gemini_api_key(prompt, temperature=temperature, json_mode=json_mode)
        except Exception as e:
            gemini_errors.append(f"Gemini API key: {e}")
            logger.warning("Gemini Developer API failed, trying next option: %s", e)

    # 2. Vertex AI Gemini (uses service account — no API key needed)
    try:
        _active_llm_label = f"gemini-vertex:{MODEL_NAME}"
        return _generate_with_vertex_gemini(prompt, temperature=temperature, json_mode=json_mode)
    except Exception as e:
        gemini_errors.append(f"Vertex AI Gemini: {e}")
        logger.warning("Vertex AI Gemini failed: %s", e)

    # 3. OpenAI fallback (only when LLM_PROVIDER=auto)
    if LLM_PROVIDER == "gemini":
        raise RuntimeError(
            f"LLM_PROVIDER=gemini but all Gemini options failed: {'; '.join(gemini_errors)}"
        )

    if not _openai_api_key():
        raise RuntimeError(
            f"All Gemini options failed and OPENAI_API_KEY is not set. "
            f"Errors: {'; '.join(gemini_errors)}"
        )

    logger.warning("All Gemini options failed — falling back to OpenAI")
    _active_llm_label = f"openai:{OPENAI_MODEL}"
    return _generate_with_openai(prompt, temperature=temperature, json_mode=json_mode)


def active_llm_label() -> str:
    return _active_llm_label or "unknown"


def classifier_matching_method() -> str:
    return "openai_classification" if active_llm_label().startswith("openai:") else "gemini_classification"

# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_query(sql: str, params=None) -> list:
    job_config = None
    if params:
        job_config = bigquery.QueryJobConfig(query_parameters=params)
    return list(bq.query(sql, job_config=job_config).result())


def insert_rows(table_name: str, rows: list[dict]):
    if not rows:
        return
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    errors = bq.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for {table_name}: {errors}")


def _row(row, field: str, default=""):
    """Safely read a field from a BigQuery row; returns default if the column is absent."""
    try:
        val = getattr(row, field, None)
        return val if val is not None else default
    except Exception:
        return default


def log_pipeline_run(
    job_name: str,
    run_id: str,
    started_at: str,
    status: str,
    records_read: int = 0,
    records_written: int = 0,
    error_message: str | None = None,
):
    """Append-only log to pipeline_runs. Never updates — avoids BQ streaming-buffer error."""
    insert_rows(
        "pipeline_runs",
        [{
            "run_id": run_id,
            "job_name": job_name,
            "started_at": started_at,
            "finished_at": now_iso(),
            "status": status,
            "records_read": records_read,
            "records_written": records_written,
            "error_message": error_message[:1000] if error_message else None,
        }],
    )

# ---------------------------------------------------------------------------
# BigQuery data accessors
# Schema: see SCHEMA.md
# ---------------------------------------------------------------------------


def get_active_projects(limit: int = 20) -> list:
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.projects`
    WHERE LOWER(CAST(status AS STRING)) NOT IN ('done', 'completed', 'cancelled', 'closed')
    LIMIT @limit
    """
    return run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])


def get_candidate_documents(limit: int = 200) -> list:
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.documents`
    LIMIT @limit
    """
    return run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])


def get_project(project_id: str):
    rows = run_query(
        f"SELECT * FROM `{PROJECT_ID}.{DATASET}.projects` WHERE project_id = @pid LIMIT 1",
        [bigquery.ScalarQueryParameter("pid", "STRING", project_id)],
    )
    return rows[0] if rows else None


def get_project_documents(project_id: str, limit: int = 30) -> list:
    sql = f"""
    SELECT d.*
    FROM `{PROJECT_ID}.{DATASET}.project_document_map` m
    JOIN `{PROJECT_ID}.{DATASET}.documents` d ON m.document_id = d.document_id
    WHERE m.project_id = @project_id
    ORDER BY m.confidence_score DESC
    LIMIT @limit
    """
    return run_query(
        sql,
        [
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )

# ---------------------------------------------------------------------------
# SCORO SYNC
# Reads active projects from Scoro API and upserts into BigQuery.projects
# projects schema: see SCHEMA.md
# ---------------------------------------------------------------------------


def _scoro_post(endpoint: str, body_extra: dict | None = None) -> dict:
    """POST to Scoro API v2 with apiKey + company_account_id in the body."""
    if not SCORO_BASE_URL or not SCORO_API_KEY:
        raise RuntimeError("SCORO_BASE_URL and SCORO_API_KEY are required for scoro-sync")
    if not SCORO_COMPANY_ACCOUNT_ID:
        raise RuntimeError(
            "SCORO_COMPANY_ACCOUNT_ID is required. "
            "Find it in Scoro → Settings → Site settings → General."
        )

    body: dict = {
        "apiKey": SCORO_API_KEY.strip(),
        "lang": "eng",
        "company_account_id": SCORO_COMPANY_ACCOUNT_ID.strip(),
    }
    if body_extra:
        body.update(body_extra)

    url = f"{SCORO_BASE_URL}/api/v2/{endpoint}"
    logger.info("Scoro POST %s", url)

    resp = requests.post(
        url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json=body,
        timeout=60,
    )

    if not resp.ok:
        raise RuntimeError(
            f"Scoro API HTTP {resp.status_code} on /{endpoint}: {resp.text[:500]}"
        )

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(
            f"Scoro API non-JSON response on /{endpoint}: {resp.text[:500]}"
        )

    if data.get("status") != "OK":
        raise RuntimeError(f"Scoro API error on /{endpoint}: {data.get('messages') or data}")

    return data


def fetch_scoro_projects(page: int = 1, per_page: int = 50) -> list[dict]:
    """Fetch one page of active projects from Scoro API v2."""
    data = _scoro_post("projects/list", {
        "page": page,
        "per_page": per_page,
        "filter": {"status": "inprogress"},
    })
    return data.get("data", [])


def _safe_date_str(val) -> str | None:
    """Return an ISO date string or None; BQ SAFE.PARSE_DATE handles the rest."""
    if not val:
        return None
    return str(val)[:10]  # keep only YYYY-MM-DD


def normalize_scoro_project(raw: dict) -> dict:
    """Map a Scoro API v2 project object to the BigQuery projects table schema."""
    # v2 uses project_id (not id), project_name (not name), manager_id (not manager object)
    scoro_id = str(raw.get("project_id") or raw.get("id") or "")

    return {
        "project_id": f"scoro_{scoro_id}",
        "scoro_id": scoro_id,
        "project_no": str(raw.get("no", "") or ""),
        "project_name": str(raw.get("project_name") or raw.get("name") or ""),
        "status": str(raw.get("status") or ""),
        # v2 returns manager_id + manager_email, not a name object
        "project_manager": str(raw.get("manager_email") or raw.get("manager_id") or ""),
        "project_members": "",   # not returned in list endpoint; enriched separately if needed
        "start_date": _safe_date_str(raw.get("date") or raw.get("start_date")),
        "due_date": _safe_date_str(raw.get("deadline") or raw.get("due_date")),
        "completed_date": str(raw.get("completed_date") or ""),
        "description": str(raw.get("description") or ""),
        "client_company": str(raw.get("company_name") or ""),
        "project_type": str(raw.get("project_type") or raw.get("c_projecttype") or ""),
        "client_country": str(raw.get("c_clientcountry") or ""),
        "business_area": str(raw.get("c_businessarea") or ""),
        "business_line_division": str(raw.get("c_businesslinedivision") or ""),
        "budget_type": str(raw.get("budget_type") or raw.get("c_budgettype") or ""),
        "po_number": str(raw.get("c_ponumber") or ""),
        "open_po_number": str(raw.get("c_openponr") or ""),
        "related_project": str(raw.get("c_relatedproject") or ""),
        "google_drive_link": str(raw.get("c_drivelink") or ""),
        "project_priority": str(raw.get("c_project_priority") or ""),
    }


def upsert_projects(rows: list[dict]):
    """MERGE Scoro projects into BigQuery.projects (insert new, update existing)."""
    if not rows:
        return

    # Explicit all-STRING schema so the staging table never mis-infers types
    # (e.g. numeric-looking scoro_id as INT64). Dates are cast in the MERGE.
    string_fields = [
        "project_id", "scoro_id", "project_no", "project_name", "status",
        "project_manager", "project_members", "start_date", "due_date",
        "completed_date", "description", "client_company", "project_type",
        "client_country", "business_area", "business_line_division",
        "budget_type", "po_number", "open_po_number", "related_project",
        "google_drive_link", "project_priority",
    ]
    schema = [bigquery.SchemaField(f, "STRING") for f in string_fields]

    tmp = f"{PROJECT_ID}.{DATASET}._tmp_projects_{uuid.uuid4().hex[:8]}"
    bq.load_table_from_json(
        rows, tmp,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema),
    ).result()

    try:
        bq.query(f"""
        MERGE `{PROJECT_ID}.{DATASET}.projects` T
        USING `{tmp}` S
        ON T.project_id = S.project_id
        WHEN MATCHED THEN UPDATE SET
            scoro_id             = S.scoro_id,
            project_no           = S.project_no,
            project_name         = S.project_name,
            status               = S.status,
            project_manager      = S.project_manager,
            project_members      = S.project_members,
            start_date           = SAFE.PARSE_DATE('%Y-%m-%d', S.start_date),
            due_date             = SAFE.PARSE_DATE('%Y-%m-%d', S.due_date),
            completed_date       = S.completed_date,
            description          = S.description,
            client_company       = S.client_company,
            project_type         = S.project_type,
            client_country       = S.client_country,
            business_area        = S.business_area,
            business_line_division = S.business_line_division,
            budget_type          = S.budget_type,
            po_number            = S.po_number,
            open_po_number       = S.open_po_number,
            related_project      = S.related_project,
            google_drive_link    = S.google_drive_link,
            project_priority     = S.project_priority,
            imported_at          = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            project_id, scoro_id, project_no, project_name, status,
            project_manager, project_members,
            start_date, due_date, completed_date, description,
            client_company, project_type, client_country,
            business_area, business_line_division, budget_type,
            po_number, open_po_number, related_project,
            google_drive_link, project_priority, imported_at
        ) VALUES (
            S.project_id, S.scoro_id, S.project_no, S.project_name, S.status,
            S.project_manager, S.project_members,
            SAFE.PARSE_DATE('%Y-%m-%d', S.start_date),
            SAFE.PARSE_DATE('%Y-%m-%d', S.due_date),
            S.completed_date, S.description,
            S.client_company, S.project_type, S.client_country,
            S.business_area, S.business_line_division, S.budget_type,
            S.po_number, S.open_po_number, S.related_project,
            S.google_drive_link, S.project_priority, CURRENT_TIMESTAMP()
        )
        """).result()
    finally:
        bq.delete_table(tmp, not_found_ok=True)


def scoro_sync():
    run_id = str(uuid.uuid4())
    started_at = now_iso()

    try:
        page, all_raw = 1, []
        while True:
            batch = fetch_scoro_projects(page=page, per_page=50)
            if not batch:
                break
            all_raw.extend(batch)
            if len(batch) < 50:
                break
            page += 1

        normalized = [normalize_scoro_project(p) for p in all_raw]
        upsert_projects(normalized)

        logger.info(json.dumps({"status": "ok", "job": "scoro-sync", "projects_upserted": len(normalized)}))
        log_pipeline_run("scoro-sync", run_id, started_at, "success",
                         records_read=len(all_raw), records_written=len(normalized))

    except Exception as e:
        log_pipeline_run("scoro-sync", run_id, started_at, "error", error_message=str(e))
        raise

# ---------------------------------------------------------------------------
# DOCUMENT DISCOVERY
# Searches Discovery Engine per project and upserts into BigQuery.documents
# documents schema: see SCHEMA.md
# ---------------------------------------------------------------------------

_gcp_credentials = None
_runtime_sa_email = None
_impersonated_token = None
_impersonated_token_exp = 0

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def _get_adc_token() -> str:
    """Plain Application Default Credentials token (the attached service account)."""
    global _gcp_credentials
    if _gcp_credentials is None:
        _gcp_credentials, _ = google.auth.default(scopes=[CLOUD_PLATFORM_SCOPE])
    _gcp_credentials.refresh(GoogleAuthRequest())
    return _gcp_credentials.token


def _runtime_service_account_email() -> str:
    """Resolve the email of the service account this job runs as."""
    global _runtime_sa_email
    if _runtime_sa_email:
        return _runtime_sa_email

    # Try the GCE/Cloud Run metadata server first
    try:
        resp = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/"
            "instance/service-accounts/default/email",
            headers={"Metadata-Flavor": "Google"},
            timeout=5,
        )
        if resp.ok and resp.text.strip():
            _runtime_sa_email = resp.text.strip()
            return _runtime_sa_email
    except Exception as e:
        logger.warning("Could not read SA email from metadata server: %s", e)

    # Fall back to credentials property
    creds, _ = google.auth.default(scopes=[CLOUD_PLATFORM_SCOPE])
    email = getattr(creds, "service_account_email", None)
    if not email or email == "default":
        raise RuntimeError(
            "Could not determine the runtime service account email for impersonation"
        )
    _runtime_sa_email = email
    return _runtime_sa_email


def _get_impersonated_token(user_email: str) -> str:
    """
    Mint an access token that acts as `user_email` via domain-wide delegation,
    WITHOUT a service-account key file. Uses the IAM Credentials signJwt API.

    Prerequisites:
      1. The runtime service account has roles/iam.serviceAccountTokenCreator on itself.
      2. A Workspace Super Admin authorized the SA's client ID for the
         cloud-platform scope under Admin Console → Security → API Controls →
         Domain-wide Delegation.
    """
    global _impersonated_token, _impersonated_token_exp

    now = int(time.time())
    if _impersonated_token and now < _impersonated_token_exp - 60:
        return _impersonated_token

    sa_email = _runtime_service_account_email()

    # 1. Build the JWT asserting the impersonated subject
    claims = {
        "iss": sa_email,
        "sub": user_email,
        "scope": CLOUD_PLATFORM_SCOPE,
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }

    # 2. Sign it via the IAM Credentials API (uses the SA's own ADC token)
    sign_url = (
        f"https://iamcredentials.googleapis.com/v1/"
        f"projects/-/serviceAccounts/{sa_email}:signJwt"
    )
    sign_resp = requests.post(
        sign_url,
        headers={
            "Authorization": f"Bearer {_get_adc_token()}",
            "Content-Type": "application/json",
        },
        json={"payload": json.dumps(claims)},
        timeout=30,
    )
    if not sign_resp.ok:
        raise RuntimeError(
            f"signJwt failed (HTTP {sign_resp.status_code}): {sign_resp.text[:500]}. "
            f"Ensure {sa_email} has roles/iam.serviceAccountTokenCreator on itself."
        )
    signed_jwt = sign_resp.json()["signedJwt"]

    # 3. Exchange the signed JWT for an access token
    token_resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": signed_jwt,
        },
        timeout=30,
    )
    if not token_resp.ok:
        raise RuntimeError(
            f"Token exchange failed (HTTP {token_resp.status_code}): {token_resp.text[:500]}. "
            f"Ensure domain-wide delegation is authorized for the SA client ID "
            f"with scope {CLOUD_PLATFORM_SCOPE}."
        )

    data = token_resp.json()
    _impersonated_token = data["access_token"]
    _impersonated_token_exp = now + int(data.get("expires_in", 3600))
    return _impersonated_token


def _get_access_token() -> str:
    """
    Return the token used for Discovery Engine search.
    If DISCOVERY_IMPERSONATE_USER is set, impersonate that Workspace user
    (required for Google Drive / Workspace data stores). Otherwise use ADC.
    """
    if DISCOVERY_IMPERSONATE_USER:
        return _get_impersonated_token(DISCOVERY_IMPERSONATE_USER)
    return _get_adc_token()


def search_discovery_engine(query: str, page_size: int = 10) -> list[dict]:
    if not DE_ENGINE_ID:
        raise RuntimeError("DISCOVERY_ENGINE_ENGINE_ID is required for document-discovery")

    url = (
        f"https://discoveryengine.googleapis.com/v1alpha/"
        f"projects/{DE_PROJECT}/locations/{DE_LOCATION}/collections/{DE_COLLECTION}"
        f"/engines/{DE_ENGINE_ID}/servingConfigs/{DE_SERVING_CONFIG}:search"
    )
    payload = {
        "query": query,
        "pageSize": page_size,
        "spellCorrectionSpec": {"mode": "AUTO"},
        "languageCode": "en-US",
        "contentSearchSpec": {"snippetSpec": {"returnSnippet": True}},
    }

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {_get_access_token()}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )

    if not resp.ok:
        # Surface the actual API error message instead of a generic HTTPError
        raise RuntimeError(
            f"Discovery Engine search failed: HTTP {resp.status_code} — {resp.text[:800]} "
            f"(query={query!r})"
        )

    return resp.json().get("results", [])


def _build_project_query(project) -> str:
    """Build a search query from project metadata (uses actual projects schema)."""
    parts = [
        _row(project, "project_name"),
        _row(project, "project_no"),       # project number / code
        _row(project, "client_company"),   # client name
    ]
    drive = _row(project, "google_drive_link")
    if drive:
        parts.append(drive)
    return " ".join(p for p in parts if p)


def discover_documents_for_project(project) -> list[dict]:
    query = _build_project_query(project)
    if not query.strip():
        return []

    results = search_discovery_engine(query, page_size=int(os.getenv("DOCUMENT_LIMIT", "20")))
    docs = []
    for item in results:
        doc = item.get("document", {})
        derived = doc.get("derivedStructData", {})
        struct = doc.get("structData", {})
        document_id = doc.get("id") or doc.get("name", "")
        if not document_id:
            continue
        snippets = derived.get("snippets", [])
        snippet = snippets[0].get("snippet", "") if snippets else ""
        docs.append({
            "document_id": document_id,
            "source_system": "discovery_engine",
            "source_type": "search_result",
            "title": derived.get("title") or struct.get("title") or document_id,
            "folder_path": "",
            "url": derived.get("link") or struct.get("uri") or struct.get("url") or "",
            "author": "",
            "text_preview": snippet[:1000],
            "full_text": snippet,
        })
    return docs


def upsert_documents(rows: list[dict]):
    """MERGE discovered documents into BigQuery.documents."""
    if not rows:
        return

    string_fields = [
        "document_id", "source_system", "source_type", "title", "folder_path",
        "url", "author", "text_preview", "full_text",
    ]
    schema = [bigquery.SchemaField(f, "STRING") for f in string_fields]

    tmp = f"{PROJECT_ID}.{DATASET}._tmp_docs_{uuid.uuid4().hex[:8]}"
    bq.load_table_from_json(
        rows, tmp,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema),
    ).result()

    try:
        bq.query(f"""
        MERGE `{PROJECT_ID}.{DATASET}.documents` T
        USING `{tmp}` S
        ON T.document_id = S.document_id
        WHEN MATCHED THEN UPDATE SET
            source_system = S.source_system,
            source_type   = S.source_type,
            title         = S.title,
            url           = S.url,
            text_preview  = S.text_preview,
            full_text     = S.full_text,
            imported_at   = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            document_id, source_system, source_type, title, folder_path,
            url, author, text_preview, full_text, imported_at
        ) VALUES (
            S.document_id, S.source_system, S.source_type, S.title, S.folder_path,
            S.url, S.author, S.text_preview, S.full_text, CURRENT_TIMESTAMP()
        )
        """).result()
    finally:
        bq.delete_table(tmp, not_found_ok=True)


def document_discovery():
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    total_discovered = 0

    try:
        projects = get_active_projects(limit=int(os.getenv("PROJECT_LIMIT", "20")))
        failures = 0
        for project in projects:
            name = _row(project, "project_name") or _row(project, "project_id")
            try:
                docs = discover_documents_for_project(project)
                upsert_documents(docs)
                total_discovered += len(docs)
                logger.info("document-discovery: %s → %d docs", name, len(docs))
            except Exception as e:
                failures += 1
                logger.error("document-discovery: project %s failed: %s", name, e)

        if total_discovered == 0 and failures > 0:
            # Every project failed — surface the problem instead of reporting success
            raise RuntimeError(f"document-discovery: all {failures} project searches failed")

        logger.info(json.dumps({
            "status": "ok", "job": "document-discovery",
            "projects_searched": len(projects), "documents_discovered": total_discovered,
        }))
        log_pipeline_run("document-discovery", run_id, started_at, "success",
                         records_read=len(projects), records_written=total_discovered)

    except Exception as e:
        log_pipeline_run("document-discovery", run_id, started_at, "error", error_message=str(e))
        raise

# ---------------------------------------------------------------------------
# TAXONOMY SYNC
# LLM classifies which documents belong to which project
# project_document_map schema: see SCHEMA.md
# ---------------------------------------------------------------------------


def classify_documents_for_project(project, documents) -> list[dict]:
    document_chunks = []
    for doc in documents:
        created_at = _row(doc, "created_at")
        modified_at = _row(doc, "modified_at")
        document_chunks.append({
            "document_id": _row(doc, "document_id"),
            "filename": _row(doc, "title"),
            "folder_path": _row(doc, "folder_path"),
            "author": _row(doc, "author"),
            "created_at": str(created_at) if created_at else None,
            "modified_at": str(modified_at) if modified_at else None,
            "preview": (_row(doc, "text_preview") or "")[:500],
        })

    # Use actual projects schema field names
    project_name = _row(project, "project_name")
    project_no = _row(project, "project_no")
    client_company = _row(project, "client_company")
    project_members = _row(project, "project_members")
    start_date = _row(project, "start_date")
    due_date = _row(project, "due_date")
    description = _row(project, "description")

    prompt = f"""
You are a document classifier for Admind Agency, a creative branding studio.

Project: {project_name} | No: {project_no} | Client: {client_company}
Team: {project_members} | Period: {start_date} to {due_date}
Description: {description}

Below are candidate documents. Return ONLY valid JSON.

Return a JSON object with this exact shape:
{{
  "matches": [
    {{
      "document_id": "string",
      "confidence_score": 0.0,
      "reason": "short reason"
    }}
  ]
}}

Rules:
- Include only documents that clearly belong to this project.
- Do not guess. Exclude documents with no clear match.
- Confidence must be between 0 and 1.

Documents:
{json.dumps(document_chunks, ensure_ascii=False)}
"""

    raw = generate_text(prompt, temperature=0, json_mode=True)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        raise RuntimeError(f"LLM returned invalid JSON: {raw}")

    return parsed.get("matches", [])


def taxonomy_sync():
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    projects_processed = 0
    mappings_created = 0
    documents = []

    try:
        projects = get_active_projects(limit=int(os.getenv("PROJECT_LIMIT", "20")))
        documents = get_candidate_documents(limit=int(os.getenv("DOCUMENT_LIMIT", "200")))

        for project in projects:
            matches = classify_documents_for_project(project, documents)
            rows = [
                {
                    "project_id": _row(project, "project_id"),
                    "document_id": m["document_id"],
                    "confidence_score": float(m.get("confidence_score", 0)),
                    "matching_method": classifier_matching_method(),
                    "classifier_reason": m.get("reason"),
                    "classified_at": now_iso(),
                    "run_id": run_id,
                }
                for m in matches
                if m.get("confidence_score", 0) >= 0.75
            ]
            insert_rows("project_document_map", rows)
            projects_processed += 1
            mappings_created += len(rows)

        logger.info(json.dumps({
            "status": "ok", "job": "taxonomy-sync", "run_id": run_id,
            "projects_processed": projects_processed,
            "documents_processed": len(documents),
            "mappings_created": mappings_created,
        }))
        log_pipeline_run("taxonomy-sync", run_id, started_at, "success",
                         records_read=len(documents), records_written=mappings_created)

    except Exception as e:
        log_pipeline_run("taxonomy-sync", run_id, started_at, "error",
                         records_read=len(documents), records_written=mappings_created,
                         error_message=str(e))
        raise

# ---------------------------------------------------------------------------
# WIKI GENERATION
# Reads matched documents and writes Markdown wiki pages to Firestore
# ---------------------------------------------------------------------------


def generate_wiki_for_project(project_id: str):
    run_id = str(uuid.uuid4())
    started_at = now_iso()

    project = get_project(project_id)
    if not project:
        raise RuntimeError(f"Project not found in BigQuery: {project_id}")

    docs = get_project_documents(project_id)

    source_docs = [
        {
            "document_id": _row(doc, "document_id"),
            "title": _row(doc, "title"),
            "url": _row(doc, "url"),
            "content": (_row(doc, "full_text") or _row(doc, "text_preview") or "")[:12000],
        }
        for doc in docs
    ]

    # Use actual projects schema field names
    project_name = _row(project, "project_name")
    project_no = _row(project, "project_no") or project_id
    client_company = _row(project, "client_company")
    project_members = _row(project, "project_members")

    prompt = f"""
You are a technical documentarian for Admind Agency.

Write the internal wiki page for:
Project: {project_name} ({project_no}) | Client: {client_company}

Using ONLY the source documents below, write Markdown with these sections:
## Overview
## Brief & Objectives
## Team
## Timeline
## Key Decisions
## Design Assets
## Meeting Intelligence
## Source Coverage

Rules:
- Only use information stated in the sources. Do not invent.
- Cite the source document title for each important fact.
- Flag sections with low source coverage.
- If a section has no source support, write "Not found in available sources."

Source documents:
{json.dumps(source_docs, ensure_ascii=False)}
"""

    markdown = generate_text(prompt, temperature=0.2, json_mode=False)
    model_used = active_llm_label()

    db.collection("wiki").document(project_id).set({
        "project_id": project_id,
        "project_name": project_name,
        "project_no": project_no,
        "client_company": client_company,
        "project_members": project_members,
        "markdown": markdown,
        "generated_at": firestore.SERVER_TIMESTAMP,
        "generated_by_model": model_used,
        "source_document_ids": [_row(doc, "document_id") for doc in docs],
    })

    log_pipeline_run("wiki-generate", run_id, started_at, "success",
                     records_read=len(docs), records_written=1)

    logger.info(json.dumps({
        "status": "ok", "job": "wiki-generate",
        "project_id": project_id, "source_document_count": len(docs),
    }))


def wiki_generate_all():
    projects = get_active_projects(limit=int(os.getenv("PROJECT_LIMIT", "20")))
    for project in projects:
        pid = _row(project, "project_id")
        if pid:
            generate_wiki_for_project(pid)

# ---------------------------------------------------------------------------
# FULL SYNC — runs the complete pipeline in order
# ---------------------------------------------------------------------------


def full_sync():
    run_id = str(uuid.uuid4())
    started_at = now_iso()

    steps = [
        ("scoro-sync", scoro_sync),
        ("document-discovery", document_discovery),
        ("taxonomy-sync", taxonomy_sync),
        ("wiki-generate", wiki_generate_all),
    ]

    for step_name, step_fn in steps:
        logger.info("full-sync: starting step %s", step_name)
        try:
            step_fn()
        except Exception as e:
            log_pipeline_run("full-sync", run_id, started_at, "error",
                             error_message=f"Step {step_name} failed: {str(e)[:800]}")
            raise RuntimeError(f"full-sync failed at step '{step_name}': {e}") from e

    log_pipeline_run("full-sync", run_id, started_at, "success")
    logger.info(json.dumps({"status": "ok", "job": "full-sync"}))

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def run_job(job: str):
    dispatch = {
        "scoro-sync": scoro_sync,
        "document-discovery": document_discovery,
        "taxonomy-sync": taxonomy_sync,
        "wiki-generate": wiki_generate_all,
        "full-sync": full_sync,
    }

    if job == "wiki-generate-one":
        project_id = os.getenv("PROJECT_ID_TO_GENERATE")
        if not project_id:
            raise RuntimeError("PROJECT_ID_TO_GENERATE env var is required for wiki-generate-one")
        generate_wiki_for_project(project_id)
        return

    fn = dispatch.get(job)
    if fn is None:
        valid = ", ".join(list(dispatch) + ["wiki-generate-one"])
        raise RuntimeError(f"Unknown JOB_TYPE '{job}'. Valid values: {valid}")
    fn()


if __name__ == "__main__":
    run_job(os.getenv("JOB_TYPE", "taxonomy-sync"))
