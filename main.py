import json
import logging
import os
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
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "auto").lower()

SCORO_BASE_URL = os.getenv("SCORO_BASE_URL", "").rstrip("/")
SCORO_API_KEY = os.getenv("SCORO_API_KEY", "")
SCORO_COMPANY_ACCOUNT_ID = os.getenv("SCORO_COMPANY_ACCOUNT_ID", "")

DE_PROJECT = os.getenv("DISCOVERY_ENGINE_PROJECT_NUMBER") or os.getenv("DISCOVERY_ENGINE_PROJECT") or PROJECT_ID
DE_LOCATION = os.getenv("DISCOVERY_ENGINE_LOCATION", "global")
DE_COLLECTION = os.getenv("DISCOVERY_ENGINE_COLLECTION", "default_collection")
DE_ENGINE_ID = os.getenv("DISCOVERY_ENGINE_ENGINE_ID", "")
DE_SERVING_CONFIG = os.getenv("DISCOVERY_ENGINE_SERVING_CONFIG", "default_search")

# ---------------------------------------------------------------------------
# GCP clients
# ---------------------------------------------------------------------------

bq = bigquery.Client(project=PROJECT_ID)
db = firestore.Client(project=PROJECT_ID)

# ---------------------------------------------------------------------------
# LLM layer
# ---------------------------------------------------------------------------

_gemini_model = None
_gemini_unavailable = False
_openai_client = None
_active_llm_label = None


def _openai_api_key():
    return os.getenv("OPENAI_API_KEY", "").strip()


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


def _get_gemini_model():
    global _gemini_model, _gemini_unavailable
    if _gemini_unavailable:
        return None
    if _gemini_model is not None:
        return _gemini_model
    try:
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        _gemini_model = GenerativeModel(MODEL_NAME)
        return _gemini_model
    except Exception as e:
        _gemini_unavailable = True
        logger.warning("Gemini unavailable, will use OpenAI fallback: %s", e)
        return None


def _generate_with_gemini(prompt: str, *, temperature: float, json_mode: bool) -> str:
    gemini = _get_gemini_model()
    if gemini is None:
        raise RuntimeError("Gemini is not available")
    config_kwargs = {"temperature": temperature}
    if json_mode:
        config_kwargs["response_mime_type"] = "application/json"
    response = gemini.generate_content(
        prompt,
        generation_config=GenerationConfig(**config_kwargs),
    )
    return response.text


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


def generate_text(prompt: str, *, temperature: float = 0, json_mode: bool = False) -> str:
    """Generate text, preferring Gemini and falling back to OpenAI when LLM_PROVIDER=auto."""
    global _active_llm_label

    if LLM_PROVIDER == "openai":
        _active_llm_label = f"openai:{OPENAI_MODEL}"
        return _generate_with_openai(prompt, temperature=temperature, json_mode=json_mode)

    if LLM_PROVIDER == "gemini":
        _active_llm_label = f"gemini:{MODEL_NAME}"
        return _generate_with_gemini(prompt, temperature=temperature, json_mode=json_mode)

    try:
        _active_llm_label = f"gemini:{MODEL_NAME}"
        return _generate_with_gemini(prompt, temperature=temperature, json_mode=json_mode)
    except Exception as gemini_error:
        if not _openai_api_key():
            raise RuntimeError(
                "Gemini failed and OPENAI_API_KEY is not set for fallback"
            ) from gemini_error
        logger.warning("Gemini failed, using OpenAI fallback: %s", gemini_error)
        _active_llm_label = f"openai:{OPENAI_MODEL}"
        return _generate_with_openai(prompt, temperature=temperature, json_mode=json_mode)


def active_llm_label() -> str:
    return _active_llm_label or "unknown"


def classifier_matching_method() -> str:
    return "openai_classification" if active_llm_label().startswith("openai:") else "gemini_classification"

# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def run_query(sql: str, params=None):
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


def _row(row, field, default=""):
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
    """Append-only log to pipeline_runs. Never updates existing rows (avoids BQ streaming-buffer error)."""
    insert_rows(
        "pipeline_runs",
        [
            {
                "run_id": run_id,
                "job_name": job_name,
                "started_at": started_at,
                "finished_at": now_iso(),
                "status": status,
                "records_read": records_read,
                "records_written": records_written,
                "error_message": error_message[:1000] if error_message else None,
            }
        ],
    )

# ---------------------------------------------------------------------------
# BigQuery data accessors
# ---------------------------------------------------------------------------


def get_active_projects(limit: int = 20):
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.projects`
    WHERE LOWER(CAST(status AS STRING)) NOT IN ('completed', 'closed', 'cancelled')
    LIMIT @limit
    """
    return run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])


def get_candidate_documents(limit: int = 200):
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


def get_project_documents(project_id: str, limit: int = 30):
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
# Reads active projects from Scoro API and upserts them into BigQuery.projects
# ---------------------------------------------------------------------------


def _scoro_headers() -> dict:
    return {"Content-Type": "application/json", "Accept": "application/json"}


def _scoro_body(extra: dict | None = None) -> dict:
    body: dict = {
        "apiKey": SCORO_API_KEY,
        "lang": "eng",
    }
    if SCORO_COMPANY_ACCOUNT_ID:
        body["company_account_id"] = SCORO_COMPANY_ACCOUNT_ID
    if extra:
        body.update(extra)
    return body


def fetch_scoro_projects(page: int = 1, per_page: int = 50) -> list[dict]:
    """Fetch one page of projects from the Scoro v2 API."""
    if not SCORO_BASE_URL or not SCORO_API_KEY:
        raise RuntimeError("SCORO_BASE_URL and SCORO_API_KEY are required for scoro-sync")

    url = f"{SCORO_BASE_URL}/api/v2/projects/list"
    body = _scoro_body({
        "page": page,
        "per_page": per_page,
        "filter": {"status": "inprogress"},
    })

    resp = requests.post(url, headers=_scoro_headers(), json=body, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("status") == "OK":
        raise RuntimeError(f"Scoro API error: {data.get('messages') or data}")

    return data.get("data", [])


def normalize_scoro_project(raw: dict) -> dict:
    """Map a Scoro project object to the BigQuery projects table schema."""
    owner = raw.get("owner_user", {}) or {}
    team = raw.get("team_users", []) or []
    team_names = ", ".join(
        f"{m.get('firstname', '')} {m.get('lastname', '')}".strip()
        for m in team
        if m.get("firstname") or m.get("lastname")
    )

    return {
        "project_id": f"scoro_{raw['id']}",
        "scoro_id": str(raw.get("id", "")),
        "project_code": str(raw.get("no", "") or raw.get("project_code", "")),
        "project_name": raw.get("name", ""),
        "client_name": (raw.get("company_name") or raw.get("client_name") or ""),
        "status": raw.get("status", ""),
        "project_manager": f"{owner.get('firstname', '')} {owner.get('lastname', '')}".strip(),
        "team_members": team_names,
        "start_date": str(raw.get("start_date") or ""),
        "end_date": str(raw.get("end_date") or raw.get("deadline") or ""),
        "description": raw.get("description", "") or "",
        "google_drive_link": raw.get("drive_url") or raw.get("external_url") or "",
        "synced_at": now_iso(),
    }


def upsert_projects(rows: list[dict]):
    """MERGE Scoro projects into BigQuery.projects (insert new, update existing)."""
    if not rows:
        return

    tmp_table = f"{PROJECT_ID}.{DATASET}._tmp_projects_{uuid.uuid4().hex[:8]}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    load_job = bq.load_table_from_json(rows, tmp_table, job_config=job_config)
    load_job.result()

    try:
        merge_sql = f"""
        MERGE `{PROJECT_ID}.{DATASET}.projects` T
        USING `{tmp_table}` S
        ON T.project_id = S.project_id
        WHEN MATCHED THEN UPDATE SET
            scoro_id          = S.scoro_id,
            project_code      = S.project_code,
            project_name      = S.project_name,
            client_name       = S.client_name,
            status            = S.status,
            project_manager   = S.project_manager,
            team_members      = S.team_members,
            start_date        = S.start_date,
            end_date          = S.end_date,
            description       = S.description,
            google_drive_link = S.google_drive_link,
            synced_at         = S.synced_at
        WHEN NOT MATCHED THEN INSERT ROW
        """
        bq.query(merge_sql).result()
    finally:
        bq.delete_table(tmp_table, not_found_ok=True)


def scoro_sync():
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    total_written = 0

    try:
        page = 1
        all_projects: list[dict] = []
        while True:
            batch = fetch_scoro_projects(page=page, per_page=50)
            if not batch:
                break
            all_projects.extend(batch)
            if len(batch) < 50:
                break
            page += 1

        normalized = [normalize_scoro_project(p) for p in all_projects]
        upsert_projects(normalized)
        total_written = len(normalized)

        logger.info(json.dumps({"status": "ok", "job": "scoro-sync", "projects_upserted": total_written}))
        log_pipeline_run("scoro-sync", run_id, started_at, "success",
                         records_read=len(all_projects), records_written=total_written)

    except Exception as e:
        log_pipeline_run("scoro-sync", run_id, started_at, "error", error_message=str(e))
        raise

# ---------------------------------------------------------------------------
# DOCUMENT DISCOVERY
# Searches Gemini Enterprise (Discovery Engine) for documents related to each
# active project and upserts results into BigQuery.documents
# ---------------------------------------------------------------------------

_gcp_credentials = None


def _get_access_token() -> str:
    global _gcp_credentials
    if _gcp_credentials is None:
        _gcp_credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
    _gcp_credentials.refresh(GoogleAuthRequest())
    return _gcp_credentials.token


def search_discovery_engine(query: str, page_size: int = 10) -> list[dict]:
    """Call Discovery Engine Search REST API and return raw result items."""
    if not DE_ENGINE_ID:
        raise RuntimeError("DISCOVERY_ENGINE_ENGINE_ID is required for document-discovery")

    url = (
        f"https://discoveryengine.googleapis.com/v1alpha/"
        f"projects/{DE_PROJECT}/locations/{DE_LOCATION}/collections/{DE_COLLECTION}"
        f"/engines/{DE_ENGINE_ID}/servingConfigs/{DE_SERVING_CONFIG}:search"
    )

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {_get_access_token()}",
            "Content-Type": "application/json",
        },
        json={
            "query": query,
            "pageSize": page_size,
            "queryExpansionSpec": {"condition": "AUTO"},
            "spellCorrectionSpec": {"mode": "AUTO"},
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def _build_project_query(project) -> str:
    parts = [
        _row(project, "project_name"),
        _row(project, "project_code"),
        _row(project, "client_name"),
    ]
    drive = _row(project, "google_drive_link")
    if drive:
        parts.append(drive)
    return " ".join(p for p in parts if p)


def discover_documents_for_project(project) -> list[dict]:
    """Search Discovery Engine for documents related to a project."""
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
            "discovered_at": now_iso(),
        })

    return docs


def upsert_documents(rows: list[dict]):
    """MERGE discovered documents into BigQuery.documents."""
    if not rows:
        return

    tmp_table = f"{PROJECT_ID}.{DATASET}._tmp_docs_{uuid.uuid4().hex[:8]}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    load_job = bq.load_table_from_json(rows, tmp_table, job_config=job_config)
    load_job.result()

    try:
        merge_sql = f"""
        MERGE `{PROJECT_ID}.{DATASET}.documents` T
        USING `{tmp_table}` S
        ON T.document_id = S.document_id
        WHEN MATCHED THEN UPDATE SET
            source_system = S.source_system,
            source_type   = S.source_type,
            title         = S.title,
            url           = S.url,
            text_preview  = S.text_preview,
            full_text     = S.full_text,
            discovered_at = S.discovered_at
        WHEN NOT MATCHED THEN INSERT ROW
        """
        bq.query(merge_sql).result()
    finally:
        bq.delete_table(tmp_table, not_found_ok=True)


def document_discovery():
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    total_discovered = 0

    try:
        projects = get_active_projects(limit=int(os.getenv("PROJECT_LIMIT", "20")))

        for project in projects:
            project_name = _row(project, "project_name") or _row(project, "project_id")
            docs = discover_documents_for_project(project)
            upsert_documents(docs)
            total_discovered += len(docs)
            logger.info("document-discovery: %s → %d documents", project_name, len(docs))

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
# Classifies which documents belong to which project using an LLM
# ---------------------------------------------------------------------------


def classify_documents_for_project(project, documents) -> list[dict]:
    document_chunks = []
    for doc in documents:
        created_at = _row(doc, "created_at")
        modified_at = _row(doc, "modified_at")
        document_chunks.append({
            "document_id": _row(doc, "document_id"),
            "filename": _row(doc, "title") or _row(doc, "name") or _row(doc, "filename"),
            "folder_path": _row(doc, "folder_path") or _row(doc, "path"),
            "author": _row(doc, "author"),
            "created_at": str(created_at) if created_at else None,
            "modified_at": str(modified_at) if modified_at else None,
            "preview": (_row(doc, "text_preview") or _row(doc, "preview") or "")[:500],
        })

    project_name = _row(project, "project_name") or _row(project, "name")
    project_code = _row(project, "project_code") or _row(project, "code") or _row(project, "project_id")
    client_name = _row(project, "client_name") or _row(project, "client")
    team_members = _row(project, "team_members") or _row(project, "team")
    start_date = _row(project, "start_date")
    end_date = _row(project, "end_date")
    description = _row(project, "description") or _row(project, "summary")

    prompt = f"""
You are a document classifier for Admind Agency, a creative branding studio.

Project: {project_name} | Code: {project_code} | Client: {client_name}
Team: {team_members} | Period: {start_date} to {end_date}
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
# Reads matched documents per project and writes a Markdown wiki to Firestore
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

    project_name = _row(project, "project_name") or _row(project, "name")
    project_code = _row(project, "project_code") or _row(project, "code") or project_id
    client_name = _row(project, "client_name") or _row(project, "client")

    prompt = f"""
You are a technical documentarian for Admind Agency.

Write the internal wiki page for:
Project: {project_name} ({project_code}) | Client: {client_name}

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
        "project_code": project_code,
        "client_name": client_name,
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
# FULL SYNC  — runs the entire pipeline in order
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
            logger.error("full-sync: step %s failed: %s", step_name, e)
            log_pipeline_run("full-sync", run_id, started_at, "error",
                             error_message=f"Step {step_name} failed: {str(e)[:800]}")
            raise

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
        raise RuntimeError(
            f"Unknown JOB_TYPE '{job}'. Valid values: {', '.join(list(dispatch) + ['wiki-generate-one'])}"
        )
    fn()


if __name__ == "__main__":
    job = os.getenv("JOB_TYPE", "taxonomy-sync")
    run_job(job)
