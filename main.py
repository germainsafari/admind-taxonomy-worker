"""
admind-taxonomy-worker  â€”  Cloud Run Job
Pipeline: Scoro â†’ BigQuery.projects
          Discovery Engine â†’ BigQuery.documents
          LLM classifier â†’ BigQuery.project_document_map
          LLM wiki writer â†’ Firestore.wiki
"""

import json
import logging
import os
import re
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
# Wiki pages benefit from a stronger model than classification does.
WIKI_MODEL_NAME = os.getenv("WIKI_MODEL_NAME", "gemini-2.5-pro")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
# auto  â†’ Gemini API key â†’ Vertex AI Gemini â†’ OpenAI
# gemini â†’ Gemini API key â†’ Vertex AI Gemini (no OpenAI fallback)
# openai â†’ OpenAI only
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
# Priority: Gemini Developer API (GEMINI_API_KEY) â†’ Vertex AI Gemini â†’ OpenAI
# ---------------------------------------------------------------------------

_vertex_model = None
_vertex_unavailable = False
_openai_client = None
_active_llm_label = None


def _gemini_api_key() -> str:
    return os.getenv("GEMINI_API_KEY", "").strip()


def _openai_api_key() -> str:
    return os.getenv("OPENAI_API_KEY", "").strip()


# â”€â”€ Gemini Developer API (AI Studio / google-generativeai) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _generate_with_gemini_api_key(prompt: str, *, temperature: float, json_mode: bool, model_name: str) -> str:
    import google.generativeai as genai  # lazy import

    genai.configure(api_key=_gemini_api_key())
    config_kwargs = {"temperature": temperature}
    if json_mode:
        config_kwargs["response_mime_type"] = "application/json"
    model = genai.GenerativeModel(model_name)
    response = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(**config_kwargs),
    )
    return response.text


# â”€â”€ Vertex AI Gemini (service account, no API key needed) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _get_vertex_model(model_name: str):
    global _vertex_model, _vertex_unavailable
    if _vertex_unavailable:
        return None
    if _vertex_model is not None and _vertex_model.get(model_name):
        return _vertex_model[model_name]
    try:
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        if _vertex_model is None:
            _vertex_model = {}
        _vertex_model[model_name] = GenerativeModel(model_name)
        return _vertex_model[model_name]
    except Exception as e:
        _vertex_unavailable = True
        logger.warning("Vertex AI Gemini unavailable: %s", e)
        return None


def _generate_with_vertex_gemini(prompt: str, *, temperature: float, json_mode: bool, model_name: str) -> str:
    model = _get_vertex_model(model_name)
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


# â”€â”€ OpenAI â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€ Router â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def generate_text(prompt: str, *, temperature: float = 0, json_mode: bool = False,
                  model_name: str | None = None) -> str:
    """
    LLM_PROVIDER=auto   â†’ Gemini API key â†’ Vertex AI Gemini â†’ OpenAI
    LLM_PROVIDER=gemini â†’ Gemini API key â†’ Vertex AI Gemini  (no OpenAI fallback)
    LLM_PROVIDER=openai â†’ OpenAI only
    """
    global _active_llm_label
    model = model_name or MODEL_NAME

    if LLM_PROVIDER == "openai":
        _active_llm_label = f"openai:{OPENAI_MODEL}"
        return _generate_with_openai(prompt, temperature=temperature, json_mode=json_mode)

    gemini_errors: list[str] = []

    # 1. Gemini Developer API (requires GEMINI_API_KEY)
    if _gemini_api_key():
        try:
            _active_llm_label = f"gemini-api:{model}"
            return _generate_with_gemini_api_key(prompt, temperature=temperature, json_mode=json_mode, model_name=model)
        except Exception as e:
            gemini_errors.append(f"Gemini API key: {e}")
            logger.warning("Gemini Developer API failed, trying next option: %s", e)

    # 2. Vertex AI Gemini (uses service account â€” no API key needed)
    try:
        _active_llm_label = f"gemini-vertex:{model}"
        return _generate_with_vertex_gemini(prompt, temperature=temperature, json_mode=json_mode, model_name=model)
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

    logger.warning("All Gemini options failed â€” falling back to OpenAI")
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


_schema_ready = False


def ensure_schema():
    """
    Idempotent DDL run once per process. Creates the per-project candidate
    table and adds the source_url column to documents. Both use IF NOT EXISTS
    so this is safe to call repeatedly and on every deploy.
    """
    global _schema_ready
    if _schema_ready:
        return

    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.project_document_candidates` (
        project_id      STRING,
        document_id     STRING,
        discovery_query STRING,
        rank            INT64,
        title           STRING,
        url             STRING,
        source_url      STRING,
        text_preview    STRING,
        discovered_at   TIMESTAMP,
        run_id          STRING
    )
    """).result()

    # documents.source_url holds the user-facing link (Drive/SharePoint) while
    # documents.url may be an internal gs:// connector URI.
    bq.query(f"""
    ALTER TABLE `{PROJECT_ID}.{DATASET}.documents`
    ADD COLUMN IF NOT EXISTS source_url STRING
    """).result()

    # Columns introduced by the custom-fields / status-label fixes.
    bq.query(f"""
    ALTER TABLE `{PROJECT_ID}.{DATASET}.projects`
    ADD COLUMN IF NOT EXISTS status_name STRING,
    ADD COLUMN IF NOT EXISTS tags STRING,
    ADD COLUMN IF NOT EXISTS project_manager_name STRING
    """).result()

    _schema_ready = True


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
    """Append-only log to pipeline_runs. Never updates â€” avoids BQ streaming-buffer error."""
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


def _project_mode() -> str:
    """active â†’ only open projects; all â†’ every project incl. completed."""
    return os.getenv("PROJECT_MODE", "active").lower()


def get_active_projects(limit: int = 20) -> list:
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.projects`
    WHERE LOWER(CAST(status AS STRING)) NOT IN ('done', 'completed', 'cancelled', 'closed', 'future')
    ORDER BY start_date DESC
    LIMIT @limit
    """
    return run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])


def get_all_projects(limit: int = 500) -> list:
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.projects`
    ORDER BY start_date DESC
    LIMIT @limit
    """
    return run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])


def get_projects_for_processing(limit: int = 20) -> list:
    """Pick the project set based on PROJECT_MODE (active vs all)."""
    if _project_mode() == "all":
        return get_all_projects(limit=limit)
    return get_active_projects(limit=limit)


def get_candidate_documents_for_project(project_id: str, limit: int = 50) -> list:
    """
    Return ONLY the documents Discovery Engine surfaced for this specific project,
    ordered by discovery rank. This replaces the old global-pool approach that
    classified every project against the same random documents.
    """
    sql = f"""
    SELECT d.*, c.rank AS candidate_rank, c.discovery_query AS candidate_query
    FROM `{PROJECT_ID}.{DATASET}.project_document_candidates` c
    JOIN `{PROJECT_ID}.{DATASET}.documents` d
      ON c.document_id = d.document_id
    WHERE c.project_id = @project_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.document_id ORDER BY c.rank ASC) = 1
    ORDER BY c.rank ASC
    LIMIT @limit
    """
    return run_query(
        sql,
        [
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def get_project_ids_with_mapped_documents(limit: int = 20) -> list:
    sql = f"""
    SELECT project_id, COUNT(*) AS mapped_document_count
    FROM `{PROJECT_ID}.{DATASET}.project_document_map`
    GROUP BY project_id
    HAVING mapped_document_count > 0
    ORDER BY mapped_document_count DESC
    LIMIT @limit
    """
    return run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])


def get_project(project_id: str):
    rows = run_query(
        f"SELECT * FROM `{PROJECT_ID}.{DATASET}.projects` WHERE project_id = @pid LIMIT 1",
        [bigquery.ScalarQueryParameter("pid", "STRING", project_id)],
    )
    return rows[0] if rows else None


# Firestore limits each document (and each string field) to ~1 MiB.
# Large wikis are split across wiki/{project_id}/markdown_chunks/{index}.
WIKI_CHUNK_BYTES = int(os.getenv("WIKI_CHUNK_BYTES", "900000"))


def _split_utf8_chunks(text: str, max_bytes: int) -> list[str]:
    """Split text into UTF-8-safe chunks that each fit within max_bytes."""
    if not text:
        return [""]
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return [text]

    chunks: list[str] = []
    start = 0
    while start < len(encoded):
        end = min(start + max_bytes, len(encoded))
        # Do not split in the middle of a multi-byte UTF-8 character.
        while end > start and end < len(encoded) and (encoded[end] & 0xC0) == 0x80:
            end -= 1
        if end == start:
            end = min(start + max_bytes, len(encoded))
        chunks.append(encoded[start:end].decode("utf-8", errors="ignore"))
        start = end
    return chunks


def _delete_wiki_chunks(project_id: str):
    """Remove stale markdown_chunks subcollection documents."""
    chunk_col = db.collection("wiki").document(project_id).collection("markdown_chunks")
    batch = db.batch()
    count = 0
    for doc in chunk_col.stream():
        batch.delete(doc.reference)
        count += 1
        if count >= 400:
            batch.commit()
            batch = db.batch()
            count = 0
    if count:
        batch.commit()


def _store_wiki_markdown(project_id: str, base_doc: dict, markdown: str):
    """
    Persist wiki markdown. Small pages fit in the main `markdown` field.
    Larger pages are split into wiki/{project_id}/markdown_chunks/{index}.
    The main doc always keeps chunk 0 in `markdown` for backward compatibility.
    """
    chunks = _split_utf8_chunks(markdown, WIKI_CHUNK_BYTES)
    chunked = len(chunks) > 1

    base_doc["markdown"] = chunks[0]
    base_doc["markdown_chunked"] = chunked
    base_doc["markdown_chunk_count"] = len(chunks)
    base_doc["markdown_total_bytes"] = len(markdown.encode("utf-8"))

    db.collection("wiki").document(project_id).set(base_doc)

    if chunked:
        chunk_col = db.collection("wiki").document(project_id).collection("markdown_chunks")
        existing = {doc.id for doc in chunk_col.stream()}
        batch = db.batch()
        for i, chunk in enumerate(chunks):
            batch.set(
                chunk_col.document(str(i)),
                {"index": i, "content": chunk, "total_chunks": len(chunks)},
            )
        for stale_id in existing - {str(i) for i in range(len(chunks))}:
            batch.delete(chunk_col.document(stale_id))
        batch.commit()
    else:
        _delete_wiki_chunks(project_id)


def get_project_documents(project_id: str, limit: int = 30) -> list:
    sql = f"""
    SELECT d.*
    FROM `{PROJECT_ID}.{DATASET}.project_document_map` m
    JOIN `{PROJECT_ID}.{DATASET}.documents` d ON m.document_id = d.document_id
    WHERE m.project_id = @project_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY d.document_id ORDER BY m.confidence_score DESC
    ) = 1
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
            "Find it in Scoro â†’ Settings â†’ Site settings â†’ General."
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


# Scoro project status values (from Scoro API v2 docs).
SCORO_ACTIVE_STATUS = "inprogress"
SCORO_ALL_STATUSES = (
    "pending", "inprogress", "cancelled", "completed", "future",
    "additional1", "additional2", "additional3", "additional4",
)


def _scoro_project_mode() -> str:
    return os.getenv("SCORO_PROJECT_MODE", "active").lower()


def _scoro_statuses_to_fetch() -> list[str]:
    """
    Scoro's list endpoint filters by one status at a time. 'all' mode queries
    every known status and deduplicates â€” omitting the filter still returns only
    inprogress on many accounts.
    """
    mode = _scoro_project_mode()
    if mode == "active":
        return [SCORO_ACTIVE_STATUS]

    custom = os.getenv("SCORO_STATUSES", "").strip()
    if custom:
        return [s.strip() for s in custom.split(",") if s.strip()]
    return list(SCORO_ALL_STATUSES)


def fetch_scoro_projects_page(
    page: int = 1,
    per_page: int = 100,
    status: str | None = None,
) -> list[dict]:
    """Fetch one page of projects, optionally filtered by status."""
    body: dict = {"page": page, "per_page": per_page}
    if status:
        body["filter"] = {"status": status}
    data = _scoro_post("projects/list", body)
    return data.get("data", [])


def fetch_all_scoro_projects() -> list[dict]:
    """Paginate through every configured status and deduplicate by project_id."""
    per_page = int(os.getenv("SCORO_PER_PAGE", "100"))
    statuses = _scoro_statuses_to_fetch()
    seen_ids: set = set()
    all_raw: list[dict] = []

    for status in statuses:
        page, status_count = 1, 0
        while True:
            batch = fetch_scoro_projects_page(page=page, per_page=per_page, status=status)
            if not batch:
                break
            for project in batch:
                pid = project.get("project_id") or project.get("id")
                if pid is None or pid in seen_ids:
                    continue
                seen_ids.add(pid)
                all_raw.append(project)
                status_count += 1
            if len(batch) < per_page:
                break
            page += 1
        logger.info("scoro-sync: status=%s added %d projects (%d unique total)",
                    status, status_count, len(all_raw))

    return all_raw


def _safe_date_str(val) -> str | None:
    """Return an ISO date string or None; BQ SAFE.PARSE_DATE handles the rest."""
    if not val:
        return None
    return str(val)[:10]  # keep only YYYY-MM-DD


# â”€â”€ Scoro lookup caches (users, companies) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Built once per sync run so manager_id / members[] / c_clientcompany resolve
# to human-readable names instead of opaque numeric IDs.

_scoro_user_cache: dict[str, str] | None = None
_scoro_company_cache: dict[str, str] | None = None


def _fetch_scoro_list(endpoint: str, body_extra: dict | None = None) -> list[dict]:
    """Paginate through a Scoro list endpoint and return every row."""
    per_page = int(os.getenv("SCORO_PER_PAGE", "100"))
    rows: list[dict] = []
    page = 1
    while True:
        body = {"page": page, "per_page": per_page}
        if body_extra:
            body.update(body_extra)
        data = _scoro_post(endpoint, body)
        batch = data.get("data", []) or []
        rows.extend(batch)
        if len(batch) < per_page:
            break
        page += 1
    return rows


def get_scoro_user_cache() -> dict[str, str]:
    """user_id â†’ 'Firstname Lastname' for every Scoro user."""
    global _scoro_user_cache
    if _scoro_user_cache is not None:
        return _scoro_user_cache
    cache: dict[str, str] = {}
    try:
        for u in _fetch_scoro_list("users/list"):
            uid = str(u.get("id") or u.get("user_id") or "")
            name = " ".join(
                p for p in [str(u.get("firstname") or "").strip(),
                            str(u.get("lastname") or "").strip()]
                if p
            ) or str(u.get("full_name") or u.get("email") or "")
            if uid and name:
                cache[uid] = name
    except Exception as e:
        logger.warning("scoro-sync: could not build user cache: %s", e)
    _scoro_user_cache = cache
    logger.info("scoro-sync: user cache built with %d users", len(cache))
    return cache


def get_scoro_company_cache() -> dict[str, str]:
    """company/contact_id â†’ company name for Scoro company contacts."""
    global _scoro_company_cache
    if _scoro_company_cache is not None:
        return _scoro_company_cache
    cache: dict[str, str] = {}
    rows: list[dict] = []
    try:
        rows = _fetch_scoro_list("companies/list")
    except Exception as e:
        logger.warning("scoro-sync: companies/list failed (%s); trying contacts/list", e)
        try:
            rows = _fetch_scoro_list("contacts/list", {"filter": {"contact_type": "company"}})
        except Exception as e2:
            logger.warning("scoro-sync: could not build company cache: %s", e2)
    for c in rows:
        cid = str(c.get("company_id") or c.get("contact_id") or c.get("id") or "")
        name = str(c.get("name") or c.get("company_name") or "").strip()
        if cid and name:
            cache[cid] = name
    _scoro_company_cache = cache
    logger.info("scoro-sync: company cache built with %d companies", len(cache))
    return cache


def _resolve_members(raw: dict, user_cache: dict[str, str]) -> str:
    """Resolve the project's members[] ID list to comma-separated names."""
    members = raw.get("members") or raw.get("project_users") or []
    if not isinstance(members, (list, tuple)):
        return ""
    names = []
    for m in members:
        mid = str(m.get("user_id") or m.get("id") or m) if isinstance(m, dict) else str(m)
        name = user_cache.get(mid)
        if name and name not in names:
            names.append(name)
    return ", ".join(names)


def normalize_scoro_project(
    raw: dict,
    user_cache: dict[str, str] | None = None,
    company_cache: dict[str, str] | None = None,
) -> dict:
    """Map a Scoro API v2 project object to the BigQuery projects table schema."""
    user_cache = user_cache or {}
    company_cache = company_cache or {}

    scoro_id = str(raw.get("project_id") or raw.get("id") or "")

    # Scoro API v2 nests every custom field under customFields.{key}.
    # Reading them as top-level keys always returned None (audit Â§3).
    cf = raw.get("customFields") or raw.get("custom_fields") or {}

    def custom(key: str) -> str:
        val = cf.get(key)
        if val is None:
            val = raw.get(key)  # fallback for accounts that flatten them
        if isinstance(val, (list, tuple)):
            return ", ".join(str(v) for v in val if v)
        return str(val or "")

    # Manager: managerId (v2 casing) â†’ resolve to name via user cache.
    manager_id = str(raw.get("managerId") or raw.get("manager_id") or "")
    manager_name = (
        user_cache.get(manager_id)
        or str(raw.get("manager_email") or "")
        or manager_id
    )

    # Client company: resolve numeric c_clientcompany / company_id to a name.
    company_id = str(raw.get("company_id") or custom("c_clientcompany") or "")
    client_company = (
        str(raw.get("company_name") or "")
        or company_cache.get(company_id)
        or ""
    )

    tags = raw.get("tags") or []
    if isinstance(tags, (list, tuple)):
        tags = ", ".join(str(t) for t in tags if t)
    else:
        tags = str(tags or "")

    return {
        "project_id": f"scoro_{scoro_id}",
        "scoro_id": scoro_id,
        "project_no": str(raw.get("no", "") or ""),
        "project_name": str(raw.get("project_name") or raw.get("name") or ""),
        # Keep the internal status code for filtering AND the human label.
        "status": str(raw.get("status") or ""),
        "status_name": str(raw.get("statusName") or raw.get("status_name") or ""),
        "project_manager": manager_name,
        "project_manager_name": manager_name,
        "project_members": _resolve_members(raw, user_cache),
        "start_date": _safe_date_str(raw.get("date") or raw.get("start_date")),
        "due_date": _safe_date_str(raw.get("deadline") or raw.get("due_date")),
        "completed_date": str(raw.get("completed_date") or ""),
        "description": str(raw.get("description") or ""),
        "client_company": client_company,
        "project_type": custom("c_projecttype") or str(raw.get("project_type") or ""),
        "client_country": custom("c_clientcountry"),
        "business_area": custom("c_businessarea"),
        "business_line_division": custom("c_businesslinedivision"),
        "budget_type": custom("c_budgettype") or str(raw.get("budget_type") or ""),
        "po_number": custom("c_ponumber"),
        "open_po_number": custom("c_openponr"),
        "related_project": custom("c_relatedproject"),
        "google_drive_link": custom("c_gdrivelink") or custom("c_drivelink"),
        "project_priority": custom("c_project_priority"),
        "tags": tags,
    }


def upsert_projects(rows: list[dict]):
    """MERGE Scoro projects into BigQuery.projects (insert new, update existing)."""
    if not rows:
        return

    # Explicit all-STRING schema so the staging table never mis-infers types
    # (e.g. numeric-looking scoro_id as INT64). Dates are cast in the MERGE.
    string_fields = [
        "project_id", "scoro_id", "project_no", "project_name", "status",
        "status_name", "project_manager", "project_manager_name",
        "project_members", "start_date", "due_date",
        "completed_date", "description", "client_company", "project_type",
        "client_country", "business_area", "business_line_division",
        "budget_type", "po_number", "open_po_number", "related_project",
        "google_drive_link", "project_priority", "tags",
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
            status_name          = S.status_name,
            project_manager      = S.project_manager,
            project_manager_name = S.project_manager_name,
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
            tags                 = S.tags,
            imported_at          = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            project_id, scoro_id, project_no, project_name, status, status_name,
            project_manager, project_manager_name, project_members,
            start_date, due_date, completed_date, description,
            client_company, project_type, client_country,
            business_area, business_line_division, budget_type,
            po_number, open_po_number, related_project,
            google_drive_link, project_priority, tags, imported_at
        ) VALUES (
            S.project_id, S.scoro_id, S.project_no, S.project_name, S.status, S.status_name,
            S.project_manager, S.project_manager_name, S.project_members,
            SAFE.PARSE_DATE('%Y-%m-%d', S.start_date),
            SAFE.PARSE_DATE('%Y-%m-%d', S.due_date),
            S.completed_date, S.description,
            S.client_company, S.project_type, S.client_country,
            S.business_area, S.business_line_division, S.budget_type,
            S.po_number, S.open_po_number, S.related_project,
            S.google_drive_link, S.project_priority, S.tags, CURRENT_TIMESTAMP()
        )
        """).result()
    finally:
        bq.delete_table(tmp, not_found_ok=True)


def scoro_sync():
    run_id = str(uuid.uuid4())
    started_at = now_iso()

    try:
        ensure_schema()
        user_cache = get_scoro_user_cache()
        company_cache = get_scoro_company_cache()
        all_raw = fetch_all_scoro_projects()
        normalized = [normalize_scoro_project(p, user_cache, company_cache) for p in all_raw]
        upsert_projects(normalized)

        status_counts: dict[str, int] = {}
        for p in normalized:
            st = p.get("status") or "unknown"
            status_counts[st] = status_counts.get(st, 0) + 1

        logger.info(json.dumps({
            "status": "ok", "job": "scoro-sync",
            "projects_upserted": len(normalized),
            "scoro_project_mode": _scoro_project_mode(),
            "statuses_queried": _scoro_statuses_to_fetch(),
            "status_breakdown": status_counts,
        }))
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
         cloud-platform scope under Admin Console â†’ Security â†’ API Controls â†’
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
        "contentSearchSpec": {
            "snippetSpec": {"returnSnippet": True},
            # Extractive content gives far richer text than a single snippet,
            # which is what made the generated wikis shallow.
            "extractiveContentSpec": {
                "maxExtractiveAnswerCount": 3,
                "maxExtractiveSegmentCount": 3,
                "returnExtractiveSegmentScore": True,
            },
        },
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
            f"Discovery Engine search failed: HTTP {resp.status_code} â€” {resp.text[:800]} "
            f"(query={query!r})"
        )

    return resp.json().get("results", [])


def _strip_name_suffixes(project_name: str) -> str:
    """
    Scoro project names follow {Deliverable}_{ClientContact}_{AdmindEmployee},
    e.g. "ABB IR 26 Website_Diana Silander_Flisikowska A". Sending the person
    names to Discovery Engine routes queries to org charts and HR documents
    (audit Â§4.1). Keep the deliverable part, drop trailing person-name segments.
    """
    if not project_name:
        return project_name
    parts = [p.strip() for p in project_name.split("_") if p.strip()]
    if len(parts) <= 1:
        return project_name

    # A segment "looks like a person" when it is 1â€“3 capitalised words with no
    # digits (e.g. "Diana Silander", "Flisikowska A", "Czerw D").
    person_re = re.compile(r"^(?:[A-ZĹĹ»ĹšÄ†][\w'â€™.-]*)(?:\s+[A-ZĹĹ»ĹšÄ†][\w'â€™.-]*){0,2}$")

    kept = [parts[0]]
    for seg in parts[1:]:
        words = seg.split()
        looks_like_person = (
            1 <= len(words) <= 3
            and not any(ch.isdigit() for ch in seg)
            and person_re.match(seg) is not None
        )
        if not looks_like_person:
            kept.append(seg)
    return " ".join(kept)


def build_project_queries(project) -> list[str]:
    """
    Build several complementary search queries per project. Discovery Engine is
    search-based, so multiple angles (number, cleaned name, client+type+tags)
    retrieve far more relevant candidates than one combined string.
    """
    project_name = _row(project, "project_name")
    project_no = _row(project, "project_no")
    client = _row(project, "client_company")
    project_type = _row(project, "project_type")
    business_area = _row(project, "business_area")
    tags = _row(project, "tags")
    description = _row(project, "description")

    clean_name = _strip_name_suffixes(project_name)

    queries: list[str] = []

    # Project number is the single most precise signal Admind uses in filenames.
    if project_no:
        queries.append(str(project_no))
    if clean_name and project_no:
        queries.append(f'"{clean_name}" {project_no}')
    if clean_name:
        queries.append(clean_name)
    if client and clean_name:
        queries.append(f"{client} {clean_name}")
    if client and (project_type or tags):
        queries.append(" ".join(p for p in [client, project_type, tags, business_area] if p))
    if client and description:
        queries.append(f"{client} {description[:200]}")

    # Deduplicate while preserving order.
    seen, unique = set(), []
    for q in queries:
        q = q.strip()
        if q and q.lower() not in seen:
            seen.add(q.lower())
            unique.append(q)
    return unique


def _extract_document(item: dict) -> dict | None:
    """Parse one Discovery Engine search result into a documents row."""
    doc = item.get("document", {})
    derived = doc.get("derivedStructData", {})
    struct = doc.get("structData", {})
    document_id = doc.get("id") or doc.get("name", "")
    if not document_id:
        return None

    parts: list[str] = []

    snippets = derived.get("snippets", []) or []
    snippet = snippets[0].get("snippet", "") if snippets else ""
    if snippet:
        parts.append(snippet)

    for ans in derived.get("extractive_answers", []) or derived.get("extractiveAnswers", []) or []:
        content = ans.get("content") or ans.get("pageContent")
        if content:
            parts.append(content)

    for seg in derived.get("extractive_segments", []) or derived.get("extractiveSegments", []) or []:
        content = seg.get("content") or seg.get("pageContent")
        if content:
            parts.append(content)

    full_text = "\n\n".join(parts)

    # url may be an internal gs:// connector URI; source_url is the user-facing
    # link when Discovery Engine provides one.
    source_url = (
        derived.get("link")
        or struct.get("link")
        or struct.get("url")
        or struct.get("source_url")
        or ""
    )
    url = source_url or struct.get("uri") or doc.get("name", "") or ""

    return {
        "document_id": document_id,
        "source_system": "discovery_engine",
        "source_type": "search_result",
        "title": derived.get("title") or struct.get("title") or document_id,
        "folder_path": "",
        "url": url,
        "source_url": source_url,
        "author": "",
        "text_preview": (snippet or full_text)[:1000],
        "full_text": full_text[:12000],
    }


def discover_documents_for_project(project) -> tuple[list[dict], list[dict]]:
    """
    Returns (documents, candidates).
    documents  â†’ rows for the global documents table (deduped by document_id).
    candidates â†’ per-project link rows (project_id, document_id, rank, queryâ€¦).
    """
    queries = build_project_queries(project)
    if not queries:
        return [], []

    project_id = _row(project, "project_id")
    page_size = int(os.getenv("DOCUMENT_LIMIT", "25"))

    docs_by_id: dict[str, dict] = {}
    # One row per document_id — MERGE key is (project_id, document_id).
    # The same doc can surface from multiple queries; keep the best (lowest) rank.
    candidate_by_doc: dict[str, dict] = {}
    rank = 0

    for query in queries:
        try:
            results = search_discovery_engine(query, page_size=page_size)
        except Exception as e:
            logger.warning("document-discovery: query failed (%s): %s", query, e)
            continue

        for item in results:
            parsed = _extract_document(item)
            if not parsed:
                continue
            doc_id = parsed["document_id"]
            docs_by_id.setdefault(doc_id, parsed)
            rank += 1
            cand = {
                "project_id": project_id,
                "document_id": doc_id,
                "discovery_query": query,
                "rank": rank,
                "title": parsed["title"],
                "url": parsed["url"],
                "source_url": parsed["source_url"],
                "text_preview": parsed["text_preview"],
                "discovered_at": now_iso(),
            }
            existing = candidate_by_doc.get(doc_id)
            if existing is None or rank < existing["rank"]:
                candidate_by_doc[doc_id] = cand

    return list(docs_by_id.values()), list(candidate_by_doc.values())


def upsert_documents(rows: list[dict]):
    """MERGE discovered documents into BigQuery.documents."""
    if not rows:
        return

    string_fields = [
        "document_id", "source_system", "source_type", "title", "folder_path",
        "url", "source_url", "author", "text_preview", "full_text",
    ]
    schema = [bigquery.SchemaField(f, "STRING") for f in string_fields]
    rows = [{f: r.get(f, "") for f in string_fields} for r in rows]

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
            source_url    = S.source_url,
            text_preview  = S.text_preview,
            full_text     = S.full_text,
            imported_at   = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            document_id, source_system, source_type, title, folder_path,
            url, source_url, author, text_preview, full_text, imported_at
        ) VALUES (
            S.document_id, S.source_system, S.source_type, S.title, S.folder_path,
            S.url, S.source_url, S.author, S.text_preview, S.full_text, CURRENT_TIMESTAMP()
        )
        """).result()
    finally:
        bq.delete_table(tmp, not_found_ok=True)


def upsert_document_candidates(rows: list[dict], run_id: str):
    """MERGE per-project discovery candidates (idempotent on project+document)."""
    if not rows:
        return

    # BigQuery MERGE requires at most one source row per target key.
    deduped: dict[tuple[str, str], dict] = {}
    for r in rows:
        key = (str(r.get("project_id", "")), str(r.get("document_id", "")))
        existing = deduped.get(key)
        if existing is None or int(r.get("rank", 999999)) < int(existing.get("rank", 999999)):
            deduped[key] = r
    rows = list(deduped.values())

    string_fields = ["project_id", "document_id", "discovery_query", "title",
                     "url", "source_url", "text_preview"]
    schema = [bigquery.SchemaField(f, "STRING") for f in string_fields]
    schema.append(bigquery.SchemaField("rank", "INT64"))
    schema.append(bigquery.SchemaField("discovered_at", "TIMESTAMP"))
    schema.append(bigquery.SchemaField("run_id", "STRING"))

    payload = []
    for r in rows:
        row = {f: r.get(f, "") for f in string_fields}
        row["rank"] = int(r.get("rank", 0))
        row["discovered_at"] = r.get("discovered_at") or now_iso()
        row["run_id"] = run_id
        payload.append(row)

    tmp = f"{PROJECT_ID}.{DATASET}._tmp_cand_{uuid.uuid4().hex[:8]}"
    bq.load_table_from_json(
        payload, tmp,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema),
    ).result()

    try:
        bq.query(f"""
        MERGE `{PROJECT_ID}.{DATASET}.project_document_candidates` T
        USING `{tmp}` S
        ON T.project_id = S.project_id AND T.document_id = S.document_id
        WHEN MATCHED THEN UPDATE SET
            discovery_query = S.discovery_query,
            rank            = S.rank,
            title           = S.title,
            url             = S.url,
            source_url      = S.source_url,
            text_preview    = S.text_preview,
            discovered_at   = S.discovered_at,
            run_id          = S.run_id
        WHEN NOT MATCHED THEN INSERT (
            project_id, document_id, discovery_query, rank, title, url,
            source_url, text_preview, discovered_at, run_id
        ) VALUES (
            S.project_id, S.document_id, S.discovery_query, S.rank, S.title, S.url,
            S.source_url, S.text_preview, S.discovered_at, S.run_id
        )
        """).result()
    finally:
        bq.delete_table(tmp, not_found_ok=True)


def document_discovery():
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    total_discovered = 0

    try:
        ensure_schema()
        projects = get_projects_for_processing(limit=int(os.getenv("PROJECT_LIMIT", "100")))
        failures = 0
        for project in projects:
            name = _row(project, "project_name") or _row(project, "project_id")
            try:
                docs, candidates = discover_documents_for_project(project)
                upsert_documents(docs)
                upsert_document_candidates(candidates, run_id)
                total_discovered += len(candidates)
                logger.info("document-discovery: %s â†’ %d docs / %d candidates",
                            name, len(docs), len(candidates))
            except Exception as e:
                failures += 1
                logger.error("document-discovery: project %s failed: %s", name, e)

        if total_discovered == 0 and failures > 0:
            # Every project failed â€” surface the problem instead of reporting success
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

    project_type = _row(project, "project_type")
    business_area = _row(project, "business_area")
    tags = _row(project, "tags")
    clean_name = _strip_name_suffixes(project_name)

    prompt = f"""
You are a project-document taxonomy classifier for Admind Agency, a creative branding studio.
Decide whether each candidate document belongs to the project below.

Project metadata:
- Project number: {project_no}
- Project name: {project_name}
- Deliverable (project name without person-name suffixes): {clean_name}
- Client/company: {client_company}
- Project type: {project_type}
- Business area: {business_area}
- Tags: {tags}
- Team: {project_members}
- Period: {start_date} to {due_date}
- Description: {description}

Naming convention: Admind project names follow
{{Deliverable}}_{{ClientContact}}_{{AdmindEmployee}} and filenames usually start
with the project number (e.g. "566508_..."). The project NUMBER and the
DELIVERABLE are the strongest evidence.

Classification rules:
- strong_match: title/path/content clearly references this project, its number,
  client, campaign, or a specific deliverable/workstream.
- possible_match: appears related to the same client or workstream but evidence
  is incomplete.
- reject: generic, unrelated, or only weakly matching.

Guidance:
- Prefer project number, exact deliverable name, and client name + deliverable as evidence.
- REJECT org charts, HR documents, employee/holiday lists, timesheets, CVs and
  other internal-operations documents. A match on an employee's name alone is
  NOT evidence â€” every Admind employee appears in hundreds of unrelated files.
- REJECT documents that clearly belong to a DIFFERENT project number, even for
  the same client (e.g. a file titled "566123_..." is not evidence for 566508).
- REJECT generic client material (brand guidelines, old case studies, other
  campaigns) unless this project is specifically about that material.
- Personal handover files (e.g. "Handover_<employee>.xlsx") belong to a project
  ONLY if the handover content is about this specific project's deliverable.
- If evidence is weak, use possible_match, not strong_match.

Return ONLY valid JSON with this exact shape:
{{
  "matches": [
    {{
      "document_id": "string",
      "decision": "strong_match | possible_match | reject",
      "confidence_score": 0.0,
      "evidence": "specific clue from the title/path/content",
      "reason": "short explanation"
    }}
  ]
}}

Documents:
{json.dumps(document_chunks, ensure_ascii=False)}
"""

    raw = generate_text(prompt, temperature=0, json_mode=True)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        raise RuntimeError(f"LLM returned invalid JSON: {raw}")

    return parsed.get("matches", [])


def _match_passes(m: dict) -> bool:
    """Keep strong and possible matches at â‰Ą0.75 (rejects dropped)."""
    decision = (m.get("decision") or "").lower()
    score = float(m.get("confidence_score", 0) or 0)
    if decision == "reject":
        return False
    # Uniform 0.75 floor â€” the old 0.55 floor for possible_match let unrelated
    # client documents pollute the wikis (audit Â§4.2).
    return score >= 0.75


def taxonomy_sync():
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    projects_processed = 0
    mappings_created = 0
    documents_seen = 0
    candidate_limit = int(os.getenv("CANDIDATE_LIMIT", os.getenv("DOCUMENT_LIMIT", "50")))

    try:
        ensure_schema()
        projects = get_projects_for_processing(limit=int(os.getenv("PROJECT_LIMIT", "100")))

        for project in projects:
            project_id = _row(project, "project_id")
            documents = get_candidate_documents_for_project(project_id, limit=candidate_limit)
            documents_seen += len(documents)

            if not documents:
                logger.info("taxonomy-sync: %s has no candidate documents", project_id)
                projects_processed += 1
                continue

            matches = classify_documents_for_project(project, documents)
            rows = [
                {
                    "project_id": project_id,
                    "document_id": m["document_id"],
                    "confidence_score": float(m.get("confidence_score", 0) or 0),
                    "matching_method": classifier_matching_method(),
                    "classifier_reason": (m.get("reason") or m.get("evidence")),
                    "classified_at": now_iso(),
                    "run_id": run_id,
                }
                for m in matches
                if m.get("document_id") and _match_passes(m)
            ]
            insert_rows("project_document_map", rows)
            projects_processed += 1
            mappings_created += len(rows)

        logger.info(json.dumps({
            "status": "ok", "job": "taxonomy-sync", "run_id": run_id,
            "projects_processed": projects_processed,
            "documents_processed": documents_seen,
            "mappings_created": mappings_created,
        }))
        log_pipeline_run("taxonomy-sync", run_id, started_at, "success",
                         records_read=documents_seen, records_written=mappings_created)

    except Exception as e:
        log_pipeline_run("taxonomy-sync", run_id, started_at, "error",
                         records_read=documents_seen, records_written=mappings_created,
                         error_message=str(e))
        raise

# ---------------------------------------------------------------------------
# WIKI GENERATION
# Reads matched documents and writes Markdown wiki pages to Firestore
# ---------------------------------------------------------------------------

# Internal Admind document patterns that are never about a single project
_NOISE_PATTERNS = re.compile(
    r"Utilization_Report|PLANNING\s|Admind_Purchased|project.categories|"
    r"\bJira\b|^TV\s*-\s*\d|_OP\s+Summary_|Handover_",
    re.IGNORECASE,
)

# Filename version suffix, e.g. _V3, _v02, _V1_final
_VERSION_RE = re.compile(r"[_\s-]v(\d+)([_.\s]|$)", re.IGNORECASE)


def _doc_version(title: str) -> int:
    """Return the numeric version suffix of a filename, or 0 if absent."""
    m = _VERSION_RE.search(title)
    return int(m.group(1)) if m else 0


def _base_name(title: str) -> str:
    """Strip version suffix so files that share a base name can be compared."""
    return _VERSION_RE.sub("", title).rstrip("._- ").lower()


def _prefilter_docs(docs: list, project_no: str) -> list:
    """
    Reduce the raw mapped-document list to the most relevant, clean set:

    Keep rules (any one is enough):
      - Title contains the project number string (strongest signal)
      - Title starts with CE_ (quotes / cost estimates for this project)

    Reject rules (always drop if matched):
      - Matches _NOISE_PATTERNS (internal ops docs with no project content)
      - Title contains '_OP Summary_' and does NOT contain the project number
        (PO summary for a different project / person — a common false positive)

    De-duplicate by base name, keeping the highest version.
    Cap the result at 20 documents.
    """
    kept: list[dict] = []
    for doc in docs:
        title = str(doc.get("title") or "")

        # Always drop noise
        if _NOISE_PATTERNS.search(title):
            continue

        has_project_no = project_no and project_no in title
        is_quote = title.upper().startswith("CE_")

        # Drop PO/OP summaries that belong to a different project
        if "_OP Summary_" in title or "_OP_Summary_" in title:
            if not has_project_no:
                continue

        # Keep if strong signal; otherwise skip
        if not (has_project_no or is_quote):
            continue

        kept.append(doc)

    # De-duplicate: keep highest version per base name
    best: dict[str, dict] = {}
    for doc in kept:
        base = _base_name(str(doc.get("title") or ""))
        existing = best.get(base)
        if existing is None or _doc_version(str(doc.get("title") or "")) > _doc_version(str(existing.get("title") or "")):
            best[base] = doc

    # Sort by descending version so final deliverables appear first
    result = sorted(best.values(),
                    key=lambda d: _doc_version(str(d.get("title") or "")),
                    reverse=True)
    return result[:20]


def generate_wiki_for_project(project_id: str):
    run_id = str(uuid.uuid4())
    started_at = now_iso()

    project = get_project(project_id)
    if not project:
        raise RuntimeError(f"Project not found in BigQuery: {project_id}")

    wiki_doc_limit = int(os.getenv("WIKI_MAX_SOURCE_DOCS", "30"))
    wiki_char_limit = int(os.getenv("WIKI_SOURCE_CHARS", "12000"))
    docs = get_project_documents(project_id, limit=wiki_doc_limit)

    # Use actual projects schema field names
    project_name = _row(project, "project_name")
    project_no = _row(project, "project_no") or project_id
    client_company = _row(project, "client_company")
    project_manager = _row(project, "project_manager_name") or _row(project, "project_manager")
    project_members = _row(project, "project_members")
    project_type = _row(project, "project_type")
    business_area = _row(project, "business_area")
    business_division = _row(project, "business_line_division")
    client_country = _row(project, "client_country")
    tags = _row(project, "tags")
    status_name = _row(project, "status_name") or _row(project, "status")
    budget_type = _row(project, "budget_type")
    po_number = _row(project, "po_number")
    drive_link = _row(project, "google_drive_link")
    priority = _row(project, "project_priority")
    start_date = _row(project, "start_date")
    due_date = _row(project, "due_date")
    description = _row(project, "description")

    base_doc = {
        "project_id": project_id,
        "project_name": project_name,
        "project_no": project_no,
        "client_company": client_company,
        "project_manager": str(project_manager or ""),
        "project_members": project_members,
        "status_name": str(status_name or ""),
        "tags": str(tags or ""),
        "business_area": str(business_area or ""),
        "client_country": str(client_country or ""),
        "google_drive_link": str(drive_link or ""),
        "generated_at": firestore.SERVER_TIMESTAMP,
        "source_document_ids": [_row(doc, "document_id") for doc in docs],
    }

    # No mapped sources â†’ build a factual page from the Scoro record alone
    # (no LLM call), so the page is still useful instead of an empty shell.
    if not docs:
        lines = [f"# {project_name}", ""]
        intro_bits = []
        if client_company:
            intro_bits.append(f"for **{client_company}**")
        if project_type:
            intro_bits.append(f"({project_type})")
        lines.append(
            f"Admind runs project **{project_no}** {' '.join(intro_bits)}".strip() + "."
        )
        if description:
            lines += ["", description]
        facts = []
        if status_name:
            facts.append(f"- **Status:** {status_name}")
        if start_date or due_date:
            facts.append(f"- **Period:** {start_date or 'â€”'} â†’ {due_date or 'ongoing'}")
        if project_manager:
            facts.append(f"- **Project manager:** {project_manager}")
        if project_members:
            facts.append(f"- **Team:** {project_members}")
        if business_area:
            facts.append(f"- **Business area:** {business_area}")
        if client_country:
            facts.append(f"- **Client country:** {client_country}")
        if tags:
            facts.append(f"- **Tags:** {tags}")
        if drive_link and str(drive_link).lower().startswith("http"):
            facts.append(f"- **Project folder:** [Google Drive]({drive_link})")
        if facts:
            lines += ["", "## Overview", ""] + facts
        lines += ["", "_Document intelligence for this project is still being indexed._"]
        placeholder = "\n".join(lines)
        _store_wiki_markdown(project_id, {
            **base_doc,
            "wiki_status": "no_sources",
            "generated_by_model": "none",
        }, placeholder)
        log_pipeline_run("wiki-generate", run_id, started_at, "success",
                         records_read=0, records_written=1)
        logger.info(json.dumps({
            "status": "ok", "job": "wiki-generate",
            "project_id": project_id, "source_document_count": 0,
            "wiki_status": "no_sources",
        }))
        return

    def _clickable(url: str) -> str:
        u = str(url or "").strip()
        return u if u.lower().startswith(("http://", "https://")) else ""

    # Pre-filter: remove noise, deduplicate versions, cap at 20
    filtered_docs = _prefilter_docs(
        [{"title": _row(d, "title"),
          "document_id": _row(d, "document_id"),
          "source_url": _row(d, "source_url"),
          "url": _row(d, "url"),
          "full_text": _row(d, "full_text"),
          "text_preview": _row(d, "text_preview")}
         for d in docs],
        project_no,
    )

    # Fallback: if filtering stripped everything, use top-5 by classifier confidence
    if not filtered_docs:
        filtered_docs = [
            {"title": _row(d, "title"),
             "document_id": _row(d, "document_id"),
             "source_url": _row(d, "source_url"),
             "url": _row(d, "url"),
             "full_text": _row(d, "full_text"),
             "text_preview": _row(d, "text_preview")}
            for d in docs[:5]
        ]

    source_docs = [
        {
            "index": i + 1,
            "title": d["title"],
            "url": _clickable(d.get("source_url") or "") or _clickable(d.get("url") or ""),
            "content": (d.get("full_text") or d.get("text_preview") or "")[:wiki_char_limit],
        }
        for i, d in enumerate(filtered_docs)
    ]

    # Build one prose background paragraph — field names never appear in the prompt
    bg = []
    if project_no and project_type and client_company:
        line = f"{project_no} is a {project_type} project for {client_company}"
        if business_area:
            line += f" in the {business_area} practice area"
        if start_date or due_date:
            line += f", running {start_date or '?'} to {due_date or 'ongoing'}"
        line += "."
        bg.append(line)
    if description:
        bg.append(description.strip().rstrip(".") + ".")
    team_bits = ", ".join(p for p in [project_manager, project_members] if p)
    if team_bits:
        bg.append(f"The Admind team includes: {team_bits}.")
    background_para = " ".join(bg) or f"{project_no} — {project_name}."

    prompt = f"""You are a senior account manager at Admind Agency writing an internal project wiki.
Your reader needs to take over or support this project tomorrow.

Background context — synthesise into your narrative, do NOT cite these facts by
field name or label:
{background_para}

SOURCE DOCUMENTS (numbered — cite by footnote number [1][2] only):
{json.dumps(source_docs, ensure_ascii=False)}

Write a professional internal project wiki in Markdown. Rules:
- NEVER cite facts from the background context block by field name. Do not write
  "Project description", "Project record", "Period:", "Status:", or any other
  field label from that block.
- Inline citations use footnote numbers [1][2] matching the source index above.
  Filenames and URLs belong only in the Source Documents table at the bottom.
- Never start any sentence with "This project".
- Omit any section that has no substantive information. Never write
  "Not found in available sources", "N/A" or "Unknown".
- Do not invent facts. Everything must come from the background context or the
  numbered source documents.
- Write in direct present tense. Name the actual deliverable, client, and
  people — never vague openers like "The project involves…".

---

# {project_name}

## Overview
One paragraph, 4–6 sentences. What was produced, for whom, why it matters,
current status. Senior account-manager voice. No bullets. No inline citations.

## Deliverables
Group final deliverables by type (e.g. Print / Digital / Motion). For each,
one sentence describing what it is and its intended use, followed by a
[Open in Drive](url) link where a URL is available. Show only the highest
version of each file — omit intermediate versions (V0, V1, V2 when V3 exists).

## Team & Stakeholders
Two clean lists:
- **Admind Team** — names and roles only
- **Client Contacts** — names and roles only
No filename citations in this section.

## Timeline & Milestones
Only meaningful milestones: quote date, kick-off, first delivery, final
delivery, approval. Not every file version.

## Key Decisions & Brief
What the client asked for and any documented constraints or clarifications.
Prose, 3–5 sentences.

## Risks & Open Items
Only genuine gaps: missing brief, unresolved feedback, unclear scope.
Omit this section entirely if there are none.

## Source Documents
A Markdown table listing every source document used:
| # | Document | Type | Link |

This is the ONLY place where full filenames and URLs appear.

---

Return ONLY the Markdown page. No preamble, no wrapping code fences.
"""

    markdown = generate_text(prompt, temperature=0.2, json_mode=False,
                             model_name=WIKI_MODEL_NAME)
    model_used = active_llm_label()
    chunks = _split_utf8_chunks(markdown, WIKI_CHUNK_BYTES)

    _store_wiki_markdown(project_id, {
        **base_doc,
        "wiki_status": "generated_chunked" if len(chunks) > 1 else "generated",
        "generated_by_model": model_used,
    }, markdown)

    log_pipeline_run("wiki-generate", run_id, started_at, "success",
                     records_read=len(docs), records_written=1)

    logger.info(json.dumps({
        "status": "ok", "job": "wiki-generate",
        "project_id": project_id, "source_document_count": len(docs),
        "markdown_chunk_count": len(chunks),
    }))


def wiki_generate_all():
    """Generate wiki pages only for projects that actually have mapped documents."""
    rows = get_project_ids_with_mapped_documents(limit=int(os.getenv("PROJECT_LIMIT", "100")))
    if not rows:
        logger.info(json.dumps({
            "status": "ok", "job": "wiki-generate",
            "note": "no projects with mapped documents",
        }))
        return

    succeeded, failures = 0, []
    for row in rows:
        pid = _row(row, "project_id")
        if not pid:
            continue
        try:
            generate_wiki_for_project(pid)
            succeeded += 1
        except Exception as e:
            failures.append({"project_id": pid, "error": str(e)[:500]})
            logger.error("wiki-generate: project %s failed: %s", pid, e)

    logger.info(json.dumps({
        "status": "ok" if succeeded > 0 else "error",
        "job": "wiki-generate",
        "projects_succeeded": succeeded,
        "projects_failed": len(failures),
        "failures": failures,
    }))

    if succeeded == 0 and failures:
        raise RuntimeError(
            f"wiki-generate: all {len(failures)} projects failed; "
            f"first error: {failures[0]['error']}"
        )

# ---------------------------------------------------------------------------
# FULL SYNC â€” runs the complete pipeline in order
# ---------------------------------------------------------------------------


def _apply_full_sync_defaults():
    """
    Defaults for a one-click full pipeline run (Console UI â†’ Execute).

    - SCORO_PROJECT_MODE=all  â†’ import the full Scoro catalog (all statuses)
    - PROJECT_MODE=active     â†’ discovery / taxonomy / wiki on open projects only
    """
    defaults = {
        "SCORO_PROJECT_MODE": "all",
        "PROJECT_MODE": "active",
    }
    for key, value in defaults.items():
        if not os.getenv(key):
            os.environ[key] = value

    logger.info(json.dumps({
        "event": "full-sync-config",
        "scoro_project_mode": os.getenv("SCORO_PROJECT_MODE"),
        "project_mode": os.getenv("PROJECT_MODE"),
        "project_limit": os.getenv("PROJECT_LIMIT", "100"),
        "document_limit": os.getenv("DOCUMENT_LIMIT", "25"),
        "candidate_limit": os.getenv("CANDIDATE_LIMIT", "50"),
    }))


def full_sync():
    _apply_full_sync_defaults()
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


def historical_full_sync():
    """Full pipeline over ALL projects (incl. completed) â€” for backfill runs."""
    os.environ["SCORO_PROJECT_MODE"] = "all"
    os.environ["PROJECT_MODE"] = "all"
    logger.info("historical-full-sync: forcing SCORO_PROJECT_MODE=all, PROJECT_MODE=all")
    full_sync()


def run_job(job: str):
    logger.info(json.dumps({
        "event": "job_start",
        "job_type": job,
        "scoro_project_mode": os.getenv("SCORO_PROJECT_MODE", "active"),
        "project_mode": os.getenv("PROJECT_MODE", "active"),
        "project_limit": os.getenv("PROJECT_LIMIT", "100"),
    }))

    dispatch = {
        "scoro-sync": scoro_sync,
        "document-discovery": document_discovery,
        "taxonomy-sync": taxonomy_sync,
        "wiki-generate": wiki_generate_all,
        "full-sync": full_sync,
        "historical-full-sync": historical_full_sync,
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
    run_job(os.getenv("JOB_TYPE", "full-sync"))
