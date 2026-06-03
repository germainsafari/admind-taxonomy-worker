import json
import os
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud import firestore
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig


PROJECT_ID = os.getenv("PROJECT_ID", "admind-data-organisation")
DATASET = os.getenv("DATASET", "admind_data_organisation")
LOCATION = os.getenv("LOCATION", "europe-west4")
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.5-flash")

bq = bigquery.Client(project=PROJECT_ID)
db = firestore.Client(project=PROJECT_ID)

vertexai.init(project=PROJECT_ID, location=LOCATION)
model = GenerativeModel(MODEL_NAME)


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


def get_active_projects(limit=20):
    sql = f"""
    SELECT
      project_id,
      project_code,
      project_name,
      client_name,
      team_members,
      start_date,
      end_date,
      description
    FROM `{PROJECT_ID}.{DATASET}.projects`
    WHERE LOWER(status) NOT IN ('completed', 'closed', 'cancelled')
    LIMIT @limit
    """
    return run_query(
        sql,
        [
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def get_candidate_documents(limit=200):
    sql = f"""
    SELECT
      document_id,
      title,
      folder_path,
      author,
      created_at,
      modified_at,
      text_preview
    FROM `{PROJECT_ID}.{DATASET}.documents`
    LIMIT @limit
    """
    return run_query(
        sql,
        [
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )


def classify_documents_for_project(project, documents):
    document_chunks = []
    for doc in documents:
        document_chunks.append(
            {
                "document_id": doc.document_id,
                "filename": doc.title,
                "folder_path": doc.folder_path,
                "author": doc.author,
                "created_at": str(doc.created_at) if doc.created_at else None,
                "modified_at": str(doc.modified_at) if doc.modified_at else None,
                "preview": (doc.text_preview or "")[:500],
            }
        )

    prompt = f"""
You are a document classifier for Admind Agency, a creative branding studio.

Project: {project.project_name} | Code: {project.project_code} | Client: {project.client_name}
Team: {project.team_members} | Period: {project.start_date} to {project.end_date}
Description: {project.description}

Below are indexed documents. Return ONLY valid JSON.

Return a JSON object with this shape:
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
- Do not guess.
- Exclude documents with no clear match.
- Confidence must be between 0 and 1.
- Use only the project metadata and document metadata below.

Documents:
{json.dumps(document_chunks, ensure_ascii=False)}
"""

    response = model.generate_content(
        prompt,
        generation_config=GenerationConfig(
            temperature=0,
            response_mime_type="application/json",
        ),
    )

    try:
        parsed = json.loads(response.text)
    except json.JSONDecodeError:
        raise RuntimeError(f"Gemini returned invalid JSON: {response.text}")

    return parsed.get("matches", [])


def taxonomy_sync():
    run_id = str(uuid.uuid4())
    started_at = now_iso()

    insert_rows(
        "taxonomy_runs",
        [
            {
                "run_id": run_id,
                "job_name": "taxonomy-sync",
                "started_at": started_at,
                "finished_at": None,
                "status": "running",
                "projects_processed": 0,
                "documents_processed": 0,
                "mappings_created": 0,
                "error_message": None,
            }
        ],
    )

    projects_processed = 0
    mappings_created = 0

    try:
        projects = get_active_projects(limit=int(os.getenv("PROJECT_LIMIT", "20")))
        documents = get_candidate_documents(limit=int(os.getenv("DOCUMENT_LIMIT", "200")))

        for project in projects:
            matches = classify_documents_for_project(project, documents)

            rows = []
            for match in matches:
                if match.get("confidence_score", 0) < 0.75:
                    continue

                rows.append(
                    {
                        "project_id": project.project_id,
                        "document_id": match["document_id"],
                        "confidence_score": float(match.get("confidence_score", 0)),
                        "matching_method": "gemini_classification",
                        "classifier_reason": match.get("reason"),
                        "classified_at": now_iso(),
                        "run_id": run_id,
                    }
                )

            insert_rows("project_document_map", rows)
            projects_processed += 1
            mappings_created += len(rows)

        update_run_success(
            "taxonomy_runs",
            run_id,
            projects_processed,
            len(documents),
            mappings_created,
        )

        print(
            json.dumps(
                {
                    "status": "ok",
                    "run_id": run_id,
                    "projects_processed": projects_processed,
                    "documents_processed": len(documents),
                    "mappings_created": mappings_created,
                }
            )
        )

    except Exception as e:
        update_run_error("taxonomy_runs", run_id, str(e))
        raise


def update_run_success(table_name, run_id, projects_processed, documents_processed, mappings_created):
    sql = f"""
    UPDATE `{PROJECT_ID}.{DATASET}.{table_name}`
    SET
      finished_at = CURRENT_TIMESTAMP(),
      status = 'success',
      projects_processed = @projects_processed,
      documents_processed = @documents_processed,
      mappings_created = @mappings_created
    WHERE run_id = @run_id
    """
    bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("projects_processed", "INT64", projects_processed),
                bigquery.ScalarQueryParameter("documents_processed", "INT64", documents_processed),
                bigquery.ScalarQueryParameter("mappings_created", "INT64", mappings_created),
            ]
        ),
    ).result()


def update_run_error(table_name, run_id, error_message):
    sql = f"""
    UPDATE `{PROJECT_ID}.{DATASET}.{table_name}`
    SET
      finished_at = CURRENT_TIMESTAMP(),
      status = 'error',
      error_message = @error_message
    WHERE run_id = @run_id
    """
    bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("error_message", "STRING", error_message[:1000]),
            ]
        ),
    ).result()


def get_project(project_id):
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.projects`
    WHERE project_id = @project_id
    LIMIT 1
    """
    rows = run_query(
        sql,
        [
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
        ],
    )
    return rows[0] if rows else None


def get_project_documents(project_id, limit=30):
    sql = f"""
    SELECT
      d.document_id,
      d.title,
      d.url,
      d.full_text
    FROM `{PROJECT_ID}.{DATASET}.project_document_map` m
    JOIN `{PROJECT_ID}.{DATASET}.documents` d
      ON m.document_id = d.document_id
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


def generate_wiki_for_project(project_id):
    run_id = str(uuid.uuid4())
    started_at = now_iso()

    project = get_project(project_id)
    if not project:
        raise RuntimeError(f"Project not found: {project_id}")

    docs = get_project_documents(project_id)

    source_docs = []
    for doc in docs:
        source_docs.append(
            {
                "document_id": doc.document_id,
                "title": doc.title,
                "url": doc.url,
                "content": (doc.full_text or "")[:12000],
            }
        )

    prompt = f"""
You are a technical documentarian for Admind Agency.

Write the internal wiki page for:
Project: {project.project_name} ({project.project_code}) | Client: {project.client_name}

Using ONLY the source documents below, write Markdown with sections:
## Overview
## Brief & Objectives
## Team
## Timeline
## Key Decisions
## Design Assets
## Meeting Intelligence
## Source Coverage

Rules:
- Only use information stated in the sources.
- Do not invent.
- Cite source document title for each important fact.
- Flag sections with low source coverage.
- If a section has no source support, write "Not found in available sources."

Source documents:
{json.dumps(source_docs, ensure_ascii=False)}
"""

    response = model.generate_content(
        prompt,
        generation_config=GenerationConfig(
            temperature=0.2,
        ),
    )

    markdown = response.text

    db.collection("wiki").document(project_id).set(
        {
            "project_id": project_id,
            "project_name": project.project_name,
            "project_code": project.project_code,
            "client_name": project.client_name,
            "markdown": markdown,
            "generated_at": firestore.SERVER_TIMESTAMP,
            "generated_by_model": MODEL_NAME,
            "source_document_ids": [doc.document_id for doc in docs],
        }
    )

    insert_rows(
        "wiki_generation_runs",
        [
            {
                "run_id": run_id,
                "project_id": project_id,
                "started_at": started_at,
                "finished_at": now_iso(),
                "status": "success",
                "model": MODEL_NAME,
                "source_document_count": len(docs),
                "error_message": None,
            }
        ],
    )

    print(
        json.dumps(
            {
                "status": "ok",
                "project_id": project_id,
                "source_document_count": len(docs),
            }
        )
    )


def wiki_generate_all():
    projects = get_active_projects(limit=int(os.getenv("PROJECT_LIMIT", "20")))
    for project in projects:
        generate_wiki_for_project(project.project_id)


if __name__ == "__main__":
    job = os.getenv("JOB_TYPE", "taxonomy-sync")

    if job == "taxonomy-sync":
        taxonomy_sync()
    elif job == "wiki-generate":
        wiki_generate_all()
    elif job == "wiki-generate-one":
        project_id = os.getenv("PROJECT_ID_TO_GENERATE")
        if not project_id:
            raise RuntimeError("PROJECT_ID_TO_GENERATE is required")
        generate_wiki_for_project(project_id)
    else:
        raise RuntimeError(f"Unknown JOB_TYPE: {job}")
