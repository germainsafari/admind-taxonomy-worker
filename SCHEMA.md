# BigQuery Schema — admind_data_organisation

Dataset: `admind-data-organisation.admind_data_organisation`

---

## projects

Cleaned/normalised project records upserted from Scoro via `scoro-sync`.

| Field | Type | Notes |
|---|---|---|
| `project_id` | STRING | Primary key — `scoro_{scoro_id}` |
| `scoro_id` | STRING | Numeric Scoro project ID as string |
| `project_no` | STRING | Scoro project number / code (e.g. "566170") |
| `project_name` | STRING | Project display name |
| `status` | STRING | Scoro internal status code, e.g. pending, inprogress, done |
| `status_name` | STRING | Human-readable Scoro status label (e.g. "In progress") |
| `project_manager` | STRING | Full name of project manager |
| `project_manager_name` | STRING | Resolved manager name (same as project_manager) |
| `project_members` | STRING | Comma-separated team member names |
| `start_date` | DATE | Project start date |
| `due_date` | DATE | Project deadline |
| `completed_date` | STRING | Completion date (nullable) |
| `description` | STRING | Project description |
| `client_company` | STRING | Client company name |
| `client_contacts` | STRING | Comma-separated client-side contact names (from Scoro related objects + project name) |
| `project_type` | STRING | e.g. Branding, Digital |
| `client_country` | STRING | Client country |
| `business_area` | STRING | Business area tag |
| `business_line_division` | STRING | Business line / division |
| `budget_type` | STRING | Budget type |
| `po_number` | STRING | Purchase order number |
| `open_po_number` | STRING | Open PO number |
| `related_project` | STRING | Related project reference |
| `google_drive_link` | STRING | Google Drive folder URL |
| `project_priority` | STRING | Priority level |
| `tags` | STRING | Comma-separated Scoro tags |
| `imported_at` | TIMESTAMP | Last upserted timestamp |

---

## project_raw

Raw Scoro API export — source of truth before normalisation. Not written by this worker.

| Field | Type | Notes |
|---|---|---|
| `project_no` | INTEGER | Scoro project number |
| `project_name` | STRING | |
| `is_personal` | INTEGER | |
| `contact_name` | STRING | |
| `contact_type` | STRING | |
| `date` | DATE | Start date |
| `due_date` | DATE | |
| `completed_date` | STRING | |
| `entity_id` | STRING | |
| `shared_between_accounts` | INTEGER | |
| `status` | STRING | |
| `project_manager` | STRING | |
| `project_members` | STRING | |
| `is_private` | INTEGER | |
| `description` | STRING | |
| `estimated_duration` | STRING | |
| `time_planned` | STRING | |
| `scheduled_to_do` | STRING | |
| `unscheduled_to_do` | STRING | |
| `unassigned_to_do` | STRING | |
| `total_to_do` | STRING | |
| `total_done` | STRING | |
| `scheduled_to_do_billable` | STRING | |
| `unscheduled_to_do_billable` | STRING | |
| `unassigned_to_do_billable` | STRING | |
| `total_to_do_billable` | STRING | |
| `done_billable` | STRING | |
| `budget_type` | STRING | |
| `price_list_id` | INTEGER | |
| `budget_revenue` | FLOAT | |
| `actual_revenue` | FLOAT | |
| `budget_bills` | FLOAT | |
| `actual_bills` | FLOAT | |
| `budget_expenses` | FLOAT | |
| `actual_expenses` | FLOAT | |
| `budget_cost` | FLOAT | |
| `actual_cost` | FLOAT | |
| `budget_labor_cost` | FLOAT | |
| `actual_labor_cost` | FLOAT | |
| `tags` | STRING | |
| `related_contacts` | STRING | |
| `c_clientcompany` | INTEGER | FK to Scoro company ID |
| `c_projecttype` | STRING | |
| `c_clientcountry` | STRING | |
| `c_businessarea` | STRING | |
| `c_businesslinedivision` | STRING | |
| `c_budgettype` | STRING | |
| `c_ponumber` | STRING | |
| `c_openponr` | STRING | |
| `c_relatedproject` | STRING | |
| `c_relatedproject_no` | STRING | |
| `c_prepaid` | INTEGER | |
| `c_prepaid_new` | STRING | |
| `c_drivelink` | STRING | Google Drive URL |
| `c_project_priority` | STRING | |

---

## documents

Candidate documents discovered from Discovery Engine / Gemini Enterprise via `document-discovery`.

| Field | Type | Notes |
|---|---|---|
| `document_id` | STRING | Primary key — Discovery Engine document ID |
| `source_system` | STRING | e.g. `discovery_engine` |
| `source_type` | STRING | e.g. `search_result` |
| `title` | STRING | Document title |
| `folder_path` | STRING | Folder path if available |
| `url` | STRING | Link to the document (may be an internal `gs://` connector URI) |
| `source_url` | STRING | User-facing source link (Drive/SharePoint) when provided by Discovery Engine |
| `author` | STRING | Author if available |
| `created_at` | TIMESTAMP | Document creation time |
| `modified_at` | TIMESTAMP | Last modified time |
| `text_preview` | STRING | Short excerpt / snippet (≤1000 chars) |
| `full_text` | STRING | Full text or longer excerpt |
| `imported_at` | TIMESTAMP | Last upserted timestamp |

---

## source_items

Project-level source links (Drive folders, Slack channels, etc.). Manually managed, not written by this worker.

| Field | Type | Notes |
|---|---|---|
| `item_id` | STRING | Primary key |
| `project_id` | STRING | FK → projects.project_id |
| `source_system` | STRING | e.g. google_drive, slack |
| `source_type` | STRING | e.g. folder, channel |
| `title` | STRING | |
| `url` | STRING | |
| `parent_url` | STRING | |
| `author` | STRING | |
| `created_at` | TIMESTAMP | |
| `modified_at` | TIMESTAMP | |
| `text_excerpt` | STRING | |
| `content_type` | STRING | |
| `lifecycle_stage` | STRING | |
| `confidence_score` | FLOAT | |
| `matching_method` | STRING | |
| `needs_human_review` | BOOLEAN | |
| `imported_at` | TIMESTAMP | |

---

## project_document_candidates

Per-project document candidates returned by Discovery Engine, written by
`document-discovery`. This table preserves the **project → document** link so
`taxonomy-sync` classifies each project against *its own* candidates instead of a
global random pool. Auto-created by the worker (`ensure_schema()`); the DDL below
is for reference.

```sql
CREATE TABLE IF NOT EXISTS
  `admind-data-organisation.admind_data_organisation.project_document_candidates` (
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
  );
```

| Field | Type | Notes |
|---|---|---|
| `project_id` | STRING | FK → projects.project_id |
| `document_id` | STRING | FK → documents.document_id |
| `discovery_query` | STRING | The search query that surfaced this candidate |
| `rank` | INT64 | Discovery rank (lower = higher relevance) |
| `title` | STRING | Document title |
| `url` | STRING | Link (may be internal `gs://`) |
| `source_url` | STRING | User-facing source link when available |
| `text_preview` | STRING | Short excerpt |
| `discovered_at` | TIMESTAMP | When discovered |
| `run_id` | STRING | FK → pipeline_runs.run_id |

---

## project_document_map

LLM-classified project↔document relationships written by `taxonomy-sync`.

| Field | Type | Notes |
|---|---|---|
| `project_id` | STRING | FK → projects.project_id |
| `document_id` | STRING | FK → documents.document_id |
| `confidence_score` | FLOAT | 0–1, only rows ≥ 0.75 are written |
| `matching_method` | STRING | `gemini_classification` or `openai_classification` |
| `classifier_reason` | STRING | LLM explanation |
| `classified_at` | TIMESTAMP | |
| `run_id` | STRING | FK → pipeline_runs.run_id |

---

## taxonomy_runs

Legacy job log for taxonomy-sync runs. Kept for backward compatibility.
New code writes to `pipeline_runs` instead.

| Field | Type | Notes |
|---|---|---|
| `run_id` | STRING | |
| `job_name` | STRING | |
| `started_at` | TIMESTAMP | |
| `finished_at` | TIMESTAMP | |
| `status` | STRING | `success` or `error` |
| `projects_processed` | INTEGER | |
| `documents_processed` | INTEGER | |
| `mappings_created` | INTEGER | |
| `error_message` | STRING | |

---

## wiki_generation_runs

Legacy job log for wiki generation runs. Kept for backward compatibility.
New code writes to `pipeline_runs` instead.

| Field | Type | Notes |
|---|---|---|
| `run_id` | STRING | |
| `project_id` | STRING | |
| `started_at` | TIMESTAMP | |
| `finished_at` | TIMESTAMP | |
| `status` | STRING | |
| `model` | STRING | LLM model label used |
| `source_document_count` | INTEGER | |
| `error_message` | STRING | |

---

## pipeline_runs

Unified append-only job log for all pipeline stages. **Create this table before deploying.**

```sql
CREATE TABLE IF NOT EXISTS
  `admind-data-organisation.admind_data_organisation.pipeline_runs` (
    run_id          STRING,
    job_name        STRING,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          STRING,
    records_read    INT64,
    records_written INT64,
    error_message   STRING
  );
```

| Field | Type | Notes |
|---|---|---|
| `run_id` | STRING | UUID |
| `job_name` | STRING | `scoro-sync`, `document-discovery`, `taxonomy-sync`, `wiki-generate`, `full-sync` |
| `started_at` | TIMESTAMP | |
| `finished_at` | TIMESTAMP | |
| `status` | STRING | `success` or `error` |
| `records_read` | INT64 | Input record count |
| `records_written` | INT64 | Output record count |
| `error_message` | STRING | First 1000 chars of exception message |

---

## Key relationships

```
projects (project_id)
    │
    ├─── source_items (project_id)
    │
    ├─── project_document_candidates (project_id) ──── documents (document_id)
    │         (discovery output, per-project)
    │
    └─── project_document_map (project_id) ──── documents (document_id)
              (LLM-confirmed, per-project)
```
