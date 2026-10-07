# Azure Data Factory — Demo Project

A hands-on Azure Data Factory project demonstrating real-world ingestion patterns:
**metadata-driven multi-table ingestion with watermark-based incremental loads**,
**event-triggered file processing**, **file management with GetMetadata/Filter/ForEach**,
and **email notifications via Logic Apps**. Secrets are managed through **Azure Key Vault** —
nothing is hardcoded.

> Factory name: `datafactory-` · Source branch: `main` · Publish branch: `adf_publish`

---

## Architecture

```
┌─────────────────────┐      ┌──────────────────────────────┐
│  metadata.table_list │─────▶│ PL_Master_Pipeline            │
└─────────────────────┘      │  ForEach table:               │
                             │   ├─▶ PL_Ingestion (per table) │
┌─────────────────────┐      │   └─▶ collect SUCCESS/FAILURE │
│ metadata.table_     │─────▶│       status per table        │
│ watermarks          │      │                               │
└─────────────────────┘      │  Then: PL_Email_Multitable    │
        │                    │   (one consolidated status    │
        │  per-table flow    │    email via Logic App)       │
        ▼                    └──────────────────────────────┘
┌──────────────────────────────┐
│ PL_Ingestion                  │
│  1. Lookup watermark          │
│  2. Count new rows            │
│     (ModifiedDate > watermark)│
│  3. If count > 0:             │
│     ├─ Copy SQL → ADLS (CSV)  │
│     └─ Update watermark       │
│  4. Email SUCCESS / FAILED    │
└──────────────────────────────┘

Event path:
  BlobCreated in input/ ──▶ TGR_Event_Simple ──▶ PL_File_Event_Trigger_Demo (copy file input → output)

File-management path:
  PL_Copy_Files: list files → keep/copy files starting with 'c' (non-empty) → delete the rest
```

---

## Prerequisites

| Resource | Purpose |
|---|---|
| Azure SQL Database (`database-` on `demo-databaseserver-`) | Source tables + `metadata` schema (`table_list`, `table_watermarks`) |
| ADLS Gen2 (`adlsstorage`) | Landing zone: `input/` and `output/` folders |
| Azure Key Vault (`sqldatabase-keyvault`) | Stores the SQL password as secret `sql-db-password` |
| Azure Logic App (HTTP trigger) | Receives email payloads and sends notification emails |

The SQL source tables are expected to have a `ModifiedDate` column (used for watermark filtering).

---

## Repository structure

```
├── factory/                        # Factory definition + global parameters
├── linkedService/                  # Connections (ADLS, SQL, Key Vault)
├── dataset/                        # Parameterized datasets
├── pipeline/                       # 6 pipelines
├── trigger/                        # Blob event trigger
└── publish_config.json             # Publish branch configuration
```

---

## Linked services

| Name | Type | Details |
|---|---|---|
| `AzureKeyVault1` | Azure Key Vault | Secret store for the SQL database password |
| `LS_ADLS_Storage` | Azure Data Lake Storage Gen2 | `https://adlsstorage.dfs.core.windows.net/` |
| `LS_SQL_Server` | Azure SQL Database | SQL authentication; **password pulled from Key Vault** (`AzureKeyVaultSecret` → `sql-db-password`), never stored in the JSON |

## Datasets (all parameterized — no hardcoded paths)

| Name | Type | Parameters | Notes |
|---|---|---|---|
| `DS_SQL_Server` | Azure SQL table | `schema` (default `dbo`), `table_name` (default `customers`) | Source for ingestion |
| `DS_ADLS_CSV_Demo` | Delimited text (CSV) | `table_name` | Sink; filename = `<table>_<utcNow()>`, folder = `<table>` |
| `DS_Input_File_Path` | Binary | `input_folder` (default `input`), `input_file` | Folder-level listing for GetMetadata |
| `DS_Input_File_Path2` | Binary | `input_folder`, `input_file` | File-level operations (copy/delete) |
| `DS_Output_File_Path` | Binary | `output_folder` (default `output`) | Copy destination |

## Pipelines

### 1. `PL_Master_Pipeline` — metadata-driven orchestrator
The entry point for bulk ingestion.
- **Get_List_Of_Tables** (Lookup): `select table_name from metadata.table_list`
- **Ingest_All_Tables** (ForEach, parallel): for each table, executes `PL_Ingestion` passing `schema`/`table_name`/`Email_URL`; appends `SUCCESS`/`FAILURE` into the `Table_Status` array variable
- **PL_Email_Multitable** (ExecutePipeline): sends one consolidated email listing every table's status, using the recipient list from the factory global parameter `Email_Recepient`
- **Fail** activity if anything fails

### 2. `PL_Ingestion` — incremental per-table load (watermark pattern)
Reusable child pipeline; takes `schema`, `table_name`, `Email_URL`.
1. **Read_Metadata_Table** (Lookup): reads `watermark_value` from `metadata.table_watermarks` for this table
2. **Get_Count_Of_Records** (Lookup): `select count(*) … where ModifiedDate > watermark`
3. **Check_Source_Count** (IfCondition): only proceeds when new rows exist (`count > 0`)
4. **Ingest_SQL_SB_Data** (Copy): `select * … where ModifiedDate > watermark` → timestamped CSV in ADLS
5. **Update_Watermark_Value** (Script): `update metadata.table_watermarks set watermark_value = SYSUTCDATETIME()`
6. **Execute_Send_Success_Email / Execute_Send_Failure_Email**: calls `PL_Email_Singletable` with status

### 3. `PL_Copy_Files` — file management with GetMetadata/Filter/ForEach
Parameters: `input_folder`, `output_folder`, `input_filename`.
- **Get_List_of_all_Files** (GetMetadata): lists child items in the input folder
- **Filter_Files_Starting_With_C**: keeps files starting with `c`
- **ForEach_Activity**: for each kept file, checks size > 1 byte → **Copy** to output folder
- **Filter_Files_Not_Starting_With_C** + **Foreach_Delete_Files_Notstarting_with_C**: deletes the rest (Delete activity)
- Includes a **Wait** activity demo

### 4. `PL_File_Event_Trigger_Demo` — event-driven copy
Parameters: `input_folder`, `input_file`, `output_folder`.
Single **Copy** activity (Binary → Binary) that copies the file that raised the blob event from `input/` to `output/`.

### 5. `PL_Email_Singletable` — reusable email child pipeline
Takes `Email_URL`, `table_name`, `status`, `email_recepient`. Builds subject (`<table> - <status>`) and body with SetVariable activities, then **WebActivity** POSTs a JSON payload to the Logic App HTTP endpoint.

### 6. `PL_Email_Multitable` — consolidated status email
Takes `Email_URL`, `email_recepient`, `table_status` (array). Subject `Ingestion Completed`; body lists every table's status plus the pipeline Run ID, then POSTs to the Logic App.

## Trigger

| Name | Type | Fires on | Target |
|---|---|---|---|
| `TGR_Event_Simple` | Blob Events trigger | `Microsoft.Storage.BlobCreated` under `input/` (empty blobs ignored) | `PL_File_Event_Trigger_Demo`, passing `@triggerBody().fileName` |

## Global parameters

| Name | Value |
|---|---|
| `Email_Recepient` | Semicolon-separated recipient list used by the multi-table status email |

---

## Key concepts demonstrated

- **Parameterization** — datasets and pipelines take parameters; one pipeline pattern serves any table
- **Metadata-driven ingestion** — table list and watermarks live in SQL (`metadata` schema), not in code
- **Incremental loads** — watermark pattern on `ModifiedDate`; watermark updated only after a successful copy
- **Conditional execution** — copy runs only when new rows exist (IfCondition on row count)
- **Event-driven processing** — BlobCreated trigger with the triggering filename passed through
- **File operations** — GetMetadata → Filter → ForEach → Copy/Delete patterns
- **Reusable child pipelines** — ExecutePipeline with `waitOnCompletion`
- **Notifications** — Logic Apps integration for success/failure and consolidated status emails
- **Secret management** — SQL password resolved from Key Vault at runtime
- **Error handling** — Fail activities and per-table SUCCESS/FAILURE tracking

---

## How to run

1. Create the Azure resources listed under Prerequisites (or point the linked services at existing ones).
2. In the SQL database, create the `metadata` schema:
   ```sql
   CREATE SCHEMA metadata;
   CREATE TABLE metadata.table_list (table_name VARCHAR(200));
   CREATE TABLE metadata.table_watermarks (table_name VARCHAR(200), watermark_value DATETIME2);
   ```
   Seed `table_list` with the tables to ingest and `table_watermarks` with an initial watermark per table.
3. Ensure source tables have a `ModifiedDate` column.
4. Import this repo into ADF via **Manage → Git configuration** (or deploy the ARM templates from the `adf_publish` branch).
5. Trigger a run of `PL_Master_Pipeline` (manual or scheduled trigger).

---


