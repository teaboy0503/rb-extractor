# Long-Term Improvements

Living notes for productionising the rare books extraction pipeline. Keep this list high-level; turn items into small implementation steps when we are ready to work on them.

## Import Pipeline

- Done: use the end-to-end runner to write per-batch result files at `results/batches/{import_batch_id}.csv`, so the importer does not scan one forever-growing `batch_results.csv`.
- Keep the existing legacy `results/batch_results.csv` for backward compatibility until the per-batch importer is proven.
- Later: add a full durable per-file state model in Supabase: uploaded, queued, extracting, extracted, imported, failed, resolved.
- Done: add per-batch run locking/idempotency so repeated clicks or duplicate jobs cannot process the same batch at the same time.
- Done: avoid deleting source images before result/checkpoint writes are durable.
- Done: use batch-scoped destination names so duplicate filenames from different batches cannot overwrite each other.
- Done: reject extractor successes with very short OCR text so blank records become import failures instead.
- Done: add a controlled retry workflow for files in `failed/`, including moving selected files back to `to_process/` and marking matching failure records resolved.

## Airtable

- Done: update `Batches` records after each import with a run summary in `Batch Notes` and `Date imported`.
- Later: add dedicated writable count/status fields to `Batches` if Airtable becomes the longer-term operations dashboard.
- Batch Airtable writes where safe, respecting Airtable limits.
- Add targeted Airtable lookups instead of scanning full tables as the dataset grows.
- Optionally link `Import Failures` records to `Batches` once the failure table has a batch link field.

## Upload Experience

- Done: add operator API endpoints to create a batch, generate signed GCS upload URLs, and check batch status.
- Done: build a small web interface for creating an import batch and uploading many files.
- Done: upload directly to GCS using signed upload URLs rather than proxying large file uploads through FastAPI.
- Done: store new uploads under a batch-specific prefix such as `imports/{import_batch_id}/to_process/`.
- Done: list recent operator batches in the UI so batches can be recovered after refresh.
- Done: add UI support for viewing unresolved failed rows for the selected batch.
- Done: add UI support for queueing all unresolved failed files back into the selected batch.
- Later: add selectable per-file retry controls once there are enough failures to need finer control.
- Consider resumable browser uploads for very large files or unreliable connections.
- Add lightweight upload validation in the UI, for example file type, duplicate filename warnings, and total batch size.

## Operations

- Done: add `run_import_pipeline.py` as the single operator command/job that runs batch processing and Airtable import in sequence.
- Done: add a UI `Run Batch` action that starts the pipeline and polls durable run status.
- Done: add an operator stop control for active batch runs, with stopped batches restartable later.
- Done: disable completed batch reruns unless there are new files waiting, while still allowing retry after failed/stale runs.
- Keep the current UI-run approach for small operator batches while the system is still Airtable-backed.
- Later: move batch execution to a dedicated Render worker/job queue before relying on very large production batches.
- Add a clearer operator status page: queued/running/succeeded/failed, run duration, imported count, failed count, and latest log tail.
- Add dry-run modes for importer and failure recorder.
- Add a richer failure lifecycle once Supabase exists, for example open, queued_for_retry, retried, resolved, abandoned.
- Add clearer environment documentation for Render jobs and local runs.
- Shorten and sanitize failure messages before writing them to Airtable, especially errors containing signed URLs.
- Done: add lightweight tests for path cleaning, duplicate detection, retry behavior, and Airtable path handling.

## Performance / Scale

- Profile 50, 250, and 500 file runs so slow stages are measured rather than guessed.
- Done: use the local extractor endpoint for UI-started batch runs where possible, avoiding public Render self-calls.
- Tune `SLEEP_SECONDS` downward once extractor/OpenAI/Airtable rate limits are understood.
- Tune `EXTRACTOR_TIMEOUT_SECONDS`, `MAX_EXTRACTOR_ATTEMPTS`, and timeout retry behaviour separately for scale tests vs production runs.
- Add configurable extraction concurrency so multiple files can be OCR/LLM processed in parallel with safe retry/backoff.
- Batch Airtable create/update calls where safe, respecting Airtable's API limits.
- Avoid full Airtable table scans on every import by using targeted lookups or a local per-run cache.
- Move long-running batch execution to a dedicated worker/job queue before relying on 500+ file production batches.
- Consider image preprocessing options, such as resizing very large uploads, if OCR quality remains stable.

## Supabase / Future App

- Design Supabase tables for batches, uploaded files, extraction results, failures, and review status before moving data out of Airtable.
- Treat GCS object paths plus batch IDs as durable references; do not rely on Airtable attachment URLs as source data.
- Build the future front end around the production workflow first: upload, run, review failures, retry, and export/search.
- Plan a one-way migration from Airtable to Supabase, then a short parallel-read period before Airtable is retired.

## Security

- Reduce Airtable token permissions once the import workflow is stable.
- Use a dedicated operator API token for the upload UI and rotate it periodically.
- Keep signed upload URLs short-lived and scoped to one object path.
- Avoid storing long signed URLs or raw provider error blobs in Airtable/Supabase records.
