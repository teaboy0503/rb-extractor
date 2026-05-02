# Long-Term Improvements

Living notes for productionising the rare books extraction pipeline. Keep this list high-level; turn items into small implementation steps when we are ready to work on them.

## Import Pipeline

- Use per-batch result files, for example `results/batches/{import_batch_id}.csv`, so the importer does not scan one forever-growing `batch_results.csv`.
- Keep the existing legacy `results/batch_results.csv` for backward compatibility until the per-batch importer is proven.
- Add a durable per-file state model: uploaded, queued, extracting, extracted, imported, failed, resolved.
- Avoid moving source images before result/checkpoint writes are durable.
- Preserve relative source paths or add collision-resistant destination names so duplicate filenames cannot overwrite each other.
- Add a controlled retry workflow for files in `failed/`, including moving selected files back to `to_process/` and marking matching failure records resolved.

## Airtable

- Update `Batches` records with run counts: uploaded, processed, imported, failed, skipped, started at, finished at.
- Batch Airtable writes where safe, respecting Airtable limits.
- Add targeted Airtable lookups instead of scanning full tables as the dataset grows.
- Optionally link `Import Failures` records to `Batches` once the failure table has a batch link field.

## Upload Experience

- Build a small web interface for creating an import batch and uploading many files.
- Upload directly to GCS using signed upload URLs or resumable uploads rather than proxying large file uploads through FastAPI.
- Store new uploads under a batch-specific prefix such as `imports/{import_batch_id}/to_process/`.

## Operations

- Add a single operator command/job that runs batch processing and Airtable import in sequence.
- Add dry-run modes for importer and failure recorder.
- Add clearer environment documentation for Render jobs and local runs.
- Shorten and sanitize failure messages before writing them to Airtable, especially errors containing signed URLs.
- Add lightweight tests for path cleaning, duplicate detection, retry behavior, and Airtable payload construction.
