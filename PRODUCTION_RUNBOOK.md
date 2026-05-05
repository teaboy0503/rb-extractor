# Production Runbook

Operational checklist for the GCS -> extractor -> Airtable import pipeline.

## Normal Batch Run

1. Open `/operator` on the Render service.
2. Save the API token.
3. Select an existing collection and location, or add new values first.
4. Create a batch.
5. Upload images. Browser uploads go directly to GCS under `imports/{batch_id}/to_process/`.
6. Click **Run Batch**.
7. Watch **Processing** until the run is complete.
8. Open **Batch Verification** and confirm it is healthy.
9. Check Airtable `Items` for the new rows.

## Smoke Test

Use this after deploys that touch upload, processing, import, Airtable links, or verification.

1. Create or select a test collection.
2. Select a location.
3. Upload 1-2 known-good title page images.
4. Run the batch.
5. Confirm **Batch Verification** is green or only has expected warnings.
6. In Airtable `Items`, confirm each new row has:
   - `Title page image`
   - `GCS bucket`
   - `GCS object path`
   - `Original filename`
   - OCR/extraction fields
   - `Related Batch`
   - linked collection
   - linked location
7. In Airtable `Batches`, confirm the reciprocal item link count matches the imported item count.
8. In GCS, confirm `imports/{batch_id}/to_process/` is empty after a successful run.

## Failure Retry

1. Open the batch in `/operator`.
2. Review the **Failures** panel.
3. Click **Retry Failed Files**.
4. Click **Run Batch** again.
5. Refresh **Batch Verification**.
6. If a file remains unresolved, inspect the error message and the GCS object path.

## Large Uploads

- The browser uploads several files in parallel directly to GCS.
- Keep the browser tab open until the upload summary finishes.
- The page warns before refresh/close while uploads are active.
- If some files fail, leave the selected files in place and click **Upload Selected** again. Already uploaded files are skipped.
- For very large batches, create one batch per coherent shelf/location group so retries and review stay understandable.

## Common Warnings

- **Airtable Items mismatch**: successful CSV rows did not all become linked Airtable rows. Re-run the batch or run the importer with the batch-specific env vars.
- **Waiting files**: files still exist in `to_process/`. Run the batch again if they are intentional new uploads.
- **Unresolved failures**: use the retry flow, then re-run the batch.
- **Missing collection/location links**: confirm the `AIRTABLE_ITEM_COLLECTION_LINK_FIELD` and `AIRTABLE_ITEM_LOCATION_LINK_FIELD` env vars match Airtable exactly.
- **Airtable verification warning**: usually an Airtable field name/env var mismatch or missing token permission.

## Important Env Vars

```text
API_KEY
GOOGLE_CREDENTIALS_JSON
OPENAI_API_KEY
AIRTABLE_API_KEY
AIRTABLE_BASE_ID

BATCH_GCS_BUCKET
BATCH_UPLOAD_ROOT_PREFIX

AIRTABLE_TABLE_NAME
AIRTABLE_BATCH_TABLE_NAME
AIRTABLE_BATCH_NAME_FIELD
AIRTABLE_ITEM_BATCH_LINK_FIELD

AIRTABLE_COLLECTIONS_TABLE_NAME
AIRTABLE_COLLECTION_NAME_FIELD
AIRTABLE_ITEM_COLLECTION_LINK_FIELD

AIRTABLE_LOCATIONS_TABLE_NAME
AIRTABLE_LOCATION_NAME_FIELD
AIRTABLE_ITEM_LOCATION_LINK_FIELD
```

## Manual Command Fallback

If the UI run button is unavailable, copy the run command from `/operator`, or run:

```bash
IMPORT_BATCH_ID=batch-YYYYMMDDTHHMMSSZ \
BATCH_INPUT_PREFIX=imports/batch-YYYYMMDDTHHMMSSZ/to_process/ \
BATCH_RESULTS_PATH=results/batches/batch-YYYYMMDDTHHMMSSZ.csv \
python3 run_import_pipeline.py
```
