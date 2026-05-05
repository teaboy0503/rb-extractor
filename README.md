# rb-extractor

## Operator API

All endpoints use the same bearer token as `/extract`.

Operator UI:

```text
https://your-render-service.onrender.com/operator
```

The page prompts for the API token in the browser and stores it in session storage.
Browser uploads use signed GCS `PUT` URLs, so the bucket must allow CORS for the
Render origin before direct browser uploads will succeed.

Collection and location dropdowns read from Airtable. The UI lets an operator
choose existing values or add new ones before creating a batch. The importer
then writes linked records into `Items` using these defaults:

```text
AIRTABLE_COLLECTIONS_TABLE_NAME=Collections
AIRTABLE_COLLECTION_NAME_FIELD=Collection name
AIRTABLE_ITEM_COLLECTION_LINK_FIELD=Collection (linked)
AIRTABLE_LOCATIONS_TABLE_NAME=Locations
AIRTABLE_LOCATION_NAME_FIELD=Location Code
AIRTABLE_ITEM_LOCATION_LINK_FIELD=Location
```

## Operator Runbook

1. Open `/operator`.
2. Save the API token.
3. Create a batch with source, collection, location, and notes as needed.
4. Upload files. They go directly to GCS under `imports/{batch_id}/to_process/`.
5. Click **Run Batch**.
6. Watch the processing steps: queued, batch processor, extraction, Airtable import, complete.
7. Check Airtable `Items` and the linked `Batches` record.
8. If failures appear, review the **Failures** panel.
9. Click **Retry Failed Files** to queue failed GCS objects back into the same batch.
10. Click **Run Batch** again to process queued retry files.

Successful and failed objects are moved into batch-scoped folders such as
`processed/{batch_id}/...` and `failed/{batch_id}/...`. This avoids collisions
between different batches that contain the same camera filename.

The processor writes the result row to the batch CSV before deleting the source
object from `to_process/`. If a write or move step fails, the source object is
left in place so it can be safely retried.

After a run succeeds, **Run Batch** is disabled unless new files are waiting.
Failed or stale runs can be retried. A stale run means the UI found an old
`running` status without a live run lock.

Set bucket CORS from Google Cloud Shell or a machine with `gcloud` installed:

```bash
gcloud storage buckets update gs://rb-title-pages-2026 --cors-file=gcs-cors.json
```

Check the applied CORS config:

```bash
gcloud storage buckets describe gs://rb-title-pages-2026 --format="default(cors_config)"
```

Create a batch:

```bash
curl -X POST "$EXTRACTOR_URL/batches" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"source":"web-upload","location":"Shelf A"}'
```

Create a signed GCS upload URL:

```bash
curl -X POST "$EXTRACTOR_URL/batches/{batch_id}/upload-url" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"filename":"IMG_0001.jpg","content_type":"image/jpeg"}'
```

Check batch status:

```bash
curl "$EXTRACTOR_URL/batches/{batch_id}" \
  -H "Authorization: Bearer $API_KEY"
```

List recent operator batches:

```bash
curl "$EXTRACTOR_URL/batches?limit=20" \
  -H "Authorization: Bearer $API_KEY"
```

Run a batch from the web service:

```bash
curl -X POST "$EXTRACTOR_URL/batches/{batch_id}/run" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{}'
```

List unresolved failed rows for a batch:

```bash
curl "$EXTRACTOR_URL/batches/{batch_id}/failures" \
  -H "Authorization: Bearer $API_KEY"
```

Queue failed files back into the same batch:

```bash
curl -X POST "$EXTRACTOR_URL/batches/{batch_id}/retry-failures" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"max_files":25}'
```
