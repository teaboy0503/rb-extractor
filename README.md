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
