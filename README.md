# rb-extractor

## Operator API

All endpoints use the same bearer token as `/extract`.

Create a batch:

```bash
curl -X POST "$EXTRACTOR_URL/batches" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"source":"web-upload"}'
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
