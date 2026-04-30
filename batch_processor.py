import os
import csv
import json
import tempfile
from datetime import datetime

import requests
from google.cloud import storage
from google.oauth2 import service_account


# -----------------------------
# Config via Render env vars
# -----------------------------
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

BUCKET_NAME = os.getenv("BATCH_GCS_BUCKET", "rb-title-pages-2026")
INPUT_PREFIX = os.getenv("BATCH_INPUT_PREFIX", "to_process/")
PROCESSED_PREFIX = os.getenv("BATCH_PROCESSED_PREFIX", "processed/")
FAILED_PREFIX = os.getenv("BATCH_FAILED_PREFIX", "failed/")
RESULTS_PATH = os.getenv("BATCH_RESULTS_PATH", "results/batch_results.csv")

EXTRACTOR_URL = os.getenv("EXTRACTOR_URL", "https://rb-extractor.onrender.com/extract")
EXTRACTOR_API_KEY = os.getenv("API_KEY", "")

MAX_FILES = int(os.getenv("MAX_FILES", "3"))


# -----------------------------
# Google client
# -----------------------------
def get_storage_client():
    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing")

    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return storage.Client(credentials=credentials, project=creds_dict.get("project_id"))


storage_client = get_storage_client()
bucket = storage_client.bucket(BUCKET_NAME)


# -----------------------------
# Helpers
# -----------------------------
def list_input_blobs():
    blobs = list(storage_client.list_blobs(BUCKET_NAME, prefix=INPUT_PREFIX))
    return [
        blob for blob in blobs
        if not blob.name.endswith("/")
    ][:MAX_FILES]


def call_extractor(blob_name):
    payload = {
        "record_id": blob_name,
        "gcs_bucket": BUCKET_NAME,
        "gcs_object_path": blob_name,
        "item_id": "",
        "collection": "",
    }

    resp = requests.post(
        EXTRACTOR_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {EXTRACTOR_API_KEY}",
        },
        json=payload,
        timeout=180,
    )

    if not resp.ok:
        raise RuntimeError(f"Extractor error {resp.status_code}: {resp.text}")

    return resp.json()


def move_blob(blob, destination_prefix):
    filename = blob.name.split("/")[-1]
    destination_name = f"{destination_prefix}{filename}"

    bucket.copy_blob(blob, bucket, destination_name)
    blob.delete()

    return destination_name


def download_existing_results():
    blob = bucket.blob(RESULTS_PATH)

    if not blob.exists():
        return []

    content = blob.download_as_text(encoding="utf-8")
    rows = list(csv.DictReader(content.splitlines()))
    return rows


def upload_results(rows):
    fieldnames = [
        "processed_at",
        "source_gcs_path",
        "final_gcs_path",
        "status",
        "error",

        "app_version",
        "image_source",
        "image_ref",

        "ocr_text",
        "ocr_confidence",
        "ocr_length",
        "llm_confidence",
        "language",

        "title",
        "author",
        "publication_place",
        "publisher",
        "publication_year",
        "edition_statement",
        "publication_statement_verbatim",
        "translator",
        "illustration_note",
        "extraction_evidence_json",
        "extraction_json",

        "GCS bucket",
        "GCS object path",
        "Original filename",
        "Image source",
        "Image format",
        "Extraction status",
    ]

    with tempfile.NamedTemporaryFile("w", newline="", encoding="utf-8", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            safe_row = {key: row.get(key, "") for key in fieldnames}
            writer.writerow(safe_row)

        temp_path = f.name

    blob = bucket.blob(RESULTS_PATH)
    blob.upload_from_filename(temp_path, content_type="text/csv")


def result_row_success(source_path, final_path, data):
    filename = source_path.split("/")[-1]

    return {
        "processed_at": datetime.utcnow().isoformat(),
        "source_gcs_path": source_path,
        "final_gcs_path": final_path,
        "status": "success",
        "error": "",

        "app_version": data.get("app_version", ""),
        "image_source": data.get("image_source", ""),
        "image_ref": data.get("image_ref", ""),

        "ocr_text": data.get("ocr_text", ""),
        "ocr_confidence": data.get("ocr_confidence", ""),
        "ocr_length": data.get("ocr_length", ""),
        "llm_confidence": data.get("llm_confidence", ""),
        "language": data.get("language", ""),

        "title": data.get("title", ""),
        "author": data.get("author", ""),
        "publication_place": data.get("publication_place", ""),
        "publisher": data.get("publisher", ""),
        "publication_year": data.get("publication_year", ""),
        "edition_statement": data.get("edition_statement", ""),
        "publication_statement_verbatim": data.get("publication_statement_verbatim", ""),
        "translator": data.get("translator", ""),
        "illustration_note": data.get("illustration_note", ""),
        "extraction_evidence_json": data.get("extraction_evidence_json", ""),
        "extraction_json": json.dumps(data, ensure_ascii=False),

        "GCS bucket": BUCKET_NAME,
        "GCS object path": final_path,
        "Original filename": filename,
        "Image source": "Google Cloud Storage",
        "Image format": "JPG",
        "Extraction status": "Done",
    }


def result_row_failed(source_path, final_path, error):
    filename = source_path.split("/")[-1]

    return {
        "processed_at": datetime.utcnow().isoformat(),
        "source_gcs_path": source_path,
        "final_gcs_path": final_path,
        "status": "failed",
        "error": str(error),

        "GCS bucket": BUCKET_NAME,
        "GCS object path": final_path,
        "Original filename": filename,
        "Image source": "Google Cloud Storage",
        "Image format": "JPG",
        "Extraction status": "Error",
    }


# -----------------------------
# Main
# -----------------------------
def main():
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Input prefix: {INPUT_PREFIX}")
    print(f"Max files: {MAX_FILES}")

    blobs = list_input_blobs()
    print(f"Found {len(blobs)} file(s) to process")

    if not blobs:
        return

    rows = download_existing_results()

    already_recorded = set()
    for row in rows:
        source = row.get("source_gcs_path", "").strip()
    if source:
        already_recorded.add(source)
    for blob in blobs:
        source_path = blob.name
            if source_path in already_recorded:
                print(f"Skipping already recorded file: {source_path}")
                continue

            print(f"Processing: {source_path}")

        try:
            data = call_extractor(source_path)
            final_path = move_blob(blob, PROCESSED_PREFIX)
            rows.append(result_row_success(source_path, final_path, data))
            already_recorded.add(source_path)
            upload_results(rows)
            print(f"Success: {source_path} -> {final_path}")

        except Exception as e:
            print(f"Failed: {source_path}: {e}")

            try:
                final_path = move_blob(blob, FAILED_PREFIX)
            except Exception:
                final_path = source_path

            rows.append(result_row_failed(source_path, final_path, e))
            already_recorded.add(source_path)
            upload_results(rows)

    print(f"Done. Results written to gs://{BUCKET_NAME}/{RESULTS_PATH}")


if __name__ == "__main__":
    main()
