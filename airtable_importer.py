import os
import csv
import json
from datetime import timedelta
from urllib.parse import quote

import requests
from google.cloud import storage
from google.oauth2 import service_account


GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

BUCKET_NAME = os.getenv("BATCH_GCS_BUCKET", "rb-title-pages-2026")
RESULTS_PATH = os.getenv("BATCH_RESULTS_PATH", "results/batch_results.csv")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Items")

MAX_IMPORT_ROWS = int(os.getenv("MAX_IMPORT_ROWS", "25"))


def get_storage_client():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return storage.Client(credentials=credentials, project=creds_dict.get("project_id"))


storage_client = get_storage_client()
bucket = storage_client.bucket(BUCKET_NAME)


def clean_gcs_object_path(row):
    raw = (
        row.get("final_gcs_path")
        or row.get("GCS object path")
        or row.get("image_ref")
        or row.get("Original filename")
        or ""
    ).strip()

    raw = raw.replace(f"gs://{BUCKET_NAME}/", "")
    raw = raw.replace(f"{BUCKET_NAME}/", "")

    filename = raw.split("/")[-1]

    if ".jpg-" in filename:
        filename = filename.split(".jpg-")[0] + ".jpg"
    if ".jpeg-" in filename:
        filename = filename.split(".jpeg-")[0] + ".jpeg"

    return f"processed/{filename}"


def download_results_csv():
    blob = bucket.blob(RESULTS_PATH)
    if not blob.exists():
        raise RuntimeError(f"Results CSV not found: gs://{BUCKET_NAME}/{RESULTS_PATH}")

    content = blob.download_as_text(encoding="utf-8")
    return list(csv.DictReader(content.splitlines()))


def signed_url_for_gcs_path(gcs_path):
    blob = bucket.blob(gcs_path)
    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=2),
        method="GET",
    )


def airtable_url():
    table = quote(AIRTABLE_TABLE_NAME)
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}"


def get_existing_gcs_paths():
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    existing = set()
    offset = None

    while True:
        url = airtable_url()
        if offset:
            url = f"{url}?offset={offset}"

        response = requests.get(url, headers=headers, timeout=60)

        if not response.ok:
            raise RuntimeError(f"Airtable read error {response.status_code}: {response.text}")

        data = response.json()

        for record in data.get("records", []):
            fields = record.get("fields", {})
            path = fields.get("GCS object path raw") or fields.get("GCS object path")
            if path:
                existing.add(path)

        offset = data.get("offset")
        if not offset:
            break

    return existing


def create_airtable_record(row):
    clean_path = clean_gcs_object_path(row)
    image_url = signed_url_for_gcs_path(clean_path)

    fields = {
        "Title page image": [
            {
                "url": image_url,
                "filename": clean_path.split("/")[-1],
            }
        ],
        "GCS bucket": BUCKET_NAME,
        "GCS object path raw": clean_path,
        "Original filename": row.get("Original filename", clean_path.split("/")[-1]),
        "Image source": "Google Cloud Storage",
        "Image format": row.get("Image format", "JPG"),

        "OCR raw text": row.get("ocr_text", ""),
        "OCR confidence": float(row["ocr_confidence"]) if row.get("ocr_confidence") else None,
        "OCR length": int(float(row["ocr_length"])) if row.get("ocr_length") else None,

        "LLM confidence": float(row["llm_confidence"]) if row.get("llm_confidence") else None,
        "Language detected": row.get("language", "Other/Unknown") or "Other/Unknown",

        "Title (extracted)": row.get("title", ""),
        "Author (extracted)": row.get("author", ""),
        "Publication place (extracted)": row.get("publication_place", ""),
        "Publisher/imprint (extracted)": row.get("publisher", ""),
        "Publication year (extracted)": int(float(row["publication_year"])) if row.get("publication_year") else None,
        "Edition statement (extracted)": row.get("edition_statement", ""),
        "Publication statement (verbatim)": row.get("publication_statement_verbatim", ""),
        "Translator (extracted)": row.get("translator", ""),
        "Illustration note (extracted)": row.get("illustration_note", ""),

        "Extraction JSON": row.get("extraction_json", ""),
        "Extraction evidence JSON": row.get("extraction_evidence_json", ""),
        "Quality flags JSON": row.get("quality_flags_json", ""),

        "Extraction status": "Done",
        "Processing status": "Extracted",
    }

    fields = {k: v for k, v in fields.items() if v not in [None, ""]}

    response = requests.post(
        airtable_url(),
        headers={
            "Authorization": f"Bearer {AIRTABLE_API_KEY}",
            "Content-Type": "application/json",
        },
        json={"fields": fields},
        timeout=60,
    )

    if not response.ok:
        raise RuntimeError(f"Airtable error {response.status_code}: {response.text}")

    return response.json()


def main():
    if not AIRTABLE_API_KEY:
        raise RuntimeError("AIRTABLE_API_KEY missing")
    if not AIRTABLE_BASE_ID:
        raise RuntimeError("AIRTABLE_BASE_ID missing")

    rows = download_results_csv()
    successful_rows = [row for row in rows if row.get("status") == "success"]

    print(f"Found {len(successful_rows)} successful rows in CSV")

    existing_paths = get_existing_gcs_paths()
    print(f"Found {len(existing_paths)} existing Airtable records with GCS paths")

    imported = 0
    skipped = 0

    for row in successful_rows:
        if imported >= MAX_IMPORT_ROWS:
            break

        gcs_path = clean_gcs_object_path(row)

        if not gcs_path:
            print(f"Skipping row with missing GCS object path: {row.get('Original filename')}")
            skipped += 1
            continue

        if gcs_path in existing_paths:
            print(f"Skipping duplicate: {gcs_path}")
            skipped += 1
            continue

        print(f"Importing: {row.get('Original filename')} -> {gcs_path}")
        create_airtable_record(row)
        existing_paths.add(gcs_path)
        imported += 1

    print(f"Done. Imported {imported} records. Skipped {skipped} records.")


if __name__ == "__main__":
    main()
