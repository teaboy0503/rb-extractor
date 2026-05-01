import os
import csv
import json
import tempfile
from datetime import timedelta

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
    table = AIRTABLE_TABLE_NAME.replace(" ", "%20")
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}"


def create_airtable_record(row):
    image_url = signed_url_for_gcs_path(row["GCS object path"])

    fields = {
        "Title page image": [
            {
                "url": image_url,
                "filename": row.get("Original filename", "title-page.jpg"),
            }
        ],

        "GCS bucket": row.get("GCS bucket", BUCKET_NAME),
        "GCS object path": row.get("GCS object path", ""),
        "Original filename": row.get("Original filename", ""),
        "Image source": {"name": "Google Cloud Storage"},
        "Image format": {"name": row.get("Image format", "JPG")},

        "OCR raw text": row.get("ocr_text", ""),
        "OCR confidence": float(row["ocr_confidence"]) if row.get("ocr_confidence") else None,
        "OCR length": int(float(row["ocr_length"])) if row.get("ocr_length") else None,

        "LLM confidence": float(row["llm_confidence"]) if row.get("llm_confidence") else None,
        "Language detected": {"name": row.get("language", "Other/Unknown") or "Other/Unknown"},

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

        "Extraction status": {"name": "Done"},
        "Processing status": {"name": "Extracted"},
    }

    # Remove empty/null fields Airtable may dislike
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

    successful_rows = [
        row for row in rows
        if row.get("status") == "success"
    ]

    print(f"Found {len(successful_rows)} successful rows in CSV")

    imported = 0

    for row in successful_rows[:MAX_IMPORT_ROWS]:
        print(f"Importing: {row.get('Original filename')}")
        create_airtable_record(row)
        imported += 1

    print(f"Done. Imported {imported} records.")


if __name__ == "__main__":
    main()
