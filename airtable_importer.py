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
AIRTABLE_BATCH_TABLE_NAME = os.getenv("AIRTABLE_BATCH_TABLE_NAME", "Batches")
AIRTABLE_BATCH_NAME_FIELD = os.getenv("AIRTABLE_BATCH_NAME_FIELD", "Batch name")
AIRTABLE_ITEM_BATCH_LINK_FIELD = os.getenv("AIRTABLE_ITEM_BATCH_LINK_FIELD", "Related Batch")
AIRTABLE_FAILURE_TABLE_NAME = os.getenv("AIRTABLE_FAILURE_TABLE_NAME", "")
AIRTABLE_FAILURE_BATCH_LINK_FIELD = os.getenv("AIRTABLE_FAILURE_BATCH_LINK_FIELD", "")
AIRTABLE_QUALITY_FLAGS_FIELD = os.getenv("AIRTABLE_QUALITY_FLAGS_FIELD", "")

MAX_IMPORT_ROWS = int(os.getenv("MAX_IMPORT_ROWS", "25"))
MAX_FAILURE_IMPORT_ROWS = int(os.getenv("MAX_FAILURE_IMPORT_ROWS", str(MAX_IMPORT_ROWS)))

batch_record_cache = {}


def get_storage_client():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return storage.Client(credentials=credentials, project=creds_dict.get("project_id"))


storage_client = get_storage_client()
bucket = storage_client.bucket(BUCKET_NAME)


def clean_gcs_object_path(row):
    final_path = (row.get("final_gcs_path") or "").strip()
    if final_path:
        return final_path

    raw = (
        row.get("GCS object path")
        or row.get("image_ref")
        or row.get("Original filename")
        or ""
    ).strip()

    if not raw:
        return ""

    raw = raw.replace(f"gs://{BUCKET_NAME}/", "")
    raw = raw.replace(f"{BUCKET_NAME}/", "")

    filename = raw.split("/")[-1]

    if ".jpg-" in filename:
        filename = filename.split(".jpg-")[0] + ".jpg"
    if ".jpeg-" in filename:
        filename = filename.split(".jpeg-")[0] + ".jpeg"

    return f"processed/{filename}"


def gcs_path_candidates(gcs_path):
    gcs_path = (gcs_path or "").strip()
    candidates = []

    def add(path):
        if path and path not in candidates:
            candidates.append(path)

    add(gcs_path)

    if gcs_path.startswith("process/"):
        add("processed/" + gcs_path.split("/", 1)[1])

    return candidates


def resolve_existing_gcs_path(gcs_path):
    for candidate in gcs_path_candidates(gcs_path):
        if bucket.blob(candidate).exists():
            return candidate

    raise RuntimeError(f"GCS object not found: gs://{BUCKET_NAME}/{gcs_path}")


def existing_path_for_gcs_path(gcs_path, existing_paths):
    for candidate in gcs_path_candidates(gcs_path):
        if candidate in existing_paths:
            return candidate

    return None


def csv_gcs_object_path(row):
    raw = (
        row.get("final_gcs_path")
        or row.get("GCS object path")
        or row.get("image_ref")
        or row.get("source_gcs_path")
        or row.get("Original filename")
        or ""
    ).strip()

    raw = raw.replace(f"gs://{BUCKET_NAME}/", "")
    raw = raw.replace(f"{BUCKET_NAME}/", "")
    return raw


def download_results_csv():
    blob = bucket.blob(RESULTS_PATH)
    if not blob.exists():
        raise RuntimeError(f"Results CSV not found: gs://{BUCKET_NAME}/{RESULTS_PATH}")

    content = blob.download_as_text(encoding="utf-8")
    return list(csv.DictReader(content.splitlines()))


def signed_url_for_gcs_path(gcs_path):
    clean_path = resolve_existing_gcs_path(gcs_path)
    blob = bucket.blob(clean_path)

    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=2),
        method="GET",
    )


def airtable_url(table_name=None):
    table = quote(table_name or AIRTABLE_TABLE_NAME)
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}"


def airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }


def airtable_formula_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def retry_count_for_row(row):
    attempts = row.get("extraction_attempts")
    if attempts in [None, ""]:
        return 0

    try:
        return max(0, int(float(attempts)) - 1)
    except:
        return 0


def get_or_create_batch_record(batch_name):
    batch_name = (batch_name or "").strip()
    if not batch_name:
        return None

    if batch_name in batch_record_cache:
        return batch_record_cache[batch_name]

    response = requests.get(
        airtable_url(AIRTABLE_BATCH_TABLE_NAME),
        headers=airtable_headers(),
        params={
            "filterByFormula": f"{{{AIRTABLE_BATCH_NAME_FIELD}}} = {airtable_formula_string(batch_name)}",
            "maxRecords": 1,
        },
        timeout=60,
    )

    if not response.ok:
        raise RuntimeError(f"Airtable batch lookup error {response.status_code}: {response.text}")

    records = response.json().get("records", [])
    if records:
        batch_record_id = records[0]["id"]
        batch_record_cache[batch_name] = batch_record_id
        return batch_record_id

    response = requests.post(
        airtable_url(AIRTABLE_BATCH_TABLE_NAME),
        headers=airtable_headers(),
        json={"fields": {AIRTABLE_BATCH_NAME_FIELD: batch_name}},
        timeout=60,
    )

    if not response.ok:
        raise RuntimeError(f"Airtable batch create error {response.status_code}: {response.text}")

    batch_record_id = response.json()["id"]
    batch_record_cache[batch_name] = batch_record_id
    return batch_record_id


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


def get_existing_failure_paths():
    if not AIRTABLE_FAILURE_TABLE_NAME:
        return set()

    existing = set()
    offset = None

    while True:
        url = airtable_url(AIRTABLE_FAILURE_TABLE_NAME)
        if offset:
            url = f"{url}?offset={offset}"

        response = requests.get(url, headers=airtable_headers(), timeout=60)

        if not response.ok:
            raise RuntimeError(f"Airtable failure read error {response.status_code}: {response.text}")

        data = response.json()

        for record in data.get("records", []):
            path = record.get("fields", {}).get("GCS object path")
            if path:
                existing.add(path)

        offset = data.get("offset")
        if not offset:
            break

    return existing


def create_airtable_record(row):
    clean_path = resolve_existing_gcs_path(clean_gcs_object_path(row))
    image_url = signed_url_for_gcs_path(clean_path)
    batch_record_id = get_or_create_batch_record(row.get("import_batch_id"))

    fields = {
        "Title page image": [
            {
                "url": image_url,
                "filename": clean_path.split("/")[-1],
            }
        ],
        "GCS bucket": BUCKET_NAME,
        "GCS object path": clean_path,
        "Original filename": row.get("Original filename", clean_path.split("/")[-1]),
        "Image source": "Google Cloud Storage",
        "Image format": row.get("Image format", "JPG"),

        "OCR raw text": row.get("ocr_text", ""),
        "OCR confidence": float(row["ocr_confidence"]) if row.get("ocr_confidence") else None,
        "OCR length": int(float(row["ocr_length"])) if row.get("ocr_length") else None,

        "LLM confidence": float(row["llm_confidence"]) if row.get("llm_confidence") else None,
        "Extraction attempts": int(float(row["extraction_attempts"])) if row.get("extraction_attempts") else None,
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

        "Extraction status": "Done",
        "Processing status": "Extracted",
    }

    if AIRTABLE_QUALITY_FLAGS_FIELD and row.get("quality_flags_json"):
        fields[AIRTABLE_QUALITY_FLAGS_FIELD] = row.get("quality_flags_json")

    if batch_record_id:
        fields[AIRTABLE_ITEM_BATCH_LINK_FIELD] = [batch_record_id]

    fields = {k: v for k, v in fields.items() if v not in [None, ""]}

    response = requests.post(
        airtable_url(),
        headers=airtable_headers(),
        json={"fields": fields},
        timeout=60,
    )

    if not response.ok:
        raise RuntimeError(f"Airtable error {response.status_code}: {response.text}")

    return response.json()


def create_failure_record(row):
    gcs_path = csv_gcs_object_path(row)
    batch_record_id = get_or_create_batch_record(row.get("import_batch_id"))

    fields = {
        "GCS bucket": BUCKET_NAME,
        "GCS object path": gcs_path,
        "Original filename": row.get("Original filename", gcs_path.split("/")[-1]),
        "Failure stage": "Extraction",
        "Error message": row.get("error", ""),
        "Retry count": retry_count_for_row(row),
        "Resolved?": False,
    }

    if batch_record_id and AIRTABLE_FAILURE_BATCH_LINK_FIELD:
        fields[AIRTABLE_FAILURE_BATCH_LINK_FIELD] = [batch_record_id]

    fields = {k: v for k, v in fields.items() if v not in [None, ""]}

    response = requests.post(
        airtable_url(AIRTABLE_FAILURE_TABLE_NAME),
        headers=airtable_headers(),
        json={"fields": fields},
        timeout=60,
    )

    if not response.ok:
        raise RuntimeError(f"Airtable failure create error {response.status_code}: {response.text}")

    return response.json()


def main():
    if not AIRTABLE_API_KEY:
        raise RuntimeError("AIRTABLE_API_KEY missing")
    if not AIRTABLE_BASE_ID:
        raise RuntimeError("AIRTABLE_BASE_ID missing")

    rows = download_results_csv()
    successful_rows = [row for row in rows if row.get("status") == "success"]
    failed_rows = [row for row in rows if row.get("status") == "failed"]

    print(f"Found {len(successful_rows)} successful rows in CSV")
    print(f"Found {len(failed_rows)} failed rows in CSV")

    existing_paths = get_existing_gcs_paths()
    print(f"Found {len(existing_paths)} existing Airtable records with GCS paths")
    existing_failure_paths = get_existing_failure_paths()
    if AIRTABLE_FAILURE_TABLE_NAME:
        print(f"Found {len(existing_failure_paths)} existing Airtable failure records with GCS paths")

    imported = 0
    skipped = 0
    duplicate_skipped = 0
    duplicate_examples = []
    failure_imported = 0
    failure_skipped = 0
    failure_duplicate_skipped = 0
    failure_duplicate_examples = []

    for row in successful_rows:
        if imported >= MAX_IMPORT_ROWS:
            break

        gcs_path = clean_gcs_object_path(row)

        if not gcs_path:
            print(f"Skipping row with missing GCS object path: {row.get('Original filename')}")
            skipped += 1
            continue

        existing_gcs_path = existing_path_for_gcs_path(gcs_path, existing_paths)
        if existing_gcs_path:
            duplicate_skipped += 1
            skipped += 1
            if len(duplicate_examples) < 5:
                duplicate_examples.append(existing_gcs_path)
            continue

        try:
            gcs_path = resolve_existing_gcs_path(gcs_path)
        except RuntimeError as error:
            print(f"Skipping row with missing GCS object path: {row.get('Original filename')}: {error}")
            skipped += 1
            continue

        existing_gcs_path = existing_path_for_gcs_path(gcs_path, existing_paths)
        if existing_gcs_path:
            duplicate_skipped += 1
            skipped += 1
            if len(duplicate_examples) < 5:
                duplicate_examples.append(existing_gcs_path)
            continue

        print(f"Importing: {row.get('Original filename')} -> {gcs_path}")
        create_airtable_record(row)
        existing_paths.add(gcs_path)
        imported += 1

    if AIRTABLE_FAILURE_TABLE_NAME:
        for row in reversed(failed_rows):
            if failure_imported >= MAX_FAILURE_IMPORT_ROWS:
                break

            gcs_path = csv_gcs_object_path(row)

            if not gcs_path:
                print(f"Skipping failed row with missing GCS object path: {row.get('Original filename')}")
                failure_skipped += 1
                continue

            existing_failure_path = existing_path_for_gcs_path(gcs_path, existing_failure_paths)
            if existing_failure_path:
                failure_duplicate_skipped += 1
                failure_skipped += 1
                if len(failure_duplicate_examples) < 5:
                    failure_duplicate_examples.append(existing_failure_path)
                continue

            print(f"Recording failure: {row.get('Original filename')} -> {gcs_path}")
            create_failure_record(row)
            existing_failure_paths.add(gcs_path)
            failure_imported += 1

    print(f"Done. Imported {imported} records. Skipped {skipped} records.")
    if duplicate_skipped:
        print(f"Skipped {duplicate_skipped} duplicate successful rows. Examples: {', '.join(duplicate_examples)}")
    if AIRTABLE_FAILURE_TABLE_NAME:
        print(f"Failures recorded {failure_imported}. Failure rows skipped {failure_skipped}.")
        if failure_duplicate_skipped:
            print(
                f"Skipped {failure_duplicate_skipped} duplicate failure rows. "
                f"Examples: {', '.join(failure_duplicate_examples)}"
            )


if __name__ == "__main__":
    main()
