import os
import csv
import json
import tempfile
import time
from datetime import datetime, UTC

import requests
from google.cloud import storage
from google.oauth2 import service_account


GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

BUCKET_NAME = os.getenv("BATCH_GCS_BUCKET", "rb-title-pages-2026")
INPUT_PREFIX = os.getenv("BATCH_INPUT_PREFIX", "to_process/")
PROCESSED_PREFIX = os.getenv("BATCH_PROCESSED_PREFIX", "processed/")
FAILED_PREFIX = os.getenv("BATCH_FAILED_PREFIX", "failed/")
RESULTS_PATH = os.getenv("BATCH_RESULTS_PATH", "results/batch_results.csv")
IMPORT_BATCH_ID = os.getenv("IMPORT_BATCH_ID") or datetime.now(UTC).strftime("batch-%Y%m%dT%H%M%SZ")

EXTRACTOR_URL = os.getenv("EXTRACTOR_URL", "https://rb-extractor.onrender.com/extract")
EXTRACTOR_API_KEY = os.getenv("API_KEY", "")

MAX_FILES = int(os.getenv("MAX_FILES", "3"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1.5"))
MAX_EXTRACTOR_ATTEMPTS = max(1, int(os.getenv("MAX_EXTRACTOR_ATTEMPTS", "3")))
EXTRACTOR_TIMEOUT_SECONDS = float(os.getenv("EXTRACTOR_TIMEOUT_SECONDS", "180"))
RETRY_EXTRACTOR_TIMEOUTS = os.getenv("RETRY_EXTRACTOR_TIMEOUTS", "").strip().lower() in {"1", "true", "yes"}
RETRY_SLEEP_SECONDS = float(os.getenv("RETRY_SLEEP_SECONDS", "5"))
MIN_SUCCESS_OCR_LENGTH = int(os.getenv("MIN_SUCCESS_OCR_LENGTH", "20"))


class ExtractorError(RuntimeError):
    def __init__(self, status_code, response_text):
        self.status_code = status_code
        super().__init__(f"Extractor error {status_code}: {response_text}")


class ExtractionQualityError(RuntimeError):
    pass


def normalize_prefix(prefix):
    prefix = (prefix or "").strip()
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    return prefix


INPUT_PREFIX = normalize_prefix(INPUT_PREFIX)
PROCESSED_PREFIX = normalize_prefix(PROCESSED_PREFIX)
FAILED_PREFIX = normalize_prefix(FAILED_PREFIX)

storage_client = None
bucket = None


def get_storage_client():
    global storage_client
    if storage_client is not None:
        return storage_client

    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing")

    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    storage_client = storage.Client(credentials=credentials, project=creds_dict.get("project_id"))
    return storage_client


def get_bucket():
    global bucket
    if bucket is None:
        bucket = get_storage_client().bucket(BUCKET_NAME)
    return bucket


def list_input_blobs():
    blobs = list(get_storage_client().list_blobs(BUCKET_NAME, prefix=INPUT_PREFIX))
    return [blob for blob in blobs if not blob.name.endswith("/")][:MAX_FILES]


def call_extractor(blob_name):
    payload = {
        "gcs_bucket": BUCKET_NAME,
        "gcs_object_path": blob_name,
    }

    response = requests.post(
        EXTRACTOR_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {EXTRACTOR_API_KEY}",
        },
        json=payload,
        timeout=EXTRACTOR_TIMEOUT_SECONDS,
    )

    if not response.ok:
        raise ExtractorError(response.status_code, response.text)

    return response.json()


def should_retry_extractor_error(error):
    if isinstance(error, requests.exceptions.ReadTimeout):
        return RETRY_EXTRACTOR_TIMEOUTS
    if isinstance(error, ExtractorError):
        return error.status_code == 429 or error.status_code >= 500
    return True


def call_extractor_with_retries(blob_name):
    last_error = None

    for attempt in range(1, MAX_EXTRACTOR_ATTEMPTS + 1):
        try:
            return call_extractor(blob_name), attempt
        except Exception as error:
            last_error = error
            setattr(last_error, "attempts", attempt)
            if attempt >= MAX_EXTRACTOR_ATTEMPTS or not should_retry_extractor_error(error):
                break

            print(f"Extractor attempt {attempt} failed for {blob_name}: {error}")
            time.sleep(RETRY_SLEEP_SECONDS * attempt)

    raise last_error


def int_value(value, default=0):
    try:
        return int(float(value))
    except:
        return default


def validate_extraction_quality(data):
    ocr_length = int_value(data.get("ocr_length"))
    if ocr_length < MIN_SUCCESS_OCR_LENGTH:
        raise ExtractionQualityError(
            f"OCR text too short: {ocr_length} characters "
            f"(minimum {MIN_SUCCESS_OCR_LENGTH})"
        )


def relative_input_path(source_path):
    source_path = (source_path or "").strip("/")
    input_prefix = normalize_prefix(INPUT_PREFIX)

    if input_prefix and source_path.startswith(input_prefix):
        relative_path = source_path[len(input_prefix):]
    else:
        relative_path = source_path.split("/")[-1]

    parts = [
        part
        for part in relative_path.split("/")
        if part and part not in [".", ".."]
    ]

    if not parts:
        parts = [source_path.split("/")[-1] or "unnamed"]

    return "/".join(parts)


def destination_object_path(source_path, destination_prefix):
    return (
        f"{normalize_prefix(destination_prefix)}"
        f"{IMPORT_BATCH_ID}/"
        f"{relative_input_path(source_path)}"
    )


def copy_blob_to_destination(blob, destination_prefix):
    batch_bucket = get_bucket()
    destination_name = destination_object_path(blob.name, destination_prefix)
    destination_blob = batch_bucket.blob(destination_name)

    if not destination_blob.exists():
        batch_bucket.copy_blob(blob, batch_bucket, destination_name)

    return destination_name


def delete_source_blob(blob):
    try:
        blob.delete()
        return True
    except Exception as error:
        print(f"Warning: could not delete source {blob.name}: {error}")
        return False


def download_existing_results():
    blob = get_bucket().blob(RESULTS_PATH)

    if not blob.exists():
        return []

    content = blob.download_as_text(encoding="utf-8")
    return list(csv.DictReader(content.splitlines()))


def upload_results(rows):
    fieldnames = [
        "processed_at", "source_gcs_path", "final_gcs_path", "status", "error",
        "app_version", "image_source", "image_ref",
        "ocr_text", "ocr_confidence", "ocr_length", "llm_confidence", "language",
        "title", "author", "publication_place", "publisher", "publication_year",
        "edition_statement", "publication_statement_verbatim", "translator",
        "illustration_note", "extraction_evidence_json", "quality_flags_json",
        "extraction_json",
        "GCS bucket", "GCS object path", "Original filename", "Image source",
        "Image format", "Extraction status",
        "import_batch_id", "extraction_attempts",
    ]

    with tempfile.NamedTemporaryFile("w", newline="", encoding="utf-8", delete=False) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})

        temp_path = tmp.name

    get_bucket().blob(RESULTS_PATH).upload_from_filename(temp_path, content_type="text/csv")


def result_row_success(source_path, final_path, data, attempts):
    filename = source_path.split("/")[-1]

    return {
        "processed_at": datetime.now(UTC).isoformat(),
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
        "quality_flags_json": data.get("quality_flags_json", ""),
        "extraction_json": json.dumps(data, ensure_ascii=False),
        "GCS bucket": BUCKET_NAME,
        "GCS object path": final_path,
        "Original filename": filename,
        "Image source": "Google Cloud Storage",
        "Image format": "JPG",
        "Extraction status": "Done",
        "import_batch_id": IMPORT_BATCH_ID,
        "extraction_attempts": attempts,
    }


def result_row_failed(source_path, final_path, error, attempts):
    filename = source_path.split("/")[-1]

    return {
        "processed_at": datetime.now(UTC).isoformat(),
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
        "import_batch_id": IMPORT_BATCH_ID,
        "extraction_attempts": attempts,
    }


def main():
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Input prefix: {INPUT_PREFIX}")
    print(f"Import batch ID: {IMPORT_BATCH_ID}")
    print(f"Results path: {RESULTS_PATH}")
    print(f"Extractor URL: {EXTRACTOR_URL}")
    print(f"Max files: {MAX_FILES}")
    print(f"Sleep seconds: {SLEEP_SECONDS}")
    print(f"Max extractor attempts: {MAX_EXTRACTOR_ATTEMPTS}")
    print(f"Extractor timeout seconds: {EXTRACTOR_TIMEOUT_SECONDS}")
    print(f"Retry extractor timeouts: {RETRY_EXTRACTOR_TIMEOUTS}")
    print(f"Minimum success OCR length: {MIN_SUCCESS_OCR_LENGTH}")

    blobs = list_input_blobs()
    print(f"Found {len(blobs)} file(s) to process")

    if not blobs:
        return

    rows = download_existing_results()

    already_successful = set()
    successful_final_paths = {}
    for row in rows:
        source = row.get("source_gcs_path", "").strip()
        final_path = row.get("final_gcs_path", "").strip()
        status = row.get("status", "").strip().lower()

        if source and status == "success":
            already_successful.add(source)
            successful_final_paths[source] = final_path

    for blob in blobs:
        source_path = blob.name

        if source_path in already_successful:
            print(f"Skipping already successful file: {source_path}")
            final_path = successful_final_paths.get(source_path, "")
            if final_path and get_bucket().blob(final_path).exists():
                if delete_source_blob(blob):
                    print(f"Cleaned up already processed source: {source_path}")
            continue

        print(f"Processing: {source_path}")
        attempts = 0

        try:
            data, attempts = call_extractor_with_retries(source_path)
            validate_extraction_quality(data)

        except Exception as error:
            attempts = getattr(error, "attempts", attempts)
            print(f"Failed: {source_path}: {error}")
            try:
                final_path = copy_blob_to_destination(blob, FAILED_PREFIX)
            except Exception:
                final_path = source_path
            row = result_row_failed(source_path, final_path, error, attempts)
            rows.append(row)
            try:
                upload_results(rows)
            except Exception:
                rows.pop()
                raise
            if final_path != source_path:
                delete_source_blob(blob)
            time.sleep(SLEEP_SECONDS)
            continue

        final_path = copy_blob_to_destination(blob, PROCESSED_PREFIX)
        row = result_row_success(source_path, final_path, data, attempts)
        rows.append(row)

        try:
            upload_results(rows)
        except Exception:
            rows.pop()
            raise

        already_successful.add(source_path)
        successful_final_paths[source_path] = final_path
        delete_source_blob(blob)

        print(f"Success: {source_path} -> {final_path}")

        time.sleep(SLEEP_SECONDS)

    print(f"Done. Results written to gs://{BUCKET_NAME}/{RESULTS_PATH}")


if __name__ == "__main__":
    main()
