import argparse
import json
import os
from datetime import UTC, datetime
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

BUCKET_NAME = os.getenv("BATCH_GCS_BUCKET", "rb-title-pages-2026")
FAILED_PREFIX = os.getenv("BATCH_FAILED_PREFIX", "failed/")
RETRY_BATCH_ID = os.getenv("RETRY_BATCH_ID") or datetime.now(UTC).strftime("retry-%Y%m%dT%H%M%SZ")
INPUT_PREFIX = os.getenv("RETRY_INPUT_PREFIX", f"to_process/{RETRY_BATCH_ID}/")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_FAILURE_TABLE_NAME = os.getenv("AIRTABLE_FAILURE_TABLE_NAME", "Import Failures")
AIRTABLE_FAILURE_GCS_PATH_FIELD = os.getenv("AIRTABLE_FAILURE_GCS_PATH_FIELD", "GCS object path")
AIRTABLE_FAILURE_ORIGINAL_FILENAME_FIELD = os.getenv("AIRTABLE_FAILURE_ORIGINAL_FILENAME_FIELD", "Original filename")
AIRTABLE_FAILURE_RESOLVED_FIELD = os.getenv("AIRTABLE_FAILURE_RESOLVED_FIELD", "Resolved?")

MAX_RETRY_FILES = int(os.getenv("MAX_RETRY_FILES", "5"))


def normalize_prefix(prefix):
    prefix = (prefix or "").strip()
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    return prefix


FAILED_PREFIX = normalize_prefix(FAILED_PREFIX)
INPUT_PREFIX = normalize_prefix(INPUT_PREFIX)


def require_config():
    missing = []
    if not GOOGLE_CREDENTIALS_JSON:
        missing.append("GOOGLE_CREDENTIALS_JSON")
    if not AIRTABLE_API_KEY:
        missing.append("AIRTABLE_API_KEY")
    if not AIRTABLE_BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if missing:
        raise RuntimeError(f"Missing required env var(s): {', '.join(missing)}")


def get_storage_client():
    from google.cloud import storage
    from google.oauth2 import service_account

    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return storage.Client(credentials=credentials, project=creds_dict.get("project_id"))


def airtable_headers(content_type=False):
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    if content_type:
        headers["Content-Type"] = "application/json"
    return headers


def airtable_url(record_id=None):
    table = quote(AIRTABLE_FAILURE_TABLE_NAME)
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}"
    if record_id:
        return f"{url}/{record_id}"
    return url


def request_json(method, url, params=None, payload=None):
    if params:
        url = f"{url}?{urlencode(params)}"

    body = None
    headers = airtable_headers(content_type=payload is not None)
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    request = Request(url, data=body, method=method, headers=headers)

    try:
        with urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Airtable API error {error.code}: {body}")


def list_failure_records(max_records):
    records = []
    offset = None

    while len(records) < max_records:
        params = {
            "pageSize": min(100, max_records - len(records)),
            "sort[0][field]": "Import Failure ID",
            "sort[0][direction]": "desc",
        }
        if offset:
            params["offset"] = offset

        data = request_json("GET", airtable_url(), params=params)
        records.extend(data.get("records", []))
        offset = data.get("offset")

        if not offset:
            break

    return records


def unresolved_failures(max_records):
    if max_records <= 0:
        return []

    records = list_failure_records(max(max_records * 5, 100))
    unresolved = []

    for record in records:
        fields = record.get("fields", {})
        if fields.get(AIRTABLE_FAILURE_RESOLVED_FIELD):
            continue
        unresolved.append(record)
        if len(unresolved) >= max_records:
            break

    return unresolved


def clean_gcs_path(path):
    path = (path or "").strip()
    path = path.replace(f"gs://{BUCKET_NAME}/", "")
    path = path.replace(f"{BUCKET_NAME}/", "")
    return path


def path_candidates(fields):
    raw_path = clean_gcs_path(fields.get(AIRTABLE_FAILURE_GCS_PATH_FIELD, ""))
    filename = (
        fields.get(AIRTABLE_FAILURE_ORIGINAL_FILENAME_FIELD)
        or raw_path.split("/")[-1]
        or ""
    ).strip()

    candidates = []

    def add(path):
        if path and path not in candidates:
            candidates.append(path)

    add(raw_path)

    if filename:
        add(f"{FAILED_PREFIX}{filename}")

    if raw_path.startswith("to_process/") and filename:
        add(f"{FAILED_PREFIX}{filename}")

    if raw_path.startswith("processed/") and filename:
        add(f"{FAILED_PREFIX}{filename}")

    return candidates


def resolve_source_blob(bucket, fields):
    for candidate in path_candidates(fields):
        blob = bucket.blob(candidate)
        if blob.exists():
            return blob

    return None


def destination_path_for_blob(blob):
    return f"{INPUT_PREFIX}{blob.name.split('/')[-1]}"


def mark_failure_resolved(record_id):
    return request_json(
        "PATCH",
        airtable_url(record_id),
        payload={"fields": {AIRTABLE_FAILURE_RESOLVED_FIELD: True}},
    )


def move_for_retry(bucket, record, apply_changes, mark_resolved):
    fields = record.get("fields", {})
    source_blob = resolve_source_blob(bucket, fields)

    if not source_blob:
        filename = fields.get(AIRTABLE_FAILURE_ORIGINAL_FILENAME_FIELD, "")
        destination_path = f"{INPUT_PREFIX}{filename}" if filename else ""
        if destination_path and bucket.blob(destination_path).exists():
            if apply_changes and mark_resolved:
                mark_failure_resolved(record["id"])
            return "already_queued", "", destination_path
        return "missing_source", "", destination_path

    destination_path = destination_path_for_blob(source_blob)
    destination_blob = bucket.blob(destination_path)

    if destination_blob.exists():
        if apply_changes and mark_resolved:
            mark_failure_resolved(record["id"])
        return "destination_exists", source_blob.name, destination_path

    if apply_changes:
        bucket.copy_blob(source_blob, bucket, destination_path)
        source_blob.delete()
        if mark_resolved:
            mark_failure_resolved(record["id"])

    return "queued", source_blob.name, destination_path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Move unresolved Airtable import failures from failed/ back to to_process/."
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=MAX_RETRY_FILES,
        help="Maximum unresolved failure records to queue for retry.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually move GCS objects and mark Airtable failures resolved.",
    )
    parser.add_argument(
        "--leave-unresolved",
        action="store_true",
        help="Do not mark Airtable failure records resolved after queueing them.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    require_config()

    mark_resolved = not args.leave_unresolved
    storage_client = get_storage_client()
    bucket = storage_client.bucket(BUCKET_NAME)
    failures = unresolved_failures(args.max_files)

    print("== Retry Failed Files ==")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Failed prefix: {FAILED_PREFIX}")
    print(f"Input prefix: {INPUT_PREFIX}")
    print(f"Retry batch ID: {RETRY_BATCH_ID}")
    print(f"Max files: {args.max_files}")
    print(f"Mode: {'apply' if args.apply else 'dry-run'}")
    print(f"Mark failures resolved: {mark_resolved}")
    print(f"Found {len(failures)} unresolved failure record(s) to inspect")

    counts = {}

    for record in failures:
        fields = record.get("fields", {})
        filename = fields.get(AIRTABLE_FAILURE_ORIGINAL_FILENAME_FIELD, record["id"])
        status, source_path, destination_path = move_for_retry(
            bucket,
            record,
            args.apply,
            mark_resolved,
        )
        counts[status] = counts.get(status, 0) + 1

        if status == "queued":
            action = "Queued" if args.apply else "Would queue"
            print(f"{action}: {filename}: {source_path} -> {destination_path}")
        elif status == "already_queued":
            print(f"Already queued: {filename}: {destination_path}")
        elif status == "destination_exists":
            print(f"Destination exists, skipped move: {filename}: {destination_path}")
        else:
            print(f"Missing failed GCS object: {filename}")

    print("Summary:")
    for status in sorted(counts):
        print(f"- {status}: {counts[status]}")

    if not args.apply:
        print("Dry run only. Re-run with --apply to move files.")
    elif counts.get("queued", 0):
        print("Next command:")
        print(
            f"IMPORT_BATCH_ID={RETRY_BATCH_ID} "
            f"BATCH_INPUT_PREFIX={INPUT_PREFIX} "
            f"MAX_FILES={counts['queued']} "
            "python3 run_import_pipeline.py"
        )


if __name__ == "__main__":
    main()
