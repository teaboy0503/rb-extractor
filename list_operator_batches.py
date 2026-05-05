import argparse
import csv
import json
import os

from google.cloud import storage
from google.oauth2 import service_account


GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
BUCKET_NAME = os.getenv("BATCH_GCS_BUCKET", "rb-title-pages-2026")
BATCH_UPLOAD_ROOT_PREFIX = os.getenv("BATCH_UPLOAD_ROOT_PREFIX", "imports/")


def normalize_prefix(value):
    value = (value or "").strip().strip("/")
    return f"{value}/" if value else ""


def get_storage_client():
    if not GOOGLE_CREDENTIALS_JSON:
        return storage.Client()

    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return storage.Client(credentials=credentials, project=creds_dict.get("project_id"))


def batch_root_prefix(batch_id):
    return f"{normalize_prefix(BATCH_UPLOAD_ROOT_PREFIX)}{batch_id}/"


def batch_input_prefix(batch_id):
    return f"{batch_root_prefix(batch_id)}to_process/"


def batch_results_path(batch_id):
    return f"results/batches/{batch_id}.csv"


def batch_run_command(batch_id):
    return (
        f"IMPORT_BATCH_ID={batch_id} "
        f"BATCH_INPUT_PREFIX={batch_input_prefix(batch_id)} "
        f"BATCH_RESULTS_PATH={batch_results_path(batch_id)} "
        "python3 run_import_pipeline.py"
    )


def list_batch_ids(client):
    batch_ids = set()
    root_prefix = normalize_prefix(BATCH_UPLOAD_ROOT_PREFIX)
    iterator = client.list_blobs(BUCKET_NAME, prefix=root_prefix, delimiter="/")

    for page in iterator.pages:
        for prefix in page.prefixes:
            parts = prefix.strip("/").split("/")
            batch_id = parts[-1] if parts else ""
            if batch_id.startswith("batch-"):
                batch_ids.add(batch_id)

    return sorted(batch_ids)


def read_manifest(bucket, batch_id):
    blob = bucket.blob(f"{batch_root_prefix(batch_id)}batch.json")
    if not blob.exists():
        return {}

    return json.loads(blob.download_as_text(encoding="utf-8"))


def count_uploads(client, batch_id):
    count = 0
    for blob in client.list_blobs(BUCKET_NAME, prefix=batch_input_prefix(batch_id)):
        if not blob.name.endswith("/"):
            count += 1
    return count


def results_counts(bucket, batch_id):
    blob = bucket.blob(batch_results_path(batch_id))
    counts = {"exists": False, "total": 0, "success": 0, "failed": 0}

    if not blob.exists():
        return counts

    counts["exists"] = True
    rows = csv.DictReader(blob.download_as_text(encoding="utf-8").splitlines())

    for row in rows:
        counts["total"] += 1
        status = (row.get("status") or "").strip().lower()
        if status == "success":
            counts["success"] += 1
        elif status == "failed":
            counts["failed"] += 1

    return counts


def sort_key(batch):
    return batch["manifest"].get("created_at") or batch["batch_id"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="List operator upload batches stored in GCS."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of recent batches to show.",
    )
    parser.add_argument(
        "--batch-id",
        default="",
        help="Show one specific batch ID.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    client = get_storage_client()
    bucket = client.bucket(BUCKET_NAME)

    batch_ids = [args.batch_id.strip()] if args.batch_id else list_batch_ids(client)
    batches = []

    for batch_id in batch_ids:
        if not batch_id:
            continue
        manifest = read_manifest(bucket, batch_id)
        batches.append(
            {
                "batch_id": batch_id,
                "manifest": manifest,
                "uploaded_count": count_uploads(client, batch_id),
                "results": results_counts(bucket, batch_id),
            }
        )

    batches.sort(key=sort_key, reverse=True)
    if args.limit > 0:
        batches = batches[: args.limit]

    print(f"Bucket: {BUCKET_NAME}")
    print(f"Batch root: {normalize_prefix(BATCH_UPLOAD_ROOT_PREFIX)}")

    if not batches:
        print("No operator batches found.")
        return

    for batch in batches:
        manifest = batch["manifest"]
        results = batch["results"]
        batch_id = batch["batch_id"]

        print("")
        print(batch_id)
        if manifest.get("created_at"):
            print(f"Created: {manifest['created_at']}")
        if manifest.get("location"):
            print(f"Location: {manifest['location']}")
        if manifest.get("target_collection"):
            print(f"Collection: {manifest['target_collection']}")
        print(f"Uploads: {batch['uploaded_count']}")
        print(
            "Results: "
            f"{results['total']} rows, "
            f"{results['success']} success, "
            f"{results['failed']} failed"
        )
        print(f"Command: {manifest.get('run_command') or batch_run_command(batch_id)}")


if __name__ == "__main__":
    main()
