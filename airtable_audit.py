import argparse
import json
import os
import re
from datetime import datetime
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "appHvUoYJgIIaBWWr")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Items")
AIRTABLE_BATCH_TABLE_NAME = os.getenv("AIRTABLE_BATCH_TABLE_NAME", "Batches")
AIRTABLE_FAILURE_TABLE_NAME = os.getenv("AIRTABLE_FAILURE_TABLE_NAME", "Import Failures")

DEFAULT_SAMPLE_RECORDS = int(os.getenv("AIRTABLE_AUDIT_SAMPLE_RECORDS", "200"))


def headers():
    return {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}


def api_url(table_name):
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote(table_name)}"


def meta_url():
    return f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"


def require_config():
    if not AIRTABLE_API_KEY:
        raise RuntimeError("Set AIRTABLE_API_KEY before running this script")
    if not AIRTABLE_BASE_ID:
        raise RuntimeError("Set AIRTABLE_BASE_ID before running this script")


def request_json(method, url, params=None):
    if params:
        url = f"{url}?{urlencode(params)}"

    request = Request(url, method=method, headers=headers())

    try:
        with urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Airtable API error {error.code}: {body}")


def fetch_schema():
    data = request_json("GET", meta_url())
    return data.get("tables", [])


def table_by_name(schema, table_name):
    for table in schema:
        if table.get("name") == table_name:
            return table
    return None


def field_names(table):
    return [field.get("name") for field in table.get("fields", [])]


def field_type(table, field_name):
    for field in table.get("fields", []):
        if field.get("name") == field_name:
            return field.get("type", "")
    return ""


def list_records(table_name, max_records=100, sort=None):
    records = []
    offset = None

    while len(records) < max_records:
        params = {"pageSize": min(100, max_records - len(records))}

        if offset:
            params["offset"] = offset

        for index, sort_item in enumerate(sort or []):
            params[f"sort[{index}][field]"] = sort_item[0]
            params[f"sort[{index}][direction]"] = sort_item[1]

        data = request_json("GET", api_url(table_name), params=params)
        records.extend(data.get("records", []))
        offset = data.get("offset")

        if not offset:
            break

    return records


def list_records_with_fallback(table_name, max_records=100, sort=None):
    try:
        return list_records(table_name, max_records=max_records, sort=sort)
    except RuntimeError:
        if not sort:
            raise
        return list_records(table_name, max_records=max_records, sort=None)


def get_record(table_name, record_id):
    return request_json("GET", f"{api_url(table_name)}/{record_id}")


def is_non_empty(value):
    if value is None:
        return False
    if value == "":
        return False
    if value == []:
        return False
    return True


def parse_batch_datetime(batch_name):
    match = re.search(r"batch-(\d{8})T(\d{6})Z", batch_name or "")
    if not match:
        return None

    try:
        return datetime.strptime("".join(match.groups()), "%Y%m%d%H%M%S")
    except ValueError:
        return None


def batch_sort_key(record):
    fields = record.get("fields", {})
    batch_name = fields.get("Batch name", "")
    parsed = parse_batch_datetime(batch_name)
    if parsed:
        return parsed.isoformat()
    return fields.get("Date imported") or record.get("createdTime") or ""


def latest_batch_record():
    records = list_records_with_fallback(
        AIRTABLE_BATCH_TABLE_NAME,
        max_records=100,
        sort=[("Batch name", "desc")],
    )
    batch_records = [
        record for record in records
        if (record.get("fields", {}).get("Batch name") or "").startswith("batch-")
    ]
    candidates = batch_records or records
    if not candidates:
        return None
    return max(candidates, key=batch_sort_key)


def linked_record_ids(fields, name_prefix):
    ids = []
    for field_name, value in fields.items():
        if not field_name.lower().startswith(name_prefix.lower()):
            continue
        if not isinstance(value, list):
            continue
        for record_id in value:
            if record_id not in ids:
                ids.append(record_id)
    return ids


def check_item_record(record, expected_batch_id):
    fields = record.get("fields", {})
    issues = []

    required_fields = [
        "Title page image",
        "GCS bucket",
        "GCS object path",
        "Original filename",
        "OCR raw text",
        "Extraction JSON",
        "Related Batch",
    ]

    for field_name in required_fields:
        if not is_non_empty(fields.get(field_name)):
            issues.append(f"missing {field_name}")

    if fields.get("Extraction status") != "Done":
        issues.append(f"Extraction status is {fields.get('Extraction status')!r}")

    if fields.get("Processing status") != "Extracted":
        issues.append(f"Processing status is {fields.get('Processing status')!r}")

    gcs_path = fields.get("GCS object path", "")
    if gcs_path and not (
        gcs_path.startswith("processed/")
        or "/processed/" in gcs_path
    ):
        issues.append(f"GCS object path does not look processed: {gcs_path}")

    related_batches = fields.get("Related Batch", [])
    if expected_batch_id and expected_batch_id not in related_batches:
        issues.append("Related Batch does not include latest batch record")

    return issues


def field_usage(records, table):
    usage = {}
    for field_name in field_names(table):
        count = 0
        for record in records:
            if is_non_empty(record.get("fields", {}).get(field_name)):
                count += 1
        usage[field_name] = count
    return usage


def format_usage(usage, field_name, sample_size):
    count = usage.get(field_name, 0)
    return f"{count}/{sample_size} sampled records populated"


def cleanup_recommendations(schema, sampled_records):
    recommendations = []

    def add(table_name, field_name, action, reason):
        table = table_by_name(schema, table_name)
        if not table or field_name not in field_names(table):
            return
        usage = field_usage(sampled_records.get(table_name, []), table)
        sample_size = len(sampled_records.get(table_name, []))
        recommendations.append({
            "table": table_name,
            "field": field_name,
            "type": field_type(table, field_name),
            "action": action,
            "usage": format_usage(usage, field_name, sample_size),
            "reason": reason,
        })

    add("Items", "Extraction queued", "Delete candidate", "Old Airtable-trigger queue flag; current pipeline starts outside Airtable.")
    add("Items", "Rotated title page image", "Delete candidate", "Legacy attachment field; orientation is handled inside the extractor response/debug path.")
    add("Items", "GCS object path-old", "Hide, then delete", "Formula/computed legacy path; current importer writes to GCS object path.")
    add("Items", "Batch ID", "Review before deleting", "Formula convenience field. Keep if views depend on it; pipeline uses Related Batch instead.")
    add("Items", "Image processing notes", "Review", "May still be useful for human QA, but current import routine does not write it.")

    add("Batches", "Items", "Delete candidate", "Legacy plain text field; linked records should be the source of truth.")
    add("Batches", "Batch Uploads", "Delete candidate", "Old Airtable-upload workflow link.")
    add("Batches", "Batch Uploads 2", "Delete candidate", "Duplicate old Airtable-upload workflow link.")
    add("Batches", "Items 2", "Keep, consider rename", "Likely reciprocal link for Items -> Related Batch. Rename to Items after old Items field is removed.")

    add("Batch Uploads", "Images", "Retire with table", "Old Airtable attachment upload workflow; current uploads should go to GCS.")
    add("Batch Uploads", "Processed", "Retire with table", "Old Airtable attachment workflow status.")
    add("Batch Uploads", "Processed count", "Retire with table", "Old Airtable attachment workflow count.")
    add("Batch Uploads", "Processed timestamp", "Retire with table", "Old Airtable attachment workflow timestamp.")

    add("Locations", "Items", "Consolidate", "Likely one of two reciprocal item-location links.")
    add("Locations", "Items 2", "Consolidate", "Likely duplicate reciprocal link from newer location field.")

    return recommendations


def print_latest_import_report(schema):
    print("== Latest Import Check ==")
    latest_batch = latest_batch_record()
    if not latest_batch:
        print("No batch records found.")
        return

    batch_fields = latest_batch.get("fields", {})
    batch_id = latest_batch["id"]
    batch_name = batch_fields.get("Batch name", "")
    item_ids = linked_record_ids(batch_fields, "Items")

    print(f"Latest batch: {batch_name} ({batch_id})")
    print(f"Date imported: {batch_fields.get('Date imported', '')}")
    print(f"Linked item records: {len(item_ids)}")

    if "Item count" in batch_fields:
        print(f"Airtable Item count: {batch_fields.get('Item count')}")

    notes = batch_fields.get("Batch Notes", "")
    if notes:
        print("Latest Batch Notes entry:")
        print(notes.split("\n\n")[-1])

    checked_items = []
    for item_id in item_ids[:25]:
        checked_items.append(get_record(AIRTABLE_TABLE_NAME, item_id))

    issue_count = 0
    for record in checked_items:
        issues = check_item_record(record, batch_id)
        if issues:
            issue_count += 1
            fields = record.get("fields", {})
            print(
                f"Item warning: {fields.get('Original filename', record['id'])}: "
                + "; ".join(issues)
            )

    if checked_items and issue_count == 0:
        print(f"Checked {len(checked_items)} linked item record(s): no obvious issues.")
    elif item_ids and len(item_ids) > len(checked_items):
        print(f"Checked first {len(checked_items)} of {len(item_ids)} linked item record(s).")

    if table_by_name(schema, AIRTABLE_FAILURE_TABLE_NAME):
        failures = list_records_with_fallback(
            AIRTABLE_FAILURE_TABLE_NAME,
            max_records=50,
            sort=[("Import Failure ID", "desc")],
        )
        unresolved = [
            record for record in failures
            if not record.get("fields", {}).get("Resolved?")
        ]
        print(f"Recent failure records checked: {len(failures)}")
        print(f"Recent unresolved failures: {len(unresolved)}")
        for record in unresolved[:5]:
            fields = record.get("fields", {})
            print(
                "Unresolved failure: "
                f"{fields.get('Original filename', record['id'])} "
                f"retry_count={fields.get('Retry count', '')} "
                f"path={fields.get('GCS object path', '')}"
            )


def print_schema_cleanup_report(schema, sample_records):
    print("\n== Schema Cleanup Suggestions ==")
    print("These are recommendations only. Hide fields first, then delete after views/scripts are checked.")

    for recommendation in cleanup_recommendations(schema, sample_records):
        print(
            f"- {recommendation['action']}: "
            f"{recommendation['table']} -> {recommendation['field']} "
            f"({recommendation['type']}; {recommendation['usage']})"
        )
        print(f"  Reason: {recommendation['reason']}")

    print("\nKeep for current pipeline:")
    print("- Items -> Title page image, GCS bucket, GCS object path, Original filename")
    print("- Items -> OCR/extraction fields, Extraction status, Processing status, Related Batch")
    print("- Batches -> Batch name, Date imported, Batch Notes, Item count, Items 2")
    print("- Import Failures -> all current fields")


def print_table_overview(schema, sample_records):
    print("\n== Table Overview ==")
    for table in schema:
        records = sample_records.get(table["name"], [])
        print(f"- {table['name']}: {len(table.get('fields', []))} fields, sampled {len(records)} records")


def parse_args():
    parser = argparse.ArgumentParser(description="Read-only Airtable import and schema audit.")
    parser.add_argument(
        "--sample-records",
        type=int,
        default=DEFAULT_SAMPLE_RECORDS,
        help="Records to sample per table for field usage counts.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    require_config()

    schema = fetch_schema()
    sample_records = {}

    for table in schema:
        table_name = table["name"]
        sample_records[table_name] = list_records_with_fallback(
            table_name,
            max_records=args.sample_records,
        )

    print(f"Airtable base: {AIRTABLE_BASE_ID}")
    print_table_overview(schema, sample_records)
    print_latest_import_report(schema)
    print_schema_cleanup_report(schema, sample_records)


if __name__ == "__main__":
    main()
