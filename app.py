from PIL import Image, ExifTags
import csv
import io
import os
import json
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import requests
from google.api_core.exceptions import NotFound, PreconditionFailed
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from google.cloud import storage
from google.cloud import vision
from google.oauth2 import service_account

from openai import OpenAI

from operator_ui import OPERATOR_UI_HTML


API_KEY = os.getenv("API_KEY", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

MAX_OCR_CHARS_FOR_LLM = int(os.getenv("MAX_OCR_CHARS_FOR_LLM", "12000"))
DEFAULT_GCS_BUCKET = os.getenv("BATCH_GCS_BUCKET", "rb-title-pages-2026")
BATCH_UPLOAD_ROOT_PREFIX = os.getenv("BATCH_UPLOAD_ROOT_PREFIX", "imports/")
SIGNED_UPLOAD_EXPIRATION_MINUTES = int(os.getenv("SIGNED_UPLOAD_EXPIRATION_MINUTES", "60"))
BATCH_RUN_LOCK_TTL_SECONDS = int(os.getenv("BATCH_RUN_LOCK_TTL_SECONDS", "21600"))
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Items")
AIRTABLE_BATCH_TABLE_NAME = os.getenv("AIRTABLE_BATCH_TABLE_NAME", "Batches")
AIRTABLE_BATCH_NAME_FIELD = os.getenv("AIRTABLE_BATCH_NAME_FIELD", "Batch name")
AIRTABLE_ITEM_BATCH_LINK_FIELD = os.getenv("AIRTABLE_ITEM_BATCH_LINK_FIELD", "Related Batch")
AIRTABLE_COLLECTIONS_TABLE_NAME = os.getenv("AIRTABLE_COLLECTIONS_TABLE_NAME", "Collections")
AIRTABLE_COLLECTION_NAME_FIELD = os.getenv("AIRTABLE_COLLECTION_NAME_FIELD", "Collection name")
AIRTABLE_ITEM_COLLECTION_LINK_FIELD = os.getenv("AIRTABLE_ITEM_COLLECTION_LINK_FIELD", "Collection (linked)")
AIRTABLE_LEGACY_COLLECTION_FIELD = os.getenv("AIRTABLE_LEGACY_COLLECTION_FIELD", "Collection")
AIRTABLE_LOCATIONS_TABLE_NAME = os.getenv("AIRTABLE_LOCATIONS_TABLE_NAME", "Locations")
AIRTABLE_LOCATION_NAME_FIELD = os.getenv("AIRTABLE_LOCATION_NAME_FIELD", "Location Code")
AIRTABLE_ITEM_LOCATION_LINK_FIELD = os.getenv("AIRTABLE_ITEM_LOCATION_LINK_FIELD", "Location")
APP_VERSION = "1.13.13-report-and-timeout-tuning"

app = FastAPI(title="RB Extractor", version=APP_VERSION)

RUNNING_BATCH_PROCESSES = {}
RUNNING_BATCH_PROCESS_LOCK = threading.Lock()


class ExtractRequest(BaseModel):
    gcs_bucket: str
    gcs_object_path: str


class CreateBatchRequest(BaseModel):
    batch_id: str | None = None
    source: str | None = None
    target_collection: str | None = None
    location: str | None = None
    notes: str | None = None


class CreateUploadUrlRequest(BaseModel):
    filename: str
    content_type: str = "application/octet-stream"
    overwrite: bool = False


class RunBatchRequest(BaseModel):
    force: bool = False


class RetryFailuresRequest(BaseModel):
    max_files: int = 25


class CreateLookupOptionRequest(BaseModel):
    name: str


def require_bearer_auth(req: Request) -> None:
    if not API_KEY:
        raise HTTPException(status_code=500, detail="API_KEY env var not set")

    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = auth.split(" ", 1)[1].strip()
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid token")


def require_airtable_config():
    if not AIRTABLE_API_KEY:
        raise HTTPException(status_code=500, detail="AIRTABLE_API_KEY env var not set")
    if not AIRTABLE_BASE_ID:
        raise HTTPException(status_code=500, detail="AIRTABLE_BASE_ID env var not set")


def airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }


def airtable_url(table_name, record_id=None):
    from urllib.parse import quote

    base = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote(table_name)}"
    if record_id:
        return f"{base}/{record_id}"
    return base


def airtable_meta_url():
    return f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"


def airtable_formula_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def airtable_lookup_config(kind):
    configs = {
        "collections": {
            "table": AIRTABLE_COLLECTIONS_TABLE_NAME,
            "field": AIRTABLE_COLLECTION_NAME_FIELD,
        },
        "locations": {
            "table": AIRTABLE_LOCATIONS_TABLE_NAME,
            "field": AIRTABLE_LOCATION_NAME_FIELD,
        },
    }
    if kind not in configs:
        raise HTTPException(status_code=404, detail="Unknown lookup type")
    return configs[kind]


def airtable_lookup_display_name(fields, preferred_field):
    preferred = fields.get(preferred_field)
    if preferred not in [None, ""]:
        return str(preferred).strip(), preferred_field

    for field_name, value in fields.items():
        if value in [None, ""] or isinstance(value, (dict, list)):
            continue
        return str(value).strip(), field_name

    return "", ""


def add_unique_name(names, value):
    value = (value or "").strip()
    if not value:
        return
    key = value.lower()
    if key not in {name.lower() for name in names}:
        names.append(value)


def list_airtable_field_values(table_name, field_name, limit=1000):
    values = []
    try:
        records = list_airtable_records(table_name, limit=limit)
    except Exception as error:
        return {
            "options": [],
            "warnings": [f"Could not read values from {table_name}.{field_name}: {error}"],
        }

    for record in records:
        value = record.get("fields", {}).get(field_name)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    add_unique_name(values, item)
        elif value not in [None, ""]:
            add_unique_name(values, str(value))

    values.sort(key=str.lower)
    return {"options": values, "warnings": []}


def list_airtable_select_field_options(table_name, field_name):
    require_airtable_config()
    warnings = []

    try:
        response = requests.get(airtable_meta_url(), headers=airtable_headers(), timeout=60)
    except Exception as error:
        fallback = list_airtable_field_values(table_name, field_name)
        return {
            "options": fallback["options"],
            "warnings": [f"Could not read Airtable schema options: {error}", *fallback["warnings"]],
        }

    if response.status_code == 403:
        fallback = list_airtable_field_values(table_name, field_name)
        return {
            "options": fallback["options"],
            "warnings": [
                "Airtable token lacks schema.bases:read; using populated legacy collection values instead.",
                *fallback["warnings"],
            ],
        }

    if not response.ok:
        fallback = list_airtable_field_values(table_name, field_name)
        return {
            "options": fallback["options"],
            "warnings": [f"Airtable schema read failed: {response.text}", *fallback["warnings"]],
        }

    data = response.json()
    for table in data.get("tables", []):
        if table.get("name") != table_name:
            continue
        for field in table.get("fields", []):
            if field.get("name") != field_name:
                continue

            values = []
            for choice in field.get("options", {}).get("choices", []):
                add_unique_name(values, choice.get("name"))

            values.sort(key=str.lower)
            return {"options": values, "warnings": warnings}

    fallback = list_airtable_field_values(table_name, field_name)
    return {
        "options": fallback["options"],
        "warnings": [
            f"Could not find legacy field {table_name}.{field_name}; using populated values if available.",
            *fallback["warnings"],
        ],
    }


def merge_airtable_lookup_options(result, names, source):
    existing = {option["name"].lower() for option in result["options"]}
    added = 0

    for name in names:
        name = (name or "").strip()
        key = name.lower()
        if not name or key in existing:
            continue
        result["options"].append({"id": "", "name": name, "source": source})
        existing.add(key)
        added += 1

    result["options"].sort(key=lambda option: option["name"].lower())
    return added


def list_airtable_lookup_options(kind, limit=1000):
    require_airtable_config()
    config = airtable_lookup_config(kind)
    options = []
    seen = set()
    used_fields = set()
    records_seen = 0
    offset = None

    while len(options) < limit:
        params = {"pageSize": min(100, limit - len(options))}
        if offset:
            params["offset"] = offset

        response = requests.get(
            airtable_url(config["table"]),
            headers=airtable_headers(),
            params=params,
            timeout=60,
        )

        if not response.ok:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Airtable lookup read failed: {response.text}",
            )

        data = response.json()
        for record in data.get("records", []):
            records_seen += 1
            name, field_name = airtable_lookup_display_name(record.get("fields", {}), config["field"])
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            used_fields.add(field_name)
            options.append({"id": record["id"], "name": name})

        offset = data.get("offset")
        if not offset:
            break

    options.sort(key=lambda option: option["name"].lower())
    warnings = []
    if records_seen and config["field"] not in used_fields:
        warnings.append(
            f"Configured field '{config['field']}' was not populated; using visible Airtable values instead."
        )

    return {
        "options": options,
        "records_seen": records_seen,
        "field": config["field"],
        "fields_used": sorted(used_fields),
        "warnings": warnings,
    }


def get_or_create_airtable_lookup_option(kind, name):
    require_airtable_config()
    config = airtable_lookup_config(kind)
    name = (name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")

    response = requests.get(
        airtable_url(config["table"]),
        headers=airtable_headers(),
        params={
            "filterByFormula": f"{{{config['field']}}} = {airtable_formula_string(name)}",
            "maxRecords": 1,
        },
        timeout=60,
    )

    if not response.ok:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Airtable lookup read failed: {response.text}",
        )

    records = response.json().get("records", [])
    if records:
        return {
            "id": records[0]["id"],
            "name": records[0].get("fields", {}).get(config["field"], name),
            "created": False,
        }

    response = requests.post(
        airtable_url(config["table"]),
        headers=airtable_headers(),
        json={"fields": {config["field"]: name}},
        timeout=60,
    )

    if not response.ok:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Airtable lookup create failed: {response.text}",
        )

    record = response.json()
    return {
        "id": record["id"],
        "name": record.get("fields", {}).get(config["field"], name),
        "created": True,
    }


def list_airtable_records(table_name, params=None, limit=1000):
    require_airtable_config()
    records = []
    offset = None

    while len(records) < limit:
        page_params = dict(params or {})
        page_params["pageSize"] = min(100, limit - len(records))
        if offset:
            page_params["offset"] = offset

        response = requests.get(
            airtable_url(table_name),
            headers=airtable_headers(),
            params=page_params,
            timeout=60,
        )

        if not response.ok:
            raise RuntimeError(f"Airtable read failed: {response.status_code}: {response.text}")

        data = response.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    return records


def get_airtable_schema_tables():
    require_airtable_config()
    response = requests.get(airtable_meta_url(), headers=airtable_headers(), timeout=60)
    if response.status_code == 403:
        raise RuntimeError("Airtable token lacks schema.bases:read.")
    if not response.ok:
        raise RuntimeError(f"Airtable schema read failed: {response.status_code}: {response.text}")
    return response.json().get("tables", [])


def airtable_table_by_name(tables, table_name):
    for table in tables:
        if table.get("name") == table_name:
            return table
    return None


def resolve_airtable_item_link_field(tables, configured_field, linked_table_name, fallback_names):
    if not tables or not configured_field:
        return configured_field

    items_table = airtable_table_by_name(tables, AIRTABLE_TABLE_NAME)
    linked_table = airtable_table_by_name(tables, linked_table_name)
    if not items_table or not linked_table:
        return configured_field

    linked_table_id = linked_table.get("id")
    link_fields = []
    for field in items_table.get("fields", []):
        if field.get("type") != "multipleRecordLinks":
            continue
        if field.get("options", {}).get("linkedTableId") == linked_table_id:
            link_fields.append(field.get("name", ""))

    if configured_field in link_fields:
        return configured_field

    for fallback_name in fallback_names:
        if fallback_name in link_fields:
            return fallback_name

    if len(link_fields) == 1:
        return link_fields[0]

    return configured_field


def get_airtable_batch_record(batch_id):
    records = list_airtable_records(
        AIRTABLE_BATCH_TABLE_NAME,
        params={
            "filterByFormula": f"{{{AIRTABLE_BATCH_NAME_FIELD}}} = {airtable_formula_string(batch_id)}",
            "maxRecords": 1,
        },
        limit=1,
    )
    return records[0] if records else None


def airtable_item_records_for_batch(batch_record):
    if not batch_record:
        return []

    batch_name = (batch_record.get("fields", {}).get(AIRTABLE_BATCH_NAME_FIELD) or "").strip()
    if not batch_name:
        return []

    formula = (
        f"FIND({airtable_formula_string(batch_name)}, "
        f"ARRAYJOIN({{{AIRTABLE_ITEM_BATCH_LINK_FIELD}}})) > 0"
    )

    records = list_airtable_records(
        AIRTABLE_TABLE_NAME,
        params={
            "filterByFormula": formula,
            "sort[0][field]": "Created Date",
            "sort[0][direction]": "desc",
        },
        limit=5000,
    )

    batch_record_id = batch_record.get("id")
    return [
        record for record in records
        if batch_record_id in record.get("fields", {}).get(AIRTABLE_ITEM_BATCH_LINK_FIELD, [])
    ]


def batch_side_linked_item_summary(batch_record):
    if not batch_record:
        return {"field": "", "count": None}

    candidates = []
    for field_name, value in batch_record.get("fields", {}).items():
        if not field_name.lower().startswith("items"):
            continue
        if isinstance(value, list):
            candidates.append((field_name, len(value)))

    if not candidates:
        return {"field": "", "count": None}

    field_name, count = max(candidates, key=lambda item: item[1])
    return {"field": field_name, "count": count}


def count_items_missing_field(records, field_name):
    if not field_name:
        return 0
    return sum(1 for record in records if not record.get("fields", {}).get(field_name))


def get_google_credentials():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return credentials, creds_dict.get("project_id")


def get_storage_client():
    credentials, project_id = get_google_credentials()
    return storage.Client(credentials=credentials, project=project_id)


def normalize_prefix(prefix):
    prefix = (prefix or "").strip()
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    return prefix


def validate_batch_id(batch_id):
    if not re.match(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,80}$", batch_id or ""):
        raise HTTPException(
            status_code=400,
            detail="batch_id may contain only letters, numbers, dots, underscores, and hyphens",
        )
    return batch_id


def make_batch_id():
    return datetime.now(UTC).strftime("batch-%Y%m%dT%H%M%SZ")


def batch_root_prefix(batch_id):
    return f"{normalize_prefix(BATCH_UPLOAD_ROOT_PREFIX)}{batch_id}/"


def batch_input_prefix(batch_id):
    return f"{batch_root_prefix(batch_id)}to_process/"


def batch_results_path(batch_id):
    return f"results/batches/{batch_id}.csv"


def batch_run_status_path(batch_id):
    return f"{batch_root_prefix(batch_id)}run_status.json"


def batch_run_log_path(batch_id):
    return f"{batch_root_prefix(batch_id)}run.log"


def batch_run_lock_path(batch_id):
    return f"{batch_root_prefix(batch_id)}run.lock.json"


def batch_run_stop_path(batch_id):
    return f"{batch_root_prefix(batch_id)}run_stop.json"


def batch_run_command(batch_id):
    input_prefix = batch_input_prefix(batch_id)
    results_path = batch_results_path(batch_id)
    return (
        f"IMPORT_BATCH_ID={batch_id} "
        f"BATCH_INPUT_PREFIX={input_prefix} "
        f"BATCH_RESULTS_PATH={results_path} "
        "python3 run_import_pipeline.py"
    )


def safe_filename(filename):
    filename = os.path.basename((filename or "").strip())
    filename = re.sub(r"[^A-Za-z0-9._ -]+", "_", filename)
    filename = filename.strip(" .")
    if not filename:
        raise HTTPException(status_code=400, detail="filename is required")
    return filename


def get_bucket(bucket_name=DEFAULT_GCS_BUCKET):
    return get_storage_client().bucket(bucket_name)


def download_gcs_bytes(bucket_name, object_path):
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(object_path)

    if not blob.exists():
        raise HTTPException(404, "GCS object not found")

    return blob.download_as_bytes()


def fix_image_orientation(image_bytes):
    try:
        image = Image.open(io.BytesIO(image_bytes))
        orientation_tag = None

        for tag, name in ExifTags.TAGS.items():
            if name == "Orientation":
                orientation_tag = tag
                break

        orientation_action = "none"
        exif = image._getexif()

        if exif and orientation_tag:
            val = exif.get(orientation_tag)
            if val == 3:
                image = image.rotate(180, expand=True)
                orientation_action = "rotated_180"
            elif val == 6:
                image = image.rotate(270, expand=True)
                orientation_action = "rotated_270"
            elif val == 8:
                image = image.rotate(90, expand=True)
                orientation_action = "rotated_90"

        output = io.BytesIO()
        fmt = image.format if image.format in ["JPEG", "PNG"] else "JPEG"

        if fmt == "JPEG" and image.mode in ("RGBA", "P", "CMYK"):
            image = image.convert("RGB")

        image.save(output, format=fmt)
        return output.getvalue(), orientation_action

    except Exception as e:
        return image_bytes, f"orientation_failed:{e}"


def get_vision_client():
    credentials, _ = get_google_credentials()
    return vision.ImageAnnotatorClient(credentials=credentials)


def run_ocr_document_text(image_bytes):
    client = get_vision_client()
    img = vision.Image(content=image_bytes)
    resp = client.document_text_detection(image=img)

    text = (resp.full_text_annotation.text or "").strip()

    confidences = []
    try:
        for page in resp.full_text_annotation.pages:
            for block in page.blocks:
                for para in block.paragraphs:
                    for word in para.words:
                        confidences.append(word.confidence)
    except:
        pass

    if not text:
        conf = 0.0
    elif not confidences:
        conf = min(0.75, len(text) / 1000)
    else:
        base = sum(confidences) / len(confidences)
        length_factor = min(1.0, len(text) / 500)
        conf = min(0.99, base * (0.5 + 0.5 * length_factor))

    return text, float(conf)


def clamp01(x):
    try:
        return max(0.0, min(1.0, float(x)))
    except:
        return 0.0


def parse_publication_year(value):
    if value in [None, ""]:
        return None

    try:
        return int(float(value))
    except:
        match = re.search(r"\b(1[4-9]\d{2}|20[0-2]\d)\b", str(value))
        if match:
            return int(match.group(1))

    return None


def normalize_parsed_output(parsed):
    parsed["llm_confidence"] = clamp01(parsed.get("llm_confidence"))
    if parsed["llm_confidence"] >= 1:
        parsed["llm_confidence"] = 0.95
    parsed["publication_year"] = parse_publication_year(parsed.get("publication_year"))
    return parsed


def build_quality_flags(ocr_text, ocr_conf, parsed):
    year = parsed.get("publication_year")

    return {
        "missing_title": not parsed.get("title"),
        "missing_author": not parsed.get("author"),
        "missing_year": year is None,
        "missing_imprint": not parsed.get("publication_statement_verbatim"),
        "short_ocr": len(ocr_text) < 80,
        "low_ocr_confidence": ocr_conf < 0.7,
        "suspect_year": bool(year and (year < 1400 or year > 2026)),
    }


def json_string_or_blank(value):
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def build_extraction_messages(ocr_text):
    return [
        {
            "role": "system",
            "content": (
                "Extract rare-book title page metadata from OCR text. "
                "Return only a JSON object with keys: llm_confidence, language, "
                "title, author, publication_place, publisher, publication_year, "
                "edition_statement, publication_statement_verbatim, translator, "
                "illustration_note, extraction_evidence_json."
            ),
        },
        {
            "role": "user",
            "content": f"OCR text:\n{ocr_text[:MAX_OCR_CHARS_FOR_LLM]}",
        },
    ]


def batch_manifest(batch_id, body):
    input_prefix = batch_input_prefix(batch_id)
    results_path = batch_results_path(batch_id)

    return {
        "batch_id": batch_id,
        "bucket": DEFAULT_GCS_BUCKET,
        "input_prefix": input_prefix,
        "results_path": results_path,
        "source": body.source or "",
        "target_collection": body.target_collection or "",
        "location": body.location or "",
        "notes": body.notes or "",
        "created_at": datetime.now(UTC).isoformat(),
        "run_command": batch_run_command(batch_id),
    }


def write_batch_manifest(manifest):
    bucket = get_bucket(DEFAULT_GCS_BUCKET)
    manifest_path = f"{batch_root_prefix(manifest['batch_id'])}batch.json"
    bucket.blob(manifest_path).upload_from_string(
        json.dumps(manifest, indent=2),
        content_type="application/json",
    )
    return manifest_path


def read_batch_manifest(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    blob = bucket.blob(f"{batch_root_prefix(batch_id)}batch.json")

    if not blob.exists():
        return {}

    try:
        return json.loads(blob.download_as_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def count_batch_uploads(batch_id, bucket=None):
    return len(list_batch_input_blob_names(batch_id, bucket))


def list_batch_input_blob_names(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    prefix = batch_input_prefix(batch_id)
    names = []

    for blob in bucket.client.list_blobs(DEFAULT_GCS_BUCKET, prefix=prefix):
        if not blob.name.endswith("/"):
            names.append(blob.name)

    return names


def batch_results_counts(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    blob = bucket.blob(batch_results_path(batch_id))

    if not blob.exists():
        return {
            "exists": False,
            "total": 0,
            "success": 0,
            "failed": 0,
        }

    rows = csv.DictReader(blob.download_as_text(encoding="utf-8").splitlines())
    counts = {
        "exists": True,
        "total": 0,
        "success": 0,
        "failed": 0,
    }

    for row in rows:
        counts["total"] += 1
        status = (row.get("status") or "").strip().lower()
        if status == "success":
            counts["success"] += 1
        elif status == "failed":
            counts["failed"] += 1

    return counts


def download_batch_results_rows(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    blob = bucket.blob(batch_results_path(batch_id))

    if not blob.exists():
        return []

    content = blob.download_as_text(encoding="utf-8")
    return list(csv.DictReader(content.splitlines()))


def successful_source_gcs_paths(rows):
    paths = set()

    for row in rows:
        status = (row.get("status") or "").strip().lower()
        source_path = (row.get("source_gcs_path") or "").strip()
        if status == "success" and source_path:
            paths.add(source_path)

    return paths


def batch_waiting_input_summary(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    input_paths = list_batch_input_blob_names(batch_id, bucket)
    successful_sources = successful_source_gcs_paths(
        download_batch_results_rows(batch_id, bucket)
    )
    waiting = []
    already_successful = []

    for path in input_paths:
        if path in successful_sources:
            already_successful.append(path)
        else:
            waiting.append(path)

    return {
        "input_count": len(input_paths),
        "waiting": waiting,
        "already_successful": already_successful,
    }


def sanitize_error_message(value, limit=700):
    value = (value or "").strip()
    value = re.sub(
        r"https://storage\.googleapis\.com/([^?\s'\"]+)\?[^'\"]+",
        r"gs://\1",
        value,
    )
    value = re.sub(r"X-Goog-[A-Za-z0-9_-]+=[^&\s'\"]+", "X-Goog-...=redacted", value)

    if len(value) > limit:
        return f"{value[:limit].rstrip()}..."

    return value


def failure_key_for_row(row):
    return (
        row.get("source_gcs_path")
        or row.get("Original filename")
        or row.get("final_gcs_path")
        or row.get("GCS object path")
        or ""
    ).strip()


def row_original_filename(row):
    return (
        row.get("Original filename")
        or (row.get("source_gcs_path") or "").split("/")[-1]
        or (row.get("final_gcs_path") or "").split("/")[-1]
        or ""
    ).strip()


def batch_failure_rows(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    rows = download_batch_results_rows(batch_id, bucket)
    states = {}

    for index, row in enumerate(rows):
        key = failure_key_for_row(row)
        if not key:
            continue

        status = (row.get("status") or "").strip().lower()
        if status == "success":
            states[key] = {"status": "success", "row": row, "index": index}
        elif status == "failed":
            states[key] = {"status": "failed", "row": row, "index": index}

    failures = []
    for key, state in states.items():
        if state["status"] != "failed":
            continue

        row = state["row"]
        filename = row_original_filename(row)
        final_path = (row.get("final_gcs_path") or row.get("GCS object path") or "").strip()
        source_path = (row.get("source_gcs_path") or "").strip()
        retry_path = f"{batch_input_prefix(batch_id)}{filename}" if filename else ""

        failures.append(
            {
                "key": key,
                "filename": filename,
                "source_gcs_path": source_path,
                "final_gcs_path": final_path,
                "error": sanitize_error_message(row.get("error")),
                "attempts": row.get("extraction_attempts") or "",
                "processed_at": row.get("processed_at") or "",
                "retry_queued": bool(retry_path and bucket.blob(retry_path).exists()),
                "retry_path": retry_path,
            }
        )

    failures.sort(key=lambda item: item.get("processed_at") or "")
    return failures


def verification_check(label, status, detail):
    return {
        "label": label,
        "status": status,
        "detail": detail,
    }


def verification_overall_status(checks):
    statuses = {check["status"] for check in checks}
    if "error" in statuses:
        return "error"
    if "warn" in statuses:
        return "warn"
    return "ok"


def build_batch_verification_checks(
    run_status,
    results,
    remaining_input_count,
    unresolved_failure_count,
    airtable,
    manifest,
    already_successful_input_count=0,
):
    checks = []
    run_state = (run_status or {}).get("status", "not_started")
    results_exists = bool(results.get("exists"))
    success_rows = int(results.get("success") or 0)
    failed_rows = int(results.get("failed") or 0)
    total_rows = int(results.get("total") or 0)
    imported_count = airtable.get("item_side_linked_count")
    batch_side_count = airtable.get("batch_side_linked_count")

    if run_state == "running" and not results_exists:
        checks.append(
            verification_check(
                "Results CSV",
                "warn",
                "Batch is running; the results CSV may not exist until files have been processed.",
            )
        )
    elif run_state == "succeeded" and not results_exists:
        checks.append(verification_check("Results CSV", "error", "Run succeeded but no results CSV was found."))
    elif results_exists:
        checks.append(verification_check("Results CSV", "ok", f"{total_rows} result row(s) found."))
    else:
        checks.append(verification_check("Results CSV", "warn", "No results CSV yet."))

    if total_rows != success_rows + failed_rows:
        checks.append(verification_check("CSV statuses", "warn", "Some result rows have an unknown status."))
    elif total_rows:
        checks.append(verification_check("CSV statuses", "ok", "Every result row is success or failed."))

    if imported_count is None:
        if run_state == "running":
            checks.append(
                verification_check(
                    "Airtable Items",
                    "warn",
                    "Batch is running; Airtable items are checked after the import step runs.",
                )
            )
        else:
            checks.append(verification_check("Airtable Items", "warn", "Could not verify Airtable linked items."))
    elif imported_count != success_rows:
        checks.append(
            verification_check(
                "Airtable Items",
                "error",
                f"{success_rows} successful CSV row(s), but {imported_count} linked Airtable item(s).",
            )
        )
    else:
        checks.append(verification_check("Airtable Items", "ok", f"{imported_count} linked item(s)."))

    if batch_side_count is not None and imported_count is not None:
        if batch_side_count != imported_count:
            checks.append(
                verification_check(
                    "Batch reciprocal link",
                    "error",
                    f"Batch side shows {batch_side_count} item(s), item side shows {imported_count}.",
                )
            )
        else:
            checks.append(verification_check("Batch reciprocal link", "ok", f"{batch_side_count} item(s)."))
    elif imported_count is not None:
        checks.append(
            verification_check(
                "Batch reciprocal link",
                "warn",
                "Could not find a reciprocal linked item field on the Airtable batch record.",
            )
        )

    if run_state == "running" and remaining_input_count:
        checks.append(
            verification_check(
                "Waiting files",
                "warn",
                (
                    f"{remaining_input_count} file(s) are still queued or processing. "
                    "Refresh when the run completes."
                ),
            )
        )
    elif run_state == "running":
        checks.append(
            verification_check(
                "Waiting files",
                "warn",
                "Batch is running; refresh when the run completes.",
            )
        )
    elif run_state == "succeeded" and remaining_input_count:
        checks.append(
            verification_check(
                "Waiting files",
                "warn",
                (
                    f"{remaining_input_count} file(s) still waiting in to_process. "
                    "This usually means the run reached its per-run file limit; run the batch again to continue."
                ),
            )
        )
    elif run_state == "stopped" and remaining_input_count:
        checks.append(
            verification_check(
                "Waiting files",
                "warn",
                f"Batch was stopped with {remaining_input_count} file(s) still waiting. Run the batch again to continue.",
            )
        )
    elif run_state == "succeeded" and already_successful_input_count:
        checks.append(
            verification_check(
                "Waiting files",
                "ok",
                (
                    "No unprocessed files waiting; ignored "
                    f"{already_successful_input_count} already-successful input file(s)."
                ),
            )
        )
    elif run_state == "succeeded":
        checks.append(verification_check("Waiting files", "ok", "No files left waiting in to_process."))

    if unresolved_failure_count:
        checks.append(
            verification_check(
                "Unresolved failures",
                "warn",
                f"{unresolved_failure_count} unresolved failed file(s).",
            )
        )
    else:
        checks.append(verification_check("Unresolved failures", "ok", "No unresolved failed files."))

    if manifest.get("target_collection") and airtable.get("items_missing_collection"):
        checks.append(
            verification_check(
                "Collection links",
                "error",
                f"{airtable['items_missing_collection']} item(s) missing the selected collection link.",
            )
        )
    elif manifest.get("target_collection") and imported_count:
        checks.append(verification_check("Collection links", "ok", "Imported items have collection links."))

    if manifest.get("location") and airtable.get("items_missing_location"):
        checks.append(
            verification_check(
                "Location links",
                "error",
                f"{airtable['items_missing_location']} item(s) missing the selected location link.",
            )
        )
    elif manifest.get("location") and imported_count:
        checks.append(verification_check("Location links", "ok", "Imported items have location links."))

    if airtable.get("warning") and run_state == "running" and "No matching Airtable batch record found" in airtable["warning"]:
        checks.append(
            verification_check(
                "Airtable verification",
                "warn",
                "Airtable batch record may not exist until the import step runs.",
            )
        )
    elif airtable.get("warning"):
        checks.append(verification_check("Airtable verification", "warn", airtable["warning"]))

    return checks


def batch_verification_payload(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    manifest = read_batch_manifest(batch_id, bucket)
    run_status = normalize_run_status_for_lock(
        batch_id,
        read_batch_run_status(batch_id, bucket),
        bucket,
    )
    results = batch_results_counts(batch_id, bucket)
    input_summary = batch_waiting_input_summary(batch_id, bucket)
    remaining_input_count = len(input_summary["waiting"])
    already_successful_input_count = len(input_summary["already_successful"])
    unresolved_failures = batch_failure_rows(batch_id, bucket)
    airtable = {
        "batch_record_id": "",
        "item_side_linked_count": None,
        "batch_side_linked_count": None,
        "batch_side_link_field": "",
        "collection_link_field": AIRTABLE_ITEM_COLLECTION_LINK_FIELD,
        "location_link_field": AIRTABLE_ITEM_LOCATION_LINK_FIELD,
        "items_missing_collection": 0,
        "items_missing_location": 0,
        "warning": "",
    }

    try:
        schema_tables = get_airtable_schema_tables()
        collection_link_field = resolve_airtable_item_link_field(
            schema_tables,
            AIRTABLE_ITEM_COLLECTION_LINK_FIELD,
            AIRTABLE_COLLECTIONS_TABLE_NAME,
            [
                "Collection (linked)",
                "Collection linked",
                "Collections",
                "Collection",
            ],
        )
        location_link_field = resolve_airtable_item_link_field(
            schema_tables,
            AIRTABLE_ITEM_LOCATION_LINK_FIELD,
            AIRTABLE_LOCATIONS_TABLE_NAME,
            [
                "Location",
                "Location (linked)",
                "Location linked",
                "Locations",
            ],
        )
        airtable["collection_link_field"] = collection_link_field
        airtable["location_link_field"] = location_link_field

        batch_record = get_airtable_batch_record(batch_id)
        if batch_record:
            item_records = airtable_item_records_for_batch(batch_record)
            batch_side = batch_side_linked_item_summary(batch_record)
            airtable.update({
                "batch_record_id": batch_record.get("id", ""),
                "item_side_linked_count": len(item_records),
                "batch_side_linked_count": batch_side["count"],
                "batch_side_link_field": batch_side["field"],
                "items_missing_collection": count_items_missing_field(
                    item_records,
                    collection_link_field,
                ),
                "items_missing_location": count_items_missing_field(
                    item_records,
                    location_link_field,
                ),
            })
        else:
            airtable["warning"] = "No matching Airtable batch record found."
    except Exception as error:
        airtable["warning"] = str(error)

    checks = build_batch_verification_checks(
        run_status,
        results,
        remaining_input_count,
        len(unresolved_failures),
        airtable,
        manifest,
        already_successful_input_count,
    )

    return {
        "batch_id": batch_id,
        "bucket": DEFAULT_GCS_BUCKET,
        "input_prefix": batch_input_prefix(batch_id),
        "results_path": batch_results_path(batch_id),
        "run_status": run_status,
        "manifest": manifest,
        "counts": {
            "remaining_input_files": remaining_input_count,
            "known_file_rows": results.get("total", 0) + remaining_input_count,
            "input_files_seen": input_summary["input_count"],
            "already_successful_input_files": already_successful_input_count,
            "csv_total_rows": results.get("total", 0),
            "csv_success_rows": results.get("success", 0),
            "csv_failed_rows": results.get("failed", 0),
            "unresolved_failure_rows": len(unresolved_failures),
            "airtable_item_records": airtable["item_side_linked_count"],
            "batch_side_item_records": airtable["batch_side_linked_count"],
        },
        "waiting_input_examples": input_summary["waiting"][:5],
        "already_successful_input_examples": input_summary["already_successful"][:5],
        "airtable": airtable,
        "checks": checks,
        "overall_status": verification_overall_status(checks),
    }


def failure_retry_source_candidates(failure):
    filename = (failure.get("filename") or "").strip()
    candidates = []

    def add(path):
        path = (path or "").strip()
        if path and path not in candidates:
            candidates.append(path)

    add(failure.get("final_gcs_path"))
    add(failure.get("source_gcs_path"))

    if filename:
        add(f"{normalize_prefix(os.getenv('BATCH_FAILED_PREFIX', 'failed/'))}{filename}")
        add(f"{normalize_prefix(os.getenv('BATCH_PROCESSED_PREFIX', 'processed/'))}{filename}")

    return candidates


def retry_batch_failures(batch_id, max_files, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    failures = batch_failure_rows(batch_id, bucket)
    max_files = max(1, min(int(max_files or 25), 100))
    summary = {
        "queued": 0,
        "already_queued": 0,
        "already_processed": 0,
        "missing_source": 0,
        "skipped": 0,
    }
    results = []
    input_prefix = batch_input_prefix(batch_id)

    for failure in failures[:max_files]:
        filename = failure.get("filename")
        destination_path = f"{input_prefix}{filename}" if filename else ""
        destination_blob = bucket.blob(destination_path) if destination_path else None
        source_blob = None

        if destination_blob and destination_blob.exists():
            status = "already_queued"
            summary[status] += 1
            results.append({**failure, "retry_status": status, "retry_path": destination_path})
            continue

        for candidate in failure_retry_source_candidates(failure):
            blob = bucket.blob(candidate)
            if blob.exists():
                source_blob = blob
                break

        if not source_blob:
            status = "missing_source"
            summary[status] += 1
            results.append({**failure, "retry_status": status, "retry_path": destination_path})
            continue

        if source_blob.name.startswith(normalize_prefix(os.getenv("BATCH_PROCESSED_PREFIX", "processed/"))):
            status = "already_processed"
            summary[status] += 1
            results.append({**failure, "retry_status": status, "retry_path": source_blob.name})
            continue

        if not destination_blob:
            status = "skipped"
            summary[status] += 1
            results.append({**failure, "retry_status": status, "retry_path": ""})
            continue

        bucket.copy_blob(source_blob, bucket, destination_path)
        source_blob.delete()
        status = "queued"
        summary[status] += 1
        results.append({**failure, "retry_status": status, "retry_path": destination_path})

    return {
        "batch_id": batch_id,
        "summary": summary,
        "results": results,
        "remaining_failures": max(0, len(failures) - len(results)),
    }


def batch_status_payload(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    manifest = read_batch_manifest(batch_id, bucket)
    uploaded_count = count_batch_uploads(batch_id, bucket)
    run_status = normalize_run_status_for_lock(
        batch_id,
        read_batch_run_status(batch_id, bucket),
        bucket,
    )

    return {
        "batch_id": batch_id,
        "bucket": manifest.get("bucket") or DEFAULT_GCS_BUCKET,
        "input_prefix": manifest.get("input_prefix") or batch_input_prefix(batch_id),
        "results_path": manifest.get("results_path") or batch_results_path(batch_id),
        "uploaded_count": uploaded_count,
        "results": batch_results_counts(batch_id, bucket),
        "run_command": manifest.get("run_command") or batch_run_command(batch_id),
        "source": manifest.get("source", ""),
        "target_collection": manifest.get("target_collection", ""),
        "location": manifest.get("location", ""),
        "notes": manifest.get("notes", ""),
        "created_at": manifest.get("created_at", ""),
        "run": run_status,
        "can_run": can_start_batch_run(run_status, uploaded_count),
    }


def list_operator_batch_ids(bucket):
    batch_ids = set()
    root_prefix = normalize_prefix(BATCH_UPLOAD_ROOT_PREFIX)

    iterator = bucket.client.list_blobs(
        DEFAULT_GCS_BUCKET,
        prefix=root_prefix,
        delimiter="/",
    )

    for page in iterator.pages:
        for prefix in page.prefixes:
            parts = prefix.strip("/").split("/")
            batch_id = parts[-1] if parts else ""
            if batch_id.startswith("batch-"):
                batch_ids.add(batch_id)

    return sorted(batch_ids, reverse=True)


def parse_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def read_batch_run_lock(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    blob = bucket.blob(batch_run_lock_path(batch_id))

    try:
        blob.reload()
    except NotFound:
        return None

    try:
        data = json.loads(blob.download_as_text(encoding="utf-8"))
    except json.JSONDecodeError:
        data = {}

    expires_at = parse_datetime(data.get("expires_at"))
    data["generation"] = blob.generation
    data["expired"] = bool(expires_at and expires_at <= datetime.now(UTC))
    return data


def acquire_batch_run_lock(batch_id, run_id, bucket=None, force=False):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    lock_blob = bucket.blob(batch_run_lock_path(batch_id))

    for _ in range(2):
        existing = read_batch_run_lock(batch_id, bucket)
        if existing:
            if not (force or existing.get("expired")):
                return None, existing

            try:
                lock_blob.delete(if_generation_match=int(existing["generation"]))
            except (NotFound, PreconditionFailed):
                continue

        now = datetime.now(UTC)
        payload = {
            "batch_id": batch_id,
            "run_id": run_id,
            "created_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=BATCH_RUN_LOCK_TTL_SECONDS)).isoformat(),
        }

        try:
            lock_blob.upload_from_string(
                json.dumps(payload, indent=2),
                content_type="application/json",
                if_generation_match=0,
            )
            lock_blob.reload()
            payload["generation"] = lock_blob.generation
            payload["expired"] = False
            return payload, None
        except PreconditionFailed:
            continue

    return None, read_batch_run_lock(batch_id, bucket)


def release_batch_run_lock(batch_id, run_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    existing = read_batch_run_lock(batch_id, bucket)

    if not existing or existing.get("run_id") != run_id:
        return

    try:
        bucket.blob(batch_run_lock_path(batch_id)).delete(
            if_generation_match=int(existing["generation"])
        )
    except (NotFound, PreconditionFailed):
        pass


def read_batch_stop_request(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    blob = bucket.blob(batch_run_stop_path(batch_id))

    if not blob.exists():
        return None

    try:
        return json.loads(blob.download_as_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_batch_stop_request(batch_id, run_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    payload = {
        "batch_id": batch_id,
        "run_id": run_id,
        "requested_at": datetime.now(UTC).isoformat(),
    }
    bucket.blob(batch_run_stop_path(batch_id)).upload_from_string(
        json.dumps(payload, indent=2),
        content_type="application/json",
    )
    return payload


def clear_batch_stop_request(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    try:
        bucket.blob(batch_run_stop_path(batch_id)).delete()
    except NotFound:
        pass


def stop_requested_for_run(batch_id, run_id, bucket=None):
    request = read_batch_stop_request(batch_id, bucket)
    if not request:
        return False
    return not request.get("run_id") or request.get("run_id") == run_id


def read_batch_run_status(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    blob = bucket.blob(batch_run_status_path(batch_id))

    if not blob.exists():
        return {
            "status": "not_started",
            "log_path": batch_run_log_path(batch_id),
            **initial_run_progress("not_started"),
        }

    try:
        return json.loads(blob.download_as_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "status": "unknown",
            "log_path": batch_run_log_path(batch_id),
            "error": "Run status file could not be parsed.",
            **initial_run_progress("unknown"),
        }


def normalize_run_status_for_lock(batch_id, run_status, bucket=None):
    if run_status.get("status") != "running":
        return run_status

    lock = read_batch_run_lock(batch_id, bucket)
    if not lock:
        return {
            **run_status,
            "status": "stale",
            "error": "No active run lock found. The previous run may have stopped.",
        }

    if lock.get("expired"):
        return {
            **run_status,
            "status": "stale",
            "error": "Run lock expired. The previous run may have stopped.",
            "lock_expires_at": lock.get("expires_at", ""),
        }

    return {
        **run_status,
        "lock_expires_at": lock.get("expires_at", ""),
    }


def can_start_batch_run(run_status, uploaded_count):
    status = run_status.get("status")
    if status == "running":
        return False
    if uploaded_count > 0:
        return True
    return status in {"failed", "stale", "stopped", "unknown"}


def register_batch_process(batch_id, run_id, process):
    with RUNNING_BATCH_PROCESS_LOCK:
        RUNNING_BATCH_PROCESSES[batch_id] = {
            "run_id": run_id,
            "process": process,
        }


def unregister_batch_process(batch_id, run_id):
    with RUNNING_BATCH_PROCESS_LOCK:
        existing = RUNNING_BATCH_PROCESSES.get(batch_id)
        if existing and existing.get("run_id") == run_id:
            RUNNING_BATCH_PROCESSES.pop(batch_id, None)


def running_batch_process(batch_id, run_id=None):
    with RUNNING_BATCH_PROCESS_LOCK:
        existing = RUNNING_BATCH_PROCESSES.get(batch_id)
        if not existing:
            return None
        if run_id and existing.get("run_id") != run_id:
            return None
        return existing.get("process")


def terminate_batch_process(process):
    if not process or process.poll() is not None:
        return False

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except Exception:
        try:
            process.terminate()
        except Exception:
            return False

    return True


def kill_batch_process(process):
    if not process or process.poll() is not None:
        return False

    try:
        os.killpg(process.pid, signal.SIGKILL)
    except Exception:
        try:
            process.kill()
        except Exception:
            return False

    return True


def force_kill_batch_process_after_timeout(process, timeout=10):
    if not process:
        return

    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        kill_batch_process(process)


def write_batch_run_status(batch_id, data, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    payload = {
        "batch_id": batch_id,
        "log_path": batch_run_log_path(batch_id),
        "updated_at": datetime.now(UTC).isoformat(),
        **data,
    }
    bucket.blob(batch_run_status_path(batch_id)).upload_from_string(
        json.dumps(payload, indent=2),
        content_type="application/json",
    )
    return payload


def upload_batch_run_log(batch_id, log_text, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    bucket.blob(batch_run_log_path(batch_id)).upload_from_string(
        log_text or "",
        content_type="text/plain",
    )


def read_batch_log_text(batch_id, bucket=None):
    bucket = bucket or get_bucket(DEFAULT_GCS_BUCKET)
    blob = bucket.blob(batch_run_log_path(batch_id))

    if blob.exists():
        return blob.download_as_text(encoding="utf-8")

    status = read_batch_run_status(batch_id, bucket)
    return status.get("log_tail") or ""


def initial_run_progress(stage="queued"):
    return {
        "stage": stage,
        "files_found": "",
        "files_processed": 0,
        "files_succeeded": 0,
        "files_failed": 0,
        "current_file": "",
        "imported_records": "",
        "last_message": "",
    }


def update_run_progress_from_line(progress, line):
    message = line.strip()
    if not message:
        return False

    previous_stage = progress.get("stage", "queued")
    progress["last_message"] = message[-500:]

    files_match = re.search(r"Found (\d+) file\(s\) to process", message)
    if files_match:
        progress["stage"] = "batch_processor"
        progress["files_found"] = int(files_match.group(1))

    if message == "== Batch processor ==":
        progress["stage"] = "batch_processor"
    elif message.startswith("Processing: "):
        progress["stage"] = "extracting"
        progress["current_file"] = message.removeprefix("Processing: ").strip()
    elif message.startswith("Success: "):
        progress["stage"] = "extracting"
        progress["files_succeeded"] = int(progress.get("files_succeeded") or 0) + 1
        progress["files_processed"] = (
            int(progress.get("files_succeeded") or 0)
            + int(progress.get("files_failed") or 0)
        )
        progress["current_file"] = ""
    elif message.startswith("Failed: "):
        progress["stage"] = "extracting"
        progress["files_failed"] = int(progress.get("files_failed") or 0) + 1
        progress["files_processed"] = (
            int(progress.get("files_succeeded") or 0)
            + int(progress.get("files_failed") or 0)
        )
        progress["current_file"] = ""
    elif message.startswith("Done. Results written"):
        progress["stage"] = "results_written"
    elif message == "== Airtable importer ==":
        progress["stage"] = "airtable_importer"
    elif message.startswith("Importing: ") or message.startswith("Recording failure: "):
        progress["stage"] = "airtable_importer"
        progress["current_file"] = message
    elif message.startswith("Done. Imported "):
        progress["stage"] = "airtable_importer"
        imported_match = re.search(r"Done\. Imported (\d+) records?", message)
        if imported_match:
            progress["imported_records"] = int(imported_match.group(1))
    elif message.startswith("Updated batch summary"):
        progress["stage"] = "airtable_summary"
        progress["current_file"] = ""

    return progress.get("stage") != previous_stage


def run_batch_pipeline(batch_id, run_id):
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    env = os.environ.copy()
    env["IMPORT_BATCH_ID"] = batch_id
    env["BATCH_INPUT_PREFIX"] = batch_input_prefix(batch_id)
    env["BATCH_RESULTS_PATH"] = batch_results_path(batch_id)
    env["PYTHONUNBUFFERED"] = "1"
    started_at = datetime.now(UTC).isoformat()
    progress = initial_run_progress("starting")
    log_lines = []
    stop_requested = False

    def current_log_tail():
        return "".join(log_lines)[-4000:]

    def write_running_status(bucket, log_tail=None):
        write_batch_run_status(
            batch_id,
            {
                "status": "running",
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": "",
                "return_code": "",
                "error": "",
                "log_tail": current_log_tail() if log_tail is None else log_tail,
                **progress,
            },
            bucket,
        )

    try:
        bucket = get_bucket(DEFAULT_GCS_BUCKET)
        clear_batch_stop_request(batch_id, bucket)
        manifest = read_batch_manifest(batch_id, bucket)
        env["BATCH_TARGET_COLLECTION"] = manifest.get("target_collection", "")
        env["BATCH_LOCATION"] = manifest.get("location", "")
        write_running_status(bucket, "Batch run queued.\n")

        process = subprocess.Popen(
            [sys.executable, "-u", "run_import_pipeline.py"],
            cwd=repo_dir,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            start_new_session=True,
        )
        register_batch_process(batch_id, run_id, process)

        last_status_write = 0
        for line in process.stdout or []:
            log_lines.append(line)
            stage_changed = update_run_progress_from_line(progress, line)
            if stop_requested_for_run(batch_id, run_id, bucket):
                stop_requested = True
                progress["stage"] = "stopping"
                progress["last_message"] = "Stop requested by operator."
                log_lines.append("Stop requested by operator.\n")
                terminate_batch_process(process)
                write_running_status(bucket)
                break

            now = time.monotonic()
            if stage_changed or now - last_status_write >= 2:
                write_running_status(bucket)
                last_status_write = now

        if stop_requested:
            try:
                return_code = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                kill_batch_process(process)
                return_code = process.wait()
        else:
            return_code = process.wait()
        log_text = "".join(log_lines)
        upload_batch_run_log(batch_id, log_text, bucket)
        stop_requested = stop_requested or stop_requested_for_run(batch_id, run_id, bucket)

        if stop_requested:
            status = "stopped"
            error = "Batch run stopped by operator."
            progress["stage"] = "stopped"
            progress["current_file"] = ""
        elif return_code == 0:
            status = "succeeded"
            error = ""
            progress["stage"] = "complete"
        else:
            status = "failed"
            error = f"run_import_pipeline.py exited with {return_code}"

        write_batch_run_status(
            batch_id,
            {
                "status": status,
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": datetime.now(UTC).isoformat(),
                "return_code": return_code,
                "error": error,
                "log_tail": log_text[-4000:],
                **progress,
            },
            bucket,
        )
    except Exception as error:
        error_text = str(error)
        try:
            write_batch_run_status(
                batch_id,
                {
                    "status": "failed",
                    "run_id": run_id,
                    "started_at": started_at,
                    "finished_at": datetime.now(UTC).isoformat(),
                    "return_code": "",
                    "error": error_text,
                    "log_tail": error_text,
                    **progress,
                },
            )
        except Exception:
            pass
    finally:
        try:
            unregister_batch_process(batch_id, run_id)
        except Exception:
            pass
        try:
            clear_batch_stop_request(batch_id)
        except Exception:
            pass
        try:
            release_batch_run_lock(batch_id, run_id)
        except Exception:
            pass


@app.get("/")
def health():
    return {"status": "ok", "version": APP_VERSION}


@app.get("/operator", response_class=HTMLResponse)
def operator_ui():
    return OPERATOR_UI_HTML


@app.post("/batches")
async def create_batch(req: Request, body: CreateBatchRequest):
    require_bearer_auth(req)

    batch_id = validate_batch_id(body.batch_id or make_batch_id())
    manifest = batch_manifest(batch_id, body)
    manifest_path = write_batch_manifest(manifest)

    return {
        **manifest,
        "manifest_path": manifest_path,
    }


@app.get("/batches")
async def list_batches(req: Request, limit: int = 20):
    require_bearer_auth(req)

    limit = max(1, min(limit, 100))
    bucket = get_bucket(DEFAULT_GCS_BUCKET)
    batch_ids = list_operator_batch_ids(bucket)[:limit]
    batches = [batch_status_payload(batch_id, bucket) for batch_id in batch_ids]
    batches.sort(key=lambda batch: batch.get("created_at") or batch["batch_id"], reverse=True)

    return {
        "bucket": DEFAULT_GCS_BUCKET,
        "batches": batches[:limit],
    }


@app.get("/airtable-options")
def get_airtable_options(req: Request):
    require_bearer_auth(req)
    collections = list_airtable_lookup_options("collections")
    locations = list_airtable_lookup_options("locations")
    legacy_collections = list_airtable_select_field_options(AIRTABLE_TABLE_NAME, AIRTABLE_LEGACY_COLLECTION_FIELD)
    legacy_added = merge_airtable_lookup_options(
        collections,
        legacy_collections["options"],
        "legacy_collection_select",
    )
    collections["legacy_options_seen"] = len(legacy_collections["options"])
    collections["legacy_options_added"] = legacy_added
    collections["warnings"].extend(legacy_collections["warnings"])

    return {
        "collections": collections["options"],
        "locations": locations["options"],
        "diagnostics": {
            "collections": {key: value for key, value in collections.items() if key != "options"},
            "locations": {key: value for key, value in locations.items() if key != "options"},
        },
    }


@app.post("/airtable-options/{kind}")
def create_airtable_option(req: Request, kind: str, body: CreateLookupOptionRequest):
    require_bearer_auth(req)

    option = get_or_create_airtable_lookup_option(kind, body.name)

    return {
        "kind": kind,
        **option,
    }


@app.post("/batches/{batch_id}/upload-url")
async def create_batch_upload_url(req: Request, batch_id: str, body: CreateUploadUrlRequest):
    require_bearer_auth(req)

    batch_id = validate_batch_id(batch_id)
    filename = safe_filename(body.filename)
    content_type = body.content_type or "application/octet-stream"
    object_path = f"{batch_input_prefix(batch_id)}{filename}"
    bucket = get_bucket(DEFAULT_GCS_BUCKET)
    blob = bucket.blob(object_path)

    if blob.exists() and not body.overwrite:
        raise HTTPException(status_code=409, detail="Object already exists")

    expires_at = datetime.now(UTC) + timedelta(minutes=SIGNED_UPLOAD_EXPIRATION_MINUTES)
    upload_url = blob.generate_signed_url(
        version="v4",
        expiration=expires_at,
        method="PUT",
        content_type=content_type,
    )

    return {
        "batch_id": batch_id,
        "bucket": DEFAULT_GCS_BUCKET,
        "object_path": object_path,
        "gcs_uri": f"gs://{DEFAULT_GCS_BUCKET}/{object_path}",
        "upload_url": upload_url,
        "method": "PUT",
        "headers": {
            "Content-Type": content_type,
        },
        "expires_at": expires_at.isoformat(),
    }


@app.get("/batches/{batch_id}")
async def get_batch_status(req: Request, batch_id: str):
    require_bearer_auth(req)

    batch_id = validate_batch_id(batch_id)
    return batch_status_payload(batch_id)


@app.get("/batches/{batch_id}/failures")
async def get_batch_failures(req: Request, batch_id: str):
    require_bearer_auth(req)

    batch_id = validate_batch_id(batch_id)
    failures = batch_failure_rows(batch_id)

    return {
        "batch_id": batch_id,
        "count": len(failures),
        "failures": failures,
    }


@app.get("/batches/{batch_id}/verification")
async def get_batch_verification(req: Request, batch_id: str):
    require_bearer_auth(req)

    batch_id = validate_batch_id(batch_id)
    return batch_verification_payload(batch_id)


@app.get("/batches/{batch_id}/log")
async def get_batch_log(req: Request, batch_id: str):
    require_bearer_auth(req)

    batch_id = validate_batch_id(batch_id)
    log_text = read_batch_log_text(batch_id)

    return {
        "batch_id": batch_id,
        "log_path": batch_run_log_path(batch_id),
        "log_text": log_text,
    }


@app.post("/batches/{batch_id}/retry-failures")
async def retry_failures(req: Request, batch_id: str, body: RetryFailuresRequest | None = None):
    require_bearer_auth(req)

    batch_id = validate_batch_id(batch_id)
    body = body or RetryFailuresRequest()
    bucket = get_bucket(DEFAULT_GCS_BUCKET)
    current_run = normalize_run_status_for_lock(
        batch_id,
        read_batch_run_status(batch_id, bucket),
        bucket,
    )

    if current_run.get("status") == "running":
        raise HTTPException(status_code=409, detail="Wait for the active run to finish before retrying failures")

    retry_result = retry_batch_failures(batch_id, body.max_files, bucket)

    return {
        **retry_result,
        "batch": batch_status_payload(batch_id, bucket),
        "failures": batch_failure_rows(batch_id, bucket),
    }


@app.post("/batches/{batch_id}/stop")
async def stop_batch(req: Request, batch_id: str):
    require_bearer_auth(req)

    batch_id = validate_batch_id(batch_id)
    bucket = get_bucket(DEFAULT_GCS_BUCKET)
    current_run = normalize_run_status_for_lock(
        batch_id,
        read_batch_run_status(batch_id, bucket),
        bucket,
    )

    if current_run.get("status") != "running":
        return batch_status_payload(batch_id, bucket)

    run_id = current_run.get("run_id", "")
    write_batch_stop_request(batch_id, run_id, bucket)
    process = running_batch_process(batch_id, run_id)
    signalled = terminate_batch_process(process)
    if signalled:
        threading.Thread(
            target=force_kill_batch_process_after_timeout,
            args=(process,),
            daemon=True,
        ).start()
    log_tail = (current_run.get("log_tail") or "").rstrip()
    if log_tail:
        log_tail = f"{log_tail}\nStop requested by operator."
    else:
        log_tail = "Stop requested by operator."

    status_update = {
        key: value
        for key, value in current_run.items()
        if key not in {"updated_at", "lock_expires_at"}
    }

    write_batch_run_status(
        batch_id,
        {
            **status_update,
            "status": "running",
            "stage": "stopping",
            "last_message": "Stop requested by operator.",
            "current_file": "",
            "error": "",
            "log_tail": log_tail[-4000:],
            "stop_signal_sent": signalled,
        },
        bucket,
    )

    return batch_status_payload(batch_id, bucket)


@app.post("/batches/{batch_id}/run")
async def run_batch(req: Request, batch_id: str, body: RunBatchRequest | None = None):
    require_bearer_auth(req)

    batch_id = validate_batch_id(batch_id)
    body = body or RunBatchRequest()
    bucket = get_bucket(DEFAULT_GCS_BUCKET)
    current_run = normalize_run_status_for_lock(
        batch_id,
        read_batch_run_status(batch_id, bucket),
        bucket,
    )
    uploaded_count = count_batch_uploads(batch_id, bucket)

    if not can_start_batch_run(current_run, uploaded_count) and not body.force:
        if current_run.get("status") == "running":
            raise HTTPException(status_code=409, detail="Batch is already running")
        if current_run.get("status") == "succeeded":
            raise HTTPException(
                status_code=409,
                detail="Batch already succeeded and has no new files waiting",
            )
        raise HTTPException(status_code=400, detail="No files are waiting in this batch")

    run_id = f"run-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    lock, existing_lock = acquire_batch_run_lock(batch_id, run_id, bucket, force=body.force)
    if not lock:
        detail = "Batch is already running"
        if existing_lock and existing_lock.get("expires_at"):
            detail = f"Batch is already running until {existing_lock['expires_at']}"
        raise HTTPException(status_code=409, detail=detail)

    write_batch_run_status(
        batch_id,
        {
            "status": "running",
            "run_id": run_id,
            "started_at": datetime.now(UTC).isoformat(),
            "finished_at": "",
            "return_code": "",
            "error": "",
            "log_tail": "Batch run queued.",
            **initial_run_progress("queued"),
        },
        bucket,
    )

    thread = threading.Thread(target=run_batch_pipeline, args=(batch_id, run_id), daemon=True)
    thread.start()

    return batch_status_payload(batch_id, bucket)


@app.post("/extract")
def extract(req: Request, body: ExtractRequest):
    require_bearer_auth(req)

    image_bytes = download_gcs_bytes(body.gcs_bucket, body.gcs_object_path)
    image_bytes, orientation_action = fix_image_orientation(image_bytes)
    downloaded_image_bytes = len(image_bytes)

    ocr_text, ocr_conf = run_ocr_document_text(image_bytes)

    del image_bytes

    client = OpenAI(api_key=OPENAI_API_KEY)

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=build_extraction_messages(ocr_text),
        response_format={"type": "json_object"},
    )

    parsed = normalize_parsed_output(json.loads(resp.choices[0].message.content))

    quality_flags = build_quality_flags(ocr_text, ocr_conf, parsed)
    extraction_evidence_json = json_string_or_blank(
        parsed.get("extraction_evidence_json") or parsed.get("extraction_evidence")
    )

    return {
        "app_version": APP_VERSION,
        "image_source": "Google Cloud Storage",
        "image_ref": body.gcs_object_path,
        "ocr_text": ocr_text,
        "ocr_confidence": ocr_conf,
        "ocr_length": len(ocr_text),
        "llm_confidence": parsed.get("llm_confidence", 0.0),
        "language": parsed.get("language", "") or "",
        "title": parsed.get("title", "") or "",
        "author": parsed.get("author", "") or "",
        "publication_place": parsed.get("publication_place", "") or "",
        "publisher": parsed.get("publisher", "") or "",
        "publication_year": parsed.get("publication_year"),
        "edition_statement": parsed.get("edition_statement", "") or "",
        "publication_statement_verbatim": parsed.get("publication_statement_verbatim", "") or "",
        "translator": parsed.get("translator", "") or "",
        "illustration_note": parsed.get("illustration_note", "") or "",
        "extraction_evidence_json": extraction_evidence_json,
        "quality_flags_json": json.dumps(quality_flags),
        "debug": {
            "downloaded_image_bytes": downloaded_image_bytes,
            "orientation_action": orientation_action,
        },
    }
