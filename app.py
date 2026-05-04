from PIL import Image, ExifTags
import csv
import io
import os
import json
import re
from datetime import UTC, datetime, timedelta

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from google.cloud import storage
from google.cloud import vision
from google.oauth2 import service_account

from openai import OpenAI


API_KEY = os.getenv("API_KEY", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

MAX_OCR_CHARS_FOR_LLM = int(os.getenv("MAX_OCR_CHARS_FOR_LLM", "12000"))
DEFAULT_GCS_BUCKET = os.getenv("BATCH_GCS_BUCKET", "rb-title-pages-2026")
BATCH_UPLOAD_ROOT_PREFIX = os.getenv("BATCH_UPLOAD_ROOT_PREFIX", "imports/")
SIGNED_UPLOAD_EXPIRATION_MINUTES = int(os.getenv("SIGNED_UPLOAD_EXPIRATION_MINUTES", "60"))
APP_VERSION = "1.7.0-operator-api"

app = FastAPI(title="RB Extractor", version=APP_VERSION)


class ExtractRequest(BaseModel):
    gcs_bucket: str
    gcs_object_path: str


class CreateBatchRequest(BaseModel):
    batch_id: str | None = None
    source: str | None = None
    target_collection: str | None = None
    notes: str | None = None


class CreateUploadUrlRequest(BaseModel):
    filename: str
    content_type: str = "application/octet-stream"
    overwrite: bool = False


def require_bearer_auth(req: Request) -> None:
    if not API_KEY:
        raise HTTPException(status_code=500, detail="API_KEY env var not set")

    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = auth.split(" ", 1)[1].strip()
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid token")


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


def count_batch_uploads(batch_id):
    bucket = get_bucket(DEFAULT_GCS_BUCKET)
    prefix = batch_input_prefix(batch_id)
    count = 0

    for blob in bucket.client.list_blobs(DEFAULT_GCS_BUCKET, prefix=prefix):
        if not blob.name.endswith("/"):
            count += 1

    return count


def batch_results_counts(batch_id):
    bucket = get_bucket(DEFAULT_GCS_BUCKET)
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


@app.get("/")
def health():
    return {"status": "ok", "version": APP_VERSION}


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

    return {
        "batch_id": batch_id,
        "bucket": DEFAULT_GCS_BUCKET,
        "input_prefix": batch_input_prefix(batch_id),
        "results_path": batch_results_path(batch_id),
        "uploaded_count": count_batch_uploads(batch_id),
        "results": batch_results_counts(batch_id),
        "run_command": batch_run_command(batch_id),
    }


@app.post("/extract")
async def extract(req: Request, body: ExtractRequest):
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
