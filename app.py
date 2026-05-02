from PIL import Image, ExifTags
import io
import os
import json
import re
from datetime import timedelta
from typing import Optional, Dict, Any, List, Tuple

import httpx
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

HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "90"))
MAX_OCR_CHARS_FOR_LLM = int(os.getenv("MAX_OCR_CHARS_FOR_LLM", "12000"))

app = FastAPI(title="RB Extractor", version="1.6.3-quality-flags")


class ExtractRequest(BaseModel):
    record_id: str
    image_url: Optional[str] = None
    gcs_bucket: Optional[str] = None
    gcs_object_path: Optional[str] = None
    item_id: Optional[str] = None
    collection: Optional[str] = None


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


def download_gcs_bytes(bucket_name, object_path):
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    if not blob.exists():
        raise HTTPException(404, "GCS object not found")

    return blob.download_as_bytes()


def generate_gcs_signed_url(bucket_name, object_path):
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=2),
        method="GET",
    )


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


def normalize_parsed_output(parsed):
    parsed["llm_confidence"] = clamp01(parsed.get("llm_confidence"))
    if parsed["llm_confidence"] >= 1:
        parsed["llm_confidence"] = 0.95
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


@app.get("/")
def health():
    return {"status": "ok", "version": "1.6.3-quality-flags"}


@app.post("/extract")
async def extract(req: Request, body: ExtractRequest):
    require_bearer_auth(req)

    if body.gcs_bucket:
        image_bytes = download_gcs_bytes(body.gcs_bucket, body.gcs_object_path)
        image_url = generate_gcs_signed_url(body.gcs_bucket, body.gcs_object_path)
    else:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
            r = await client.get(body.image_url)
            image_bytes = r.content
        image_url = body.image_url

    image_bytes, orientation_action = fix_image_orientation(image_bytes)
    downloaded_image_bytes = len(image_bytes)

    ocr_text, ocr_conf = run_ocr_document_text(image_bytes)

    del image_bytes

    client = OpenAI(api_key=OPENAI_API_KEY)

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "user", "content": ocr_text},
        ],
        response_format={"type": "json_object"},
    )

    parsed = normalize_parsed_output(json.loads(resp.choices[0].message.content))

    quality_flags = build_quality_flags(ocr_text, ocr_conf, parsed)
    extraction_evidence_json = json_string_or_blank(
        parsed.get("extraction_evidence_json") or parsed.get("extraction_evidence")
    )

    return {
        "app_version": "1.6.3-quality-flags",
        "image_source": "Google Cloud Storage" if body.gcs_bucket else "URL",
        "image_ref": (body.gcs_object_path or "") if body.gcs_bucket else (image_url or ""),
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
