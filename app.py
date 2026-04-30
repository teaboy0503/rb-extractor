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

app = FastAPI(title="RB Extractor", version="1.6.1-gcs")


class ExtractRequest(BaseModel):
    record_id: str
    image_url: Optional[str] = None
    gcs_bucket: Optional[str] = None
    gcs_object_path: Optional[str] = None
    item_id: Optional[str] = None
    collection: Optional[str] = None


def require_bearer_auth(req: Request) -> None:
    if not API_KEY:
        raise HTTPException(status_code=500, detail="API_KEY env var not set on server")

    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization: Bearer <token> header")

    token = auth.split(" ", 1)[1].strip()
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid token")


def get_google_credentials():
    if not GOOGLE_CREDENTIALS_JSON:
        raise HTTPException(status_code=500, detail="GOOGLE_CREDENTIALS_JSON env var not set on server")

    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        return credentials, creds_dict.get("project_id")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load Google credentials: {str(e)}")


def get_storage_client() -> storage.Client:
    credentials, project_id = get_google_credentials()
    return storage.Client(credentials=credentials, project=project_id)


def download_gcs_bytes(bucket_name: str, object_path: str) -> bytes:
    try:
        client = get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_path)

        if not blob.exists():
            raise HTTPException(status_code=404, detail=f"GCS object not found: gs://{bucket_name}/{object_path}")

        return blob.download_as_bytes()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to download from GCS: {str(e)}")


def generate_gcs_signed_url(bucket_name: str, object_path: str) -> str:
    try:
        client = get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_path)

        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(hours=2),
            method="GET",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create signed GCS URL: {str(e)}")


def fix_image_orientation(image_bytes: bytes) -> Tuple[bytes, str]:
    try:
        image = Image.open(io.BytesIO(image_bytes))
        orientation_tag = None

        for tag, name in ExifTags.TAGS.items():
            if name == "Orientation":
                orientation_tag = tag
                break

        orientation_action = "none"
        exif = image._getexif()

        if exif is not None and orientation_tag is not None:
            orientation_value = exif.get(orientation_tag)

            if orientation_value == 3:
                image = image.rotate(180, expand=True)
                orientation_action = "rotated_180"
            elif orientation_value == 6:
                image = image.rotate(270, expand=True)
                orientation_action = "rotated_270"
            elif orientation_value == 8:
                image = image.rotate(90, expand=True)
                orientation_action = "rotated_90"
            else:
                orientation_action = f"exif_present_no_rotation_{orientation_value}"
        else:
            orientation_action = "no_exif"

        output = io.BytesIO()

        if image.mode != "RGB":
            image = image.convert("RGB")

        image.save(output, format="JPEG", quality=90)
        return output.getvalue(), orientation_action

    except Exception as e:
        return image_bytes, f"orientation_fix_failed:{str(e)}"


def get_vision_client() -> vision.ImageAnnotatorClient:
    try:
        credentials, _ = get_google_credentials()
        return vision.ImageAnnotatorClient(credentials=credentials)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to init Vision client: {str(e)}")


def run_ocr_document_text(image_bytes: bytes) -> Tuple[str, float]:
    client = get_vision_client()
    img = vision.Image(content=image_bytes)
    resp = client.document_text_detection(image=img)

    if resp.error.message:
        raise HTTPException(status_code=500, detail=f"Vision API error: {resp.error.message}")

    full_text = (resp.full_text_annotation.text or "").strip()

    confidences: List[float] = []
    try:
        for page in resp.full_text_annotation.pages:
            for block in page.blocks:
                for para in block.paragraphs:
                    for word in para.words:
                        confidences.append(float(word.confidence))
    except Exception:
        confidences = []

    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    return full_text, float(avg_conf)


_ROMAN_MAP = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}


def roman_to_int(s: str) -> Optional[int]:
    if not s:
        return None

    s = re.sub(r"[^IVXLCDM]", "", s.upper())
    if len(s) < 3:
        return None

    total = 0
    prev = 0

    for ch in reversed(s):
        if ch not in _ROMAN_MAP:
            return None

        val = _ROMAN_MAP[ch]
        if val < prev:
            total -= val
        else:
            total += val
            prev = val

    return total if 1400 <= total <= 2026 else None


def coerce_year(value: Any) -> Optional[int]:
    if value is None:
        return None

    if isinstance(value, int):
        return value if 1400 <= value <= 2026 else None

    if isinstance(value, float) and value.is_integer():
        iv = int(value)
        return iv if 1400 <= iv <= 2026 else None

    s = str(value).strip()
    m = re.search(r"\b(1[4-9]\d{2}|20[0-2]\d)\b", s)

    if m:
        return int(m.group(1))

    return roman_to_int(s)


def normalize_language(lang: Any) -> str:
    allowed = {"English", "French", "German", "Latin", "Other/Unknown"}

    if not lang:
        return "Other/Unknown"

    s = str(lang).strip()
    if s in allowed:
        return s

    low = s.lower()

    if "french" in low or "français" in low:
        return "French"
    if "german" in low or "deutsch" in low:
        return "German"
    if "latin" in low or "lat." in low:
        return "Latin"
    if "english" in low:
        return "English"

    return "Other/Unknown"


def clamp01(x: Any) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.0

    return max(0.0, min(1.0, v))


def normalize_parsed_output(parsed: Dict[str, Any]) -> Dict[str, Any]:
    parsed["language"] = normalize_language(parsed.get("language"))
    parsed["llm_confidence"] = clamp01(parsed.get("llm_confidence"))
    parsed["publication_year"] = coerce_year(parsed.get("publication_year"))

    text_fields = [
        "title",
        "author",
        "publication_place",
        "publisher",
        "edition_statement",
        "publication_statement_verbatim",
        "translator",
        "illustration_note",
    ]

    for k in text_fields:
        parsed[k] = "" if parsed.get(k) is None else str(parsed.get(k, "")).strip()

    if "evidence" not in parsed or parsed["evidence"] is None or not isinstance(parsed["evidence"], dict):
        parsed["evidence"] = {}

    for k in ["title", "author", "publication_place", "publisher", "publication_year"]:
        parsed["evidence"][k] = "" if parsed["evidence"].get(k) is None else str(parsed["evidence"].get(k, "")).strip()

    return parsed


def llm_parse_title_page(ocr_text: str, image_url: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY env var not set on server")

    client = OpenAI(api_key=OPENAI_API_KEY)

    text = ocr_text.strip()
    if len(text) > MAX_OCR_CHARS_FOR_LLM:
        text = text[:MAX_OCR_CHARS_FOR_LLM]

    system = (
        "You are an expert rare-books cataloguer working from a title page image and OCR text. "
        "Extract bibliographic metadata accurately and conservatively. "
        "Use BOTH the image and OCR text. "
        "Return STRICT JSON only. No markdown. No explanation."
    )

    user_text = {
        "task": "Extract bibliographic metadata from this rare book title page.",
        "inputs": {
            "ocr_text": text,
            "image_role": "Use the image to understand layout, hierarchy, emphasis, and the lower imprint block.",
        },
        "cataloguing_rules": [
            "Do not invent details not supported by OCR or image.",
            "If uncertain, leave a field empty, or null for publication_year.",
            "If year is Roman numerals, convert to integer.",
            "Preserve imprint/publication statement in publication_statement_verbatim.",
            "Title is usually the largest central text.",
            "Publisher/imprint is usually in the lower block.",
            "Translator only if explicitly stated.",
            "Illustration note should capture references to plates, engravings, profiles, figures, Kupfern, etc.",
            "Language must be one of: English, French, German, Latin, Other/Unknown.",
        ],
        "required_json_schema": {
            "language": "English|French|German|Latin|Other/Unknown",
            "title": "string",
            "author": "string",
            "publication_place": "string",
            "publisher": "string",
            "publication_year": "integer|null",
            "edition_statement": "string",
            "publication_statement_verbatim": "string",
            "translator": "string",
            "illustration_note": "string",
            "llm_confidence": "number 0..1",
            "evidence": {
                "title": "string",
                "author": "string",
                "publication_place": "string",
                "publisher": "string",
                "publication_year": "string",
            },
        },
    }

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": json.dumps(user_text, ensure_ascii=False)},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            },
        ],
        response_format={"type": "json_object"},
    )

    try:
        parsed = json.loads(resp.choices[0].message.content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse extraction JSON: {str(e)}")

    return normalize_parsed_output(parsed)


def verify_title_page_extraction(ocr_text: str, image_url: str, draft: Dict[str, Any]) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY env var not set on server")

    client = OpenAI(api_key=OPENAI_API_KEY)

    text = ocr_text.strip()
    if len(text) > MAX_OCR_CHARS_FOR_LLM:
        text = text[:MAX_OCR_CHARS_FOR_LLM]

    system = (
        "You are an expert rare-books cataloguer verifying extracted bibliographic metadata. "
        "Check each field against OCR and image. Correct or blank unsupported values. "
        "Return STRICT JSON only. No markdown. No explanation."
    )

    user_text = {
        "task": "Verify and correct extracted bibliographic metadata.",
        "ocr_text": text,
        "draft_extraction": draft,
        "verification_rules": [
            "Only keep values supported by OCR or image.",
            "If publication_year is uncertain, return null.",
            "If publication_place or publisher is uncertain, return empty string.",
            "Preserve publication_statement_verbatim close to printed imprint.",
            "Adjust llm_confidence downward if key fields are uncertain.",
        ],
        "required_json_schema": {
            "language": "English|French|German|Latin|Other/Unknown",
            "title": "string",
            "author": "string",
            "publication_place": "string",
            "publisher": "string",
            "publication_year": "integer|null",
            "edition_statement": "string",
            "publication_statement_verbatim": "string",
            "translator": "string",
            "illustration_note": "string",
            "llm_confidence": "number 0..1",
            "evidence": {
                "title": "string",
                "author": "string",
                "publication_place": "string",
                "publisher": "string",
                "publication_year": "string",
            },
        },
    }

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": json.dumps(user_text, ensure_ascii=False)},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            },
        ],
        response_format={"type": "json_object"},
    )

    try:
        verified = json.loads(resp.choices[0].message.content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse verification JSON: {str(e)}")

    return normalize_parsed_output(verified)


@app.get("/")
def health():
    return {"status": "ok", "version": "1.6.1-gcs"}


@app.post("/extract")
async def extract(req: Request, body: ExtractRequest):
    require_bearer_auth(req)

    if body.gcs_bucket and body.gcs_object_path:
        image_bytes = download_gcs_bytes(body.gcs_bucket, body.gcs_object_path)
        image_source = "gcs"
        image_ref = f"gs://{body.gcs_bucket}/{body.gcs_object_path}"
        openai_image_url = generate_gcs_signed_url(body.gcs_bucket, body.gcs_object_path)

    elif body.image_url:
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS, follow_redirects=True) as client:
                r = await client.get(body.image_url)
                r.raise_for_status()
                image_bytes = r.content

            image_source = "url"
            image_ref = body.image_url
            openai_image_url = body.image_url

        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to download image: {str(e)}")

    else:
        raise HTTPException(status_code=400, detail="Provide either image_url or both gcs_bucket and gcs_object_path")

    image_bytes, orientation_action = fix_image_orientation(image_bytes)

    downloaded_image_bytes = len(image_bytes)

    ocr_text, ocr_conf = run_ocr_document_text(image_bytes)

    del image_bytes

    try:
        first_pass = llm_parse_title_page(ocr_text, openai_image_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM extraction failed: {str(e)}")

    try:
        final_pass = verify_title_page_extraction(ocr_text, openai_image_url, first_pass)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM verification failed: {str(e)}")

    return {
        "record_id": body.record_id,
        "app_version": "1.6.1-gcs",
        "image_source": image_source,
        "image_ref": image_ref,
        "ocr_text": ocr_text,
        "ocr_confidence": float(ocr_conf),
        "llm_confidence": float(final_pass.get("llm_confidence", 0.0)),
        "language": final_pass.get("language", "Other/Unknown"),
        "title": final_pass.get("title", ""),
        "author": final_pass.get("author", ""),
        "publication_place": final_pass.get("publication_place", ""),
        "publisher": final_pass.get("publisher", ""),
        "publication_year": final_pass.get("publication_year", None),
        "edition_statement": final_pass.get("edition_statement", ""),
        "publication_statement_verbatim": final_pass.get("publication_statement_verbatim", ""),
        "translator": final_pass.get("translator", ""),
        "illustration_note": final_pass.get("illustration_note", ""),
        "ocr_length": len(ocr_text),
        "extraction_evidence_json": json.dumps(final_pass.get("evidence", {}), ensure_ascii=False),
        "debug": {
            "downloaded_image_bytes": downloaded_image_bytes,
            "collection": body.collection,
            "item_id": body.item_id,
            "model": OPENAI_MODEL,
            "ocr_chars_sent_to_llm": min(len(ocr_text), MAX_OCR_CHARS_FOR_LLM),
            "orientation_action": orientation_action,
            "image_sent_to_llm": True,
            "openai_image_url_source": image_source,
            "ocr_preview": ocr_text[:300],
            "first_pass_llm_confidence": float(first_pass.get("llm_confidence", 0.0)),
            "second_pass_verifier_used": True,
        },
    }
