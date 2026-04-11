from PIL import Image, ExifTags
import io
import os
import json
import re
from typing import Optional, Dict, Any, List, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from google.cloud import vision
from google.oauth2 import service_account

from openai import OpenAI


# -----------------------------
# Environment variables required
# -----------------------------
# Airtable -> Render auth
API_KEY = os.getenv("API_KEY", "")

# Google Vision credentials (paste full JSON into this env var)
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

# Optional tuning
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "90"))
MAX_OCR_CHARS_FOR_LLM = int(os.getenv("MAX_OCR_CHARS_FOR_LLM", "12000"))


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="RB Extractor", version="1.4.0")


# -----------------------------
# Request model
# -----------------------------
class ExtractRequest(BaseModel):
    record_id: str
    image_url: str
    item_id: Optional[str] = None
    collection: Optional[str] = None


# -----------------------------
# Auth
# -----------------------------
def require_bearer_auth(req: Request) -> None:
    if not API_KEY:
        raise HTTPException(status_code=500, detail="API_KEY env var not set on server")

    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization: Bearer <token> header")

    token = auth.split(" ", 1)[1].strip()
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid token")


# -----------------------------
# Image orientation correction
# -----------------------------
def fix_image_orientation(image_bytes: bytes) -> Tuple[bytes, str]:
    """
    Reads EXIF orientation data and rotates the image if necessary.
    Returns:
      - corrected image bytes
      - orientation action string for debug
    """
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

        fmt = image.format if image.format in ["JPEG", "PNG"] else "JPEG"
        if fmt == "JPEG" and image.mode in ("RGBA", "P"):
            image = image.convert("RGB")

        image.save(output, format=fmt)
        return output.getvalue(), orientation_action

    except Exception as e:
        return image_bytes, f"orientation_fix_failed:{str(e)}"


# -----------------------------
# Google Vision OCR
# -----------------------------
def get_vision_client() -> vision.ImageAnnotatorClient:
    if not GOOGLE_CREDENTIALS_JSON:
        raise HTTPException(status_code=500, detail="GOOGLE_CREDENTIALS_JSON env var not set on server")

    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        return vision.ImageAnnotatorClient(credentials=credentials)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to init Vision client: {str(e)}")


def run_ocr_document_text(image_bytes: bytes) -> Tuple[str, float]:
    """
    Uses document_text_detection (best for scanned pages / dense text).
    Returns (full_text, avg_word_confidence).
    """
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


# -----------------------------
# Roman numeral parsing (best effort)
# -----------------------------
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

    if 1400 <= total <= 2026:
        return total
    return None


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

    r = roman_to_int(s)
    return r


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
        "illustration_note"
    ]

    for k in text_fields:
        if k not in parsed or parsed[k] is None:
            parsed[k] = ""
        else:
            parsed[k] = str(parsed[k]).strip()

    if "evidence" not in parsed or parsed["evidence"] is None or not isinstance(parsed["evidence"], dict):
        parsed["evidence"] = {}

    evidence_fields = ["title", "author", "publication_place", "publisher", "publication_year"]
    for k in evidence_fields:
        if k not in parsed["evidence"] or parsed["evidence"][k] is None:
            parsed["evidence"][k] = ""
        else:
            parsed["evidence"][k] = str(parsed["evidence"][k]).strip()

    return parsed


# -----------------------------
# OpenAI parsing (OCR text + image)
# -----------------------------
def llm_parse_title_page(ocr_text: str, image_url: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY env var not set on server")

    client = OpenAI(api_key=OPENAI_API_KEY)

    text = ocr_text.strip()
    if len(text) > MAX_OCR_CHARS_FOR_LLM:
        text = text[:MAX_OCR_CHARS_FOR_LLM]

    system = (
        "You are an expert rare-books cataloguer working from a title page image and OCR text. "
        "Your job is to extract bibliographic metadata accurately and conservatively. "
        "Use BOTH the image and OCR text. The image is important because OCR may lose layout, line grouping, emphasis, and the exact position of imprint lines. "
        "Treat this as a title page of an antiquarian or historical printed book. "
        "Prioritise bibliographic accuracy over completeness. "
        "Return STRICT JSON only. No markdown. No explanation."
    )

    user_text = {
        "task": "Extract bibliographic metadata from this rare book title page.",
        "inputs": {
            "ocr_text": text,
            "image_role": "Use the image to understand layout, hierarchy, emphasis, and the lower imprint block."
        },
        "cataloguing_rules": [
            "Do not invent details not supported by the OCR text or visible image.",
            "If uncertain, leave a field as empty string, or null for publication_year.",
            "If a year appears in Roman numerals, convert it to an integer year.",
            "Preserve the imprint/publication statement as closely as possible in publication_statement_verbatim.",
            "Correct obvious OCR mistakes where meaning is clear, but do not guess missing words.",
            "Title is usually the largest or most prominent central text.",
            "Author is often introduced by words such as BY, PAR, VON, APUD, or equivalent contextual placement.",
            "Publisher/imprint is usually in the lower block of the page.",
            "Publication place often appears immediately before or within the imprint block.",
            "Edition statement often appears above the imprint line.",
            "Translator should be extracted only if the page explicitly states that the work was translated by someone.",
            "Illustration note should capture references to plates, engravings, profiles, figures, Kupfern, etc.",
            "Language must be one of: English, French, German, Latin, Other/Unknown."
        ],
        "field_guidance": {
            "title": "Main work title only, normalised for obvious OCR errors.",
            "author": "Main named author only, not translator/editor unless clearly the main author.",
            "publication_place": "City/place of publication if explicitly shown.",
            "publisher": "Printer, bookseller, publisher, or imprint entity.",
            "publication_year": "Integer year only, converted from Roman numerals if needed.",
            "edition_statement": "Edition wording or format statement such as Fifth Edition, revised edition, etc.",
            "publication_statement_verbatim": "The imprint/publication statement as printed, preserved as closely as possible.",
            "translator": "Named translator if explicitly stated.",
            "illustration_note": "Any explicit reference to plates, engravings, profiles, figures, Kupfern, etc."
        },
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
                "publication_year": "string"
            }
        }
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
                    {"type": "image_url", "image_url": {"url": image_url}}
                ],
            },
        ],
        response_format={"type": "json_object"},
    )

    content = resp.choices[0].message.content
    try:
        parsed = json.loads(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse LLM JSON: {str(e)}")

    return normalize_parsed_output(parsed)


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def health():
    return {"status": "ok", "version": "1.4.0-prompt-upgrade"}


@app.post("/extract")
async def extract(req: Request, body: ExtractRequest):
    require_bearer_auth(req)

    # 1) Download image bytes
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS, follow_redirects=True) as client:
            r = await client.get(body.image_url)
            r.raise_for_status()
            image_bytes = r.content
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to download image: {str(e)}")

    # 2) Fix image orientation before OCR
    image_bytes, orientation_action = fix_image_orientation(image_bytes)

    # 3) OCR
    ocr_text, ocr_conf = run_ocr_document_text(image_bytes)

    # 4) LLM parse using OCR text + image URL
    try:
        parsed = llm_parse_title_page(ocr_text, body.image_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM parsing failed: {str(e)}")

    # 5) Return combined payload
    return {
        "record_id": body.record_id,
        "app_version": "1.4.0-prompt-upgrade",
        "ocr_text": ocr_text,
        "ocr_confidence": float(ocr_conf),
        "llm_confidence": float(parsed.get("llm_confidence", 0.0)),
        "language": parsed.get("language", "Other/Unknown"),

        "title": parsed.get("title", ""),
        "author": parsed.get("author", ""),
        "publication_place": parsed.get("publication_place", ""),
        "publisher": parsed.get("publisher", ""),
        "publication_year": parsed.get("publication_year", None),
        "edition_statement": parsed.get("edition_statement", ""),
        "publication_statement_verbatim": parsed.get("publication_statement_verbatim", ""),
        "translator": parsed.get("translator", ""),
        "illustration_note": parsed.get("illustration_note", ""),
        "ocr_length": len(ocr_text),
        "extraction_evidence_json": json.dumps(parsed.get("evidence", {}), ensure_ascii=False),

        "debug": {
            "downloaded_image_bytes": len(image_bytes),
            "collection": body.collection,
            "item_id": body.item_id,
            "model": OPENAI_MODEL,
            "ocr_chars_sent_to_llm": min(len(ocr_text), MAX_OCR_CHARS_FOR_LLM),
            "orientation_action": orientation_action,
            "image_sent_to_llm": True,
            "ocr_preview": ocr_text[:300]
        }
    }
