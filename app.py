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
app = FastAPI(title="RB Extractor", version="1.0.0")


# -----------------------------
# Request/Response models
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
    """
    Converts roman numerals like 'MDCCLXV' to 1765.
    Returns None if not plausible.
    """
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

    # plausible printing years for your collection
    if 1400 <= total <= 2026:
        return total
    return None


def coerce_year(value: Any) -> Optional[int]:
    """
    Accepts int or strings like '1765', 'MDCCLXV', '1765?'.
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value if 1400 <= value <= 2026 else None
    if isinstance(value, float) and value.is_integer():
        iv = int(value)
        return iv if 1400 <= iv <= 2026 else None

    s = str(value).strip()
    # digits first
    m = re.search(r"\b(1[4-9]\d{2}|20[0-2]\d)\b", s)
    if m:
        return int(m.group(1))

    # roman
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


# -----------------------------
# OpenAI parsing
# -----------------------------
def llm_parse_title_page(ocr_text: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY env var not set on server")

    client = OpenAI(api_key=OPENAI_API_KEY)

    # Keep prompt bounded
    text = ocr_text.strip()
    if len(text) > MAX_OCR_CHARS_FOR_LLM:
        text = text[:MAX_OCR_CHARS_FOR_LLM]

    system = (
        "You are a careful rare-books cataloguer. "
        "You extract bibliographic metadata from OCR text of a TITLE PAGE. "
        "Return STRICT JSON only. No markdown. No explanation."
    )

    user_obj = {
        "task": "Extract bibliographic fields from OCR text of a rare book title page.",
        "ocr_text": text,
        "output_rules": [
            "Do not invent details not supported by the OCR text.",
            "If uncertain, leave field as empty string (or null for publication_year).",
            "If the year is roman numerals (e.g., MDCCLXV), return it as an integer year.",
            "Prefer the first/main author name on the title page.",
            "Publisher/imprint is the printer/publisher line (often begins with 'Printed for', 'Chez', 'Verlag', 'Apud', etc.).",
            "publication_place is the place name if present (e.g., London, Paris, Leipzig).",
            "language must be one of: English, French, German, Latin, Other/Unknown."
        ],
        "required_json_schema": {
            "language": "English|French|German|Latin|Other/Unknown",
            "title": "string",
            "author": "string",
            "publication_place": "string",
            "publisher": "string",
            "publication_year": "integer|null",
            "edition_statement": "string",
            "llm_confidence": "number 0..1"
        }
    }

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user_obj, ensure_ascii=False)},
        ],
        response_format={"type": "json_object"},
    )

    content = resp.choices[0].message.content
    try:
        parsed = json.loads(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse LLM JSON: {str(e)}")

    # Normalize/coerce
    parsed["language"] = normalize_language(parsed.get("language"))
    parsed["llm_confidence"] = clamp01(parsed.get("llm_confidence"))
    parsed["publication_year"] = coerce_year(parsed.get("publication_year"))

    # Ensure keys exist
    for k in ["title", "author", "publication_place", "publisher", "edition_statement"]:
        if k not in parsed or parsed[k] is None:
            parsed[k] = ""
        else:
            parsed[k] = str(parsed[k]).strip()

    return parsed


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def health():
    return {"status": "ok"}


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

    # 2) OCR
    ocr_text, ocr_conf = run_ocr_document_text(image_bytes)

    # 3) LLM parse
    parsed = llm_parse_title_page(ocr_text)

    # 4) Return combined payload (Airtable script will write fields)
    return {
        "record_id": body.record_id,
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

        "debug": {
            "downloaded_image_bytes": len(image_bytes),
            "collection": body.collection,
            "item_id": body.item_id,
            "model": OPENAI_MODEL,
            "ocr_chars_sent_to_llm": min(len(ocr_text), MAX_OCR_CHARS_FOR_LLM),
        }
    }
