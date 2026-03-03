import os
import json
from typing import Optional, Dict, Any, List

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from google.cloud import vision
from google.oauth2 import service_account


# -----------------------------
# Config
# -----------------------------
API_KEY = os.getenv("API_KEY", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SECONDS", "60"))


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="RB Extractor", version="0.2.0")


# -----------------------------
# Models
# -----------------------------
class ExtractRequest(BaseModel):
    record_id: str
    image_url: str
    item_id: Optional[str] = None
    collection: Optional[str] = None


class ExtractResponse(BaseModel):
    record_id: str
    ocr_text: str
    ocr_confidence: float
    llm_confidence: float
    language: str
    title: str
    author: str
    publication_place: str
    publisher: str
    publication_year: Optional[int]
    edition_statement: str
    debug: Dict[str, Any]


# -----------------------------
# Auth helper
# -----------------------------
def require_bearer_auth(req: Request):
    """
    Expects: Authorization: Bearer <API_KEY>
    """
    if not API_KEY:
        # Hard fail if you forgot to set it in Render
        raise HTTPException(status_code=500, detail="API_KEY env var not set on server")

    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = auth.split(" ", 1)[1].strip()
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid token")


# -----------------------------
# Google Vision client + OCR
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


def run_ocr_document_text(image_bytes: bytes) -> (str, float):
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
                        # word.confidence is float 0..1
                        confidences.append(float(word.confidence))
    except Exception:
        # If confidence extraction fails, don't fail OCR
        confidences = []

    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    return full_text, float(avg_conf)


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/extract", response_model=ExtractResponse)
async def extract(req: Request, body: ExtractRequest):
    # Auth
    require_bearer_auth(req)

    # Download image bytes from Airtable attachment URL
    try:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, follow_redirects=True) as client:
            r = await client.get(body.image_url)
            r.raise_for_status()
            image_bytes = r.content
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to download image: {str(e)}")

    # OCR
    ocr_text, ocr_conf = run_ocr_document_text(image_bytes)

    # For now, LLM parsing not implemented (next step)
    resp = ExtractResponse(
        record_id=body.record_id,
        ocr_text=ocr_text,
        ocr_confidence=ocr_conf,
        llm_confidence=0.0,
        language="Other/Unknown",
        title="",
        author="",
        publication_place="",
        publisher="",
        publication_year=None,
        edition_statement="",
        debug={
            "downloaded_image_bytes": len(image_bytes),
            "collection": body.collection,
            "item_id": body.item_id,
        },
    )

    return resp
