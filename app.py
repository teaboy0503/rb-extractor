from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import Optional
import httpx
import os

app = FastAPI(title="RB Extractor")

class ExtractRequest(BaseModel):
    record_id: str
    image_url: str
    item_id: Optional[str] = None
    collection: Optional[str] = "Race"

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/extract")
async def extract(req: ExtractRequest, authorization: Optional[str] = Header(default=None)):
    expected = os.getenv("API_KEY", "")
    if not expected:
        raise HTTPException(status_code=500, detail="API_KEY not set on server")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    token = authorization.split(" ", 1)[1]
    if token != expected:
        raise HTTPException(status_code=403, detail="Invalid API key")

    # Prove we can fetch the Airtable attachment URL
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(req.image_url)
        r.raise_for_status()
        img_len = len(r.content)

    # Stub response (OCR/LLM comes next)
    return {
        "record_id": req.record_id,
        "ocr_text": "",
        "ocr_confidence": 0.0,
        "llm_confidence": 0.0,
        "language": "Other/Unknown",
        "title": "",
        "author": "",
        "publication_place": "",
        "publisher": "",
        "publication_year": None,
        "edition_statement": "",
        "debug": {
            "downloaded_image_bytes": img_len,
            "item_id": req.item_id,
            "collection": req.collection
        }
    }
