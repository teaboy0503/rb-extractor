# Rare Books Pipeline – System Context

## Overview
This system processes rare book title page images through:

1. Extractor API (FastAPI)
2. Batch processor (GCS → extractor → CSV)
3. Airtable importer (CSV → Airtable)
4. Future: enrichment pipeline

## Key Files
- app.py → extraction API (OCR + LLM)
- batch_processor.py → runs extraction jobs
- airtable_importer.py → uploads results to Airtable

## Current Features
- OCR via Google Vision
- LLM extraction + verification
- Confidence scoring (OCR + LLM)
- Quality flags JSON
- GCS storage pipeline
- Airtable sync with deduplication

## Constraints
- DO NOT change field names without explicit instruction
- CSV schema must remain backward compatible
- Airtable schema must not break existing imports

## Next Objectives
- Improve data quality flags
- Add enrichment pipeline (LLM-based)
- Prepare for Supabase migration

## Style Rules
- Minimal breaking changes
- Always preserve existing outputs
- Add new fields instead of modifying old ones
