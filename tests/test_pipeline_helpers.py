import unittest
import sys
import types


def module(name):
    value = types.ModuleType(name)
    sys.modules[name] = value
    return value


if "PIL" not in sys.modules:
    pil = module("PIL")
    pil.Image = object()
    pil.ExifTags = types.SimpleNamespace(TAGS={})

if "fastapi" not in sys.modules:
    fastapi = module("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code=None, detail=None):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class FastAPI:
        def __init__(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            return lambda func: func

        def post(self, *args, **kwargs):
            return lambda func: func

    fastapi.FastAPI = FastAPI
    fastapi.HTTPException = HTTPException
    fastapi.Request = object
    responses = module("fastapi.responses")
    responses.HTMLResponse = object

if "pydantic" not in sys.modules:
    pydantic = module("pydantic")

    class BaseModel:
        def __init__(self, **kwargs):
            for key, value in self.__class__.__dict__.items():
                if not key.startswith("_") and not callable(value):
                    setattr(self, key, value)
            for key, value in kwargs.items():
                setattr(self, key, value)

    pydantic.BaseModel = BaseModel

if "google" not in sys.modules:
    google = module("google")
    api_core = module("google.api_core")
    exceptions = module("google.api_core.exceptions")

    class NotFound(Exception):
        pass

    class PreconditionFailed(Exception):
        pass

    exceptions.NotFound = NotFound
    exceptions.PreconditionFailed = PreconditionFailed
    cloud = module("google.cloud")
    storage = module("google.cloud.storage")
    vision = module("google.cloud.vision")
    oauth2 = module("google.oauth2")
    service_account = module("google.oauth2.service_account")

    class Client:
        pass

    class Credentials:
        @classmethod
        def from_service_account_info(cls, _info):
            return cls()

    storage.Client = Client
    vision.ImageAnnotatorClient = object
    vision.Image = object
    service_account.Credentials = Credentials
    google.cloud = cloud
    cloud.storage = storage
    cloud.vision = vision
    google.oauth2 = oauth2
    oauth2.service_account = service_account

if "openai" not in sys.modules:
    openai = module("openai")
    openai.OpenAI = object

if "requests" not in sys.modules:
    requests = module("requests")
    requests.get = None
    requests.post = None
    requests.patch = None

import app
import airtable_importer
import batch_processor


class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name

    def exists(self):
        return self.name in self.bucket.objects

    def delete(self):
        self.bucket.objects.discard(self.name)


class FakeBucket:
    def __init__(self, objects=None):
        self.objects = set(objects or [])
        self.copied = []

    def blob(self, name):
        return FakeBlob(self, name)

    def copy_blob(self, source_blob, _bucket, destination_name):
        self.copied.append((source_blob.name, destination_name))
        self.objects.add(destination_name)
        return self.blob(destination_name)


class BatchProcessorPathTests(unittest.TestCase):
    def setUp(self):
        self.original_input_prefix = batch_processor.INPUT_PREFIX
        self.original_import_batch_id = batch_processor.IMPORT_BATCH_ID
        self.original_bucket = batch_processor.bucket

    def tearDown(self):
        batch_processor.INPUT_PREFIX = self.original_input_prefix
        batch_processor.IMPORT_BATCH_ID = self.original_import_batch_id
        batch_processor.bucket = self.original_bucket

    def test_destination_path_is_batch_scoped_and_preserves_relative_path(self):
        batch_processor.INPUT_PREFIX = "imports/batch-1/to_process/"
        batch_processor.IMPORT_BATCH_ID = "batch-1"

        path = batch_processor.destination_object_path(
            "imports/batch-1/to_process/subdir/IMG_0001.jpg",
            "processed/",
        )

        self.assertEqual(path, "processed/batch-1/subdir/IMG_0001.jpg")

    def test_relative_input_path_drops_parent_segments(self):
        batch_processor.INPUT_PREFIX = "to_process/"

        path = batch_processor.relative_input_path("to_process/../IMG_0001.jpg")

        self.assertEqual(path, "IMG_0001.jpg")

    def test_copy_blob_to_destination_does_not_overwrite_existing_destination(self):
        batch_processor.INPUT_PREFIX = "imports/batch-1/to_process/"
        batch_processor.IMPORT_BATCH_ID = "batch-1"
        batch_processor.bucket = FakeBucket({
            "imports/batch-1/to_process/IMG_0001.jpg",
            "processed/batch-1/IMG_0001.jpg",
        })
        source_blob = batch_processor.bucket.blob("imports/batch-1/to_process/IMG_0001.jpg")

        path = batch_processor.copy_blob_to_destination(source_blob, "processed/")

        self.assertEqual(path, "processed/batch-1/IMG_0001.jpg")
        self.assertEqual(batch_processor.bucket.copied, [])


class AirtablePathTests(unittest.TestCase):
    def test_clean_gcs_object_path_prefers_final_gcs_path(self):
        row = {
            "final_gcs_path": "processed/batch-1/IMG_0001.jpg",
            "Original filename": "IMG_0001.jpg",
        }

        self.assertEqual(
            airtable_importer.clean_gcs_object_path(row),
            "processed/batch-1/IMG_0001.jpg",
        )

    def test_existing_path_for_gcs_path_accepts_legacy_process_prefix(self):
        existing = {"processed/IMG_0001.jpg"}

        self.assertEqual(
            airtable_importer.existing_path_for_gcs_path("process/IMG_0001.jpg", existing),
            "processed/IMG_0001.jpg",
        )


class BatchMetadataTests(unittest.TestCase):
    def setUp(self):
        self.original_manifest_cache = airtable_importer.batch_manifest_cache
        self.original_batch_target_collection = airtable_importer.BATCH_TARGET_COLLECTION
        self.original_batch_location = airtable_importer.BATCH_LOCATION

    def tearDown(self):
        airtable_importer.batch_manifest_cache = self.original_manifest_cache
        airtable_importer.BATCH_TARGET_COLLECTION = self.original_batch_target_collection
        airtable_importer.BATCH_LOCATION = self.original_batch_location

    def test_batch_manifest_keeps_collection_and_location(self):
        body = app.CreateBatchRequest(
            source="web-upload",
            target_collection="Rare Books",
            location="Shelf A",
            notes="Small smoke test",
        )

        manifest = app.batch_manifest("batch-1", body)

        self.assertEqual(manifest["target_collection"], "Rare Books")
        self.assertEqual(manifest["location"], "Shelf A")

    def test_importer_reads_collection_and_location_from_manifest(self):
        airtable_importer.batch_manifest_cache = {
            "target_collection": "Rare Books",
            "location": "Shelf A",
        }
        airtable_importer.BATCH_TARGET_COLLECTION = ""
        airtable_importer.BATCH_LOCATION = ""

        self.assertEqual(
            airtable_importer.batch_metadata_value("target_collection"),
            "Rare Books",
        )
        self.assertEqual(
            airtable_importer.batch_metadata_value("location"),
            "Shelf A",
        )

    def test_importer_env_metadata_overrides_manifest(self):
        airtable_importer.batch_manifest_cache = {"target_collection": "Rare Books"}

        self.assertEqual(
            airtable_importer.batch_metadata_value("target_collection", "Archive"),
            "Archive",
        )


class LookupOptionsTests(unittest.TestCase):
    def test_lookup_display_name_prefers_configured_field(self):
        name, field = app.airtable_lookup_display_name(
            {"Collection name": "Rare Books", "Other": "Fallback"},
            "Collection name",
        )

        self.assertEqual(name, "Rare Books")
        self.assertEqual(field, "Collection name")

    def test_lookup_display_name_falls_back_to_visible_scalar_field(self):
        name, field = app.airtable_lookup_display_name(
            {"Name": "Rare Books", "Notes": "Do not use"},
            "Collection name",
        )

        self.assertEqual(name, "Rare Books")
        self.assertEqual(field, "Name")


class BatchVerificationTests(unittest.TestCase):
    def test_verification_checks_are_ok_when_counts_match(self):
        checks = app.build_batch_verification_checks(
            {"status": "succeeded"},
            {"exists": True, "total": 2, "success": 2, "failed": 0},
            0,
            0,
            {
                "item_side_linked_count": 2,
                "batch_side_linked_count": 2,
                "items_missing_collection": 0,
                "items_missing_location": 0,
                "warning": "",
            },
            {"target_collection": "Rare Books", "location": "Shelf A"},
        )

        self.assertEqual(app.verification_overall_status(checks), "ok")

    def test_verification_flags_airtable_import_mismatch(self):
        checks = app.build_batch_verification_checks(
            {"status": "succeeded"},
            {"exists": True, "total": 2, "success": 2, "failed": 0},
            0,
            0,
            {
                "item_side_linked_count": 1,
                "batch_side_linked_count": 1,
                "items_missing_collection": 0,
                "items_missing_location": 0,
                "warning": "",
            },
            {},
        )

        self.assertEqual(app.verification_overall_status(checks), "error")
        self.assertTrue(any(check["label"] == "Airtable Items" for check in checks))

    def test_verification_flags_remaining_input_files(self):
        checks = app.build_batch_verification_checks(
            {"status": "succeeded"},
            {"exists": True, "total": 1, "success": 1, "failed": 0},
            1,
            0,
            {
                "item_side_linked_count": 1,
                "batch_side_linked_count": 1,
                "items_missing_collection": 0,
                "items_missing_location": 0,
                "warning": "",
            },
            {},
        )

        self.assertEqual(app.verification_overall_status(checks), "warn")


class FailureRetryTests(unittest.TestCase):
    def setUp(self):
        self.original_download_rows = app.download_batch_results_rows

    def tearDown(self):
        app.download_batch_results_rows = self.original_download_rows

    def test_batch_failure_rows_hides_failure_after_later_success(self):
        rows = [
            {
                "source_gcs_path": "imports/batch-1/to_process/IMG_0001.jpg",
                "final_gcs_path": "failed/batch-1/IMG_0001.jpg",
                "Original filename": "IMG_0001.jpg",
                "status": "failed",
                "error": "bad OCR",
            },
            {
                "source_gcs_path": "imports/batch-1/to_process/IMG_0001.jpg",
                "final_gcs_path": "processed/batch-1/IMG_0001.jpg",
                "Original filename": "IMG_0001.jpg",
                "status": "success",
            },
        ]
        app.download_batch_results_rows = lambda _batch_id, _bucket=None: rows

        failures = app.batch_failure_rows("batch-1", FakeBucket())

        self.assertEqual(failures, [])

    def test_retry_batch_failures_moves_failed_object_back_to_batch_input(self):
        rows = [
            {
                "source_gcs_path": "imports/batch-1/to_process/IMG_0002.jpg",
                "final_gcs_path": "failed/batch-1/IMG_0002.jpg",
                "Original filename": "IMG_0002.jpg",
                "status": "failed",
                "error": "temporary extractor error",
            },
        ]
        app.download_batch_results_rows = lambda _batch_id, _bucket=None: rows
        bucket = FakeBucket({"failed/batch-1/IMG_0002.jpg"})

        result = app.retry_batch_failures("batch-1", 25, bucket)

        self.assertEqual(result["summary"]["queued"], 1)
        self.assertIn("imports/batch-1/to_process/IMG_0002.jpg", bucket.objects)
        self.assertNotIn("failed/batch-1/IMG_0002.jpg", bucket.objects)
        self.assertEqual(
            bucket.copied,
            [("failed/batch-1/IMG_0002.jpg", "imports/batch-1/to_process/IMG_0002.jpg")],
        )

    def test_sanitize_error_message_removes_signed_url_query(self):
        error = (
            "Timeout while downloading "
            "https://storage.googleapis.com/rb-title-pages-2026/to_process/IMG.jpg?"
            "X-Goog-Algorithm=abc&X-Goog-Signature=secret"
        )

        clean = app.sanitize_error_message(error)

        self.assertIn("gs://rb-title-pages-2026/to_process/IMG.jpg", clean)
        self.assertNotIn("X-Goog-Signature", clean)


if __name__ == "__main__":
    unittest.main()
