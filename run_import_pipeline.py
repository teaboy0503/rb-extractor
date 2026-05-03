import argparse
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


def batch_id_now():
    return datetime.now(UTC).strftime("batch-%Y%m%dT%H%M%SZ")


def default_results_path(import_batch_id):
    return f"results/batches/{import_batch_id}.csv"


def run_step(label, command, env, cwd):
    print(f"\n== {label} ==")
    subprocess.run(command, cwd=cwd, env=env, check=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run batch extraction and Airtable import as one batch."
    )
    parser.add_argument(
        "--batch-id",
        default=os.getenv("IMPORT_BATCH_ID") or batch_id_now(),
        help="Import batch ID to use for the full run.",
    )
    parser.add_argument(
        "--results-path",
        default=None,
        help="GCS CSV path. Defaults to results/batches/{batch_id}.csv.",
    )
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help="Run extraction only and leave the CSV ready for later import.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    repo_dir = Path(__file__).resolve().parent
    env = os.environ.copy()

    import_batch_id = args.batch_id
    results_path = (
        args.results_path
        or env.get("BATCH_RESULTS_PATH")
        or default_results_path(import_batch_id)
    )

    env["IMPORT_BATCH_ID"] = import_batch_id
    env["BATCH_RESULTS_PATH"] = results_path

    if "MAX_IMPORT_ROWS" not in env and env.get("MAX_FILES"):
        env["MAX_IMPORT_ROWS"] = env["MAX_FILES"]

    if "MAX_FAILURE_IMPORT_ROWS" not in env and env.get("MAX_IMPORT_ROWS"):
        env["MAX_FAILURE_IMPORT_ROWS"] = env["MAX_IMPORT_ROWS"]

    print(f"Import batch ID: {import_batch_id}")
    print(f"Results path: gs://{env.get('BATCH_GCS_BUCKET', 'rb-title-pages-2026')}/{results_path}")

    run_step("Batch processor", [sys.executable, "batch_processor.py"], env, repo_dir)

    if args.skip_import:
        print("\nSkipped Airtable import.")
        return

    run_step("Airtable importer", [sys.executable, "airtable_importer.py"], env, repo_dir)


if __name__ == "__main__":
    main()
