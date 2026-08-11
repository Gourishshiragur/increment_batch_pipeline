"""
Enterprise Pipeline Orchestrator

Flow

--generate
Generate Snapshots
        ↓
Bronze
        ↓
Silver (includes reconciliation record write -- not a separate stage)
        ↓
Gold
        ↓
Validation

Without --generate

Bronze
 ↓
Silver (includes reconciliation record write -- not a separate stage)
 ↓
Gold
 ↓
Validation
"""

import uuid
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
import shutil
from utils.config_loader import (
    get_config,
    get_paths,
    get_environment,
    get_metadata,
)

ROOT = Path(__file__).resolve().parent


def snapshots_exist(landing_dir: Path) -> bool:
    return any(landing_dir.glob("snapshot_day*.csv"))


def clean_snapshots(landing_dir: Path):
    """
    Remove previously generated snapshots.
    """
    if landing_dir.exists():
        shutil.rmtree(landing_dir)

    landing_dir.mkdir(parents=True, exist_ok=True)


def run_script(
    script: str,
    *args: str,
    run_id: str,
    execution_id: str,
    pass_ids: bool = True,
):

    script_path = ROOT / script

    print("\n" + "=" * 80)
    print(f"Running : {script_path.name}")
    print("=" * 80)

    start = time.time()

    # data/generate_snapshots.py uses argparse (a --day flag), not the
    # positional day/run_id/execution_id convention every pipeline stage
    # script uses -- and it has no concept of run_id/execution_id at all
    # (it's a data-generation utility, not an audited pipeline stage), so
    # pass_ids=False skips appending them for that one.
    extra_args = [run_id, execution_id] if pass_ids else []

    result = subprocess.run(
        [
            sys.executable,
            str(script_path),
            *args,
            *extra_args,
        ],
        cwd=ROOT,
        text=True,
    )

    elapsed = round(time.time() - start, 2)

    if result.returncode not in (0, 10):
        raise RuntimeError(
            f"{script_path.name} failed " f"(Exit Code {result.returncode})"
        )

    return {
        "runtime": elapsed,
        "status": "SKIPPED" if result.returncode == 10 else "SUCCESS",
    }


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate fresh snapshots before pipeline",
    )

    parser.add_argument(
        "--from",
        dest="start_from",
        choices=["bronze", "silver", "gold", "validation"],
        default="bronze",
    )

    parser.add_argument(
        "--only", choices=["generate", "bronze", "silver", "gold", "validation"]
    )

    args = parser.parse_args()

    config = get_config()
    paths = get_paths()
    environment = get_environment()

    is_databricks = environment == "databricks"

    REPORTS_DIR = ROOT / config["reports_directory"]

    LANDING_DIR = ROOT / paths["landing"]

    metadata = get_metadata()

    orchestration = metadata["orchestration"]

    stages = orchestration["stages"]

    execution_order = orchestration["execution_order"]

    execution = []

    if args.only:

        if not stages[args.only]["enabled"]:
            raise RuntimeError(f"Stage '{args.only}' is disabled in metadata.")

        execution.append(
            (
                args.only,
                stages[args.only]["script"],
            )
        )

    else:

        order = execution_order

        generate_required = False

        if args.generate:
            if is_databricks:
                print("\n--generate is not supported in Databricks.")
                print("Please populate the landing path before running the pipeline.\n")
            else:
                print("\n--generate specified. Regenerating snapshots...\n")
                clean_snapshots(LANDING_DIR)
                generate_required = True

        elif is_databricks:
            print("\nRunning in Databricks.")
            print("Skipping sample snapshot generation.")
            print("Expecting input data in the configured landing path.\n")

        elif not snapshots_exist(LANDING_DIR):
            print("\nNo snapshots found.")
            print("Generating snapshots automatically...\n")
            generate_required = True

        else:
            print("\nExisting snapshots detected.")
            print("Skipping snapshot generation.\n")

        if generate_required:

            if stages["generate"]["enabled"]:

                execution.append(
                    (
                        "generate",
                        stages["generate"]["script"],
                    )
                )

        start_index = order.index(args.start_from)

        for stage in order[start_index:]:

            if not stages[stage]["enabled"]:
                continue

            execution.append(
                (
                    stage,
                    stages[stage]["script"],
                )
            )

    REPORTS_DIR.mkdir(exist_ok=True)

    shared_run_id = str(uuid.uuid4())
    shared_execution_id = str(uuid.uuid4())

    pipeline_summary = {
        "status": "PASS",
        "execution_mode": "incremental",
        "generate_snapshots": args.generate,
        "stages": {},
    }

    total_start = time.time()

    try:

        for stage_name, script in execution:

            if stages[stage_name]["requires_snapshot"]:

                snapshot_files = sorted(LANDING_DIR.glob("snapshot_day*.csv"))
                if not snapshot_files:
                    raise RuntimeError(
                        "No snapshot files found in the landing directory."
                    )

                for file in snapshot_files:

                    day = file.stem.replace("snapshot_day", "")

                    result = run_script(
                        script,
                        day,
                        run_id=shared_run_id,
                        execution_id=shared_execution_id,
                    )
                    pipeline_summary["stages"][f"{stage_name}_day{day}"] = {
                        "status": result["status"],
                        "runtime_seconds": result["runtime"],
                    }
            else:

                result = run_script(
                    script,
                    run_id=shared_run_id,
                    execution_id=shared_execution_id,
                    pass_ids=(stage_name != "generate"),
                )

                pipeline_summary["stages"][stage_name] = {
                    "status": result["status"],
                    "runtime_seconds": result["runtime"],
                }

        pipeline_summary["total_runtime_seconds"] = round(
            time.time() - total_start,
            2,
        )

    except Exception as ex:

        pipeline_summary["status"] = "FAILED"
        pipeline_summary["error"] = str(ex)

    with open(
        REPORTS_DIR / "pipeline_execution_report.json", "w", encoding="utf-8"
    ) as f:

        json.dump(pipeline_summary, f, indent=4)

    print("\nPipeline Summary\n")
    print(json.dumps(pipeline_summary, indent=4))


if __name__ == "__main__":
    main()
