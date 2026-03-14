#!/usr/bin/env python3
"""Fetch prices for all VICTORY branches using get_branch_prices.py."""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from chain_logging import configure_chain_logger, get_log_paths


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_store_id(value: str) -> str:
    value = value.strip()
    if value.isdigit():
        return str(int(value))
    return value


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Fetch prices for all VICTORY branches")
    parser.add_argument("--branches-file", default=str(script_dir / "branches.json"), help="Path to branches.json")
    parser.add_argument("--links-map", default=str(script_dir / "links-map.json"), help="Path to links-map.json")
    parser.add_argument("--mode", default="full", choices=["full", "refresh"], help="full=PriceFull, refresh=Price")
    parser.add_argument("--output-dir", default=str(script_dir / "prices"), help="Output base directory")
    parser.add_argument("--max-workers", type=int, default=4, help="Parallel workers")
    parser.add_argument("--max-branches", type=int, default=None, help="Optional limit for testing")
    parser.add_argument("--timeout", type=int, default=30, help="Per-branch request timeout in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Per-branch retries")
    parser.add_argument("--python-bin", default=sys.executable, help="Python executable for subprocess calls")
    parser.add_argument("--refresh-links-map", action="store_true", help="Run scrape_links.py before fetching prices")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue even if a branch fails")
    parser.add_argument(
        "--skip-missing-price-files",
        action="store_true",
        default=True,
        help="Treat 'No price file found for store' as skipped instead of failed",
    )
    parser.add_argument("--debug", action="store_true", help="Verbose logging")
    return parser.parse_args()


def is_missing_price_file_result(result: Dict[str, object]) -> bool:
    blob = f"{result.get('stdout', '')}\n{result.get('stderr', '')}".lower()
    return (
        "no price file found for store_id=" in blob
        or "parsed zero price records for selected store" in blob
    )


def load_store_ids(branches_file: Path) -> List[str]:
    if not branches_file.exists():
        raise FileNotFoundError(f"Branches file not found: {branches_file}")

    payload = json.loads(branches_file.read_text(encoding="utf-8"))
    records = payload.get("records")
    if not isinstance(records, list):
        raise RuntimeError("Invalid branches file: missing records array")

    store_ids: List[str] = []
    seen = set()
    for row in records:
        if not isinstance(row, dict):
            continue
        raw_store_id = row.get("store_id")
        if raw_store_id is None:
            continue
        store_id = clean_store_id(str(raw_store_id))
        if not store_id:
            continue
        if store_id in seen:
            continue
        seen.add(store_id)
        store_ids.append(store_id)

    if not store_ids:
        raise RuntimeError("No store_ids found in branches file")

    return store_ids


def run_branch_job(
    python_bin: str,
    script_path: Path,
    links_map: Path,
    output_dir: Path,
    store_id: str,
    mode: str,
    timeout: int,
    retries: int,
    debug: bool,
    logger: logging.Logger,
) -> Dict[str, object]:
    output_file = output_dir / f"{store_id}.json"

    cmd = [
        python_bin,
        str(script_path),
        "--store-id",
        store_id,
        "--mode",
        mode,
        "--links-map",
        str(links_map),
        "--output",
        str(output_file),
        "--timeout",
        str(timeout),
        "--retries",
        str(retries),
    ]
    if debug:
        cmd.append("--debug")

    started_at = iso_now()
    t0 = time.time()
    completed = subprocess.run(cmd, capture_output=True, text=True)
    duration = round(time.time() - t0, 3)

    if completed.returncode != 0:
        logger.error("Branch job failed for store_id=%s mode=%s rc=%s", store_id, mode, completed.returncode)
    elif debug:
        logger.debug("Branch job completed for store_id=%s mode=%s in %.3fs", store_id, mode, duration)

    return {
        "store_id": store_id,
        "started_at": started_at,
        "finished_at": iso_now(),
        "duration_sec": duration,
        "output_file": str(output_file),
        "return_code": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "status": "ok" if completed.returncode == 0 else "failed",
    }


def main() -> int:
    args = parse_args()
    logger = configure_chain_logger("get_all_branches_prices", debug=args.debug)
    chain_log, script_log = get_log_paths("get_all_branches_prices")

    try:
        logger.info("Starting get_all_branches_prices mode=%s", args.mode)

        script_dir = Path(__file__).resolve().parent
        branch_script_path = script_dir / "get_branch_prices.py"
        scrape_links_script_path = script_dir / "scrape_links.py"
        if not branch_script_path.exists():
            raise FileNotFoundError(f"Missing branch script: {branch_script_path}")

        branches_file = Path(args.branches_file)
        links_map = Path(args.links_map)
        output_dir = Path(args.output_dir) / args.mode
        output_dir.mkdir(parents=True, exist_ok=True)

        if args.refresh_links_map:
            scrape_cmd = [args.python_bin, str(scrape_links_script_path), "--output", str(links_map)]
            if args.debug:
                scrape_cmd.append("--debug")
            logger.info("Refreshing links-map before branch fetches")
            scrape_completed = subprocess.run(scrape_cmd)
            if scrape_completed.returncode != 0:
                raise RuntimeError("Failed to refresh links-map via scrape_links.py")

        store_ids = load_store_ids(branches_file)
        if args.max_branches is not None:
            store_ids = store_ids[: max(0, args.max_branches)]

        logger.info("mode=%s stores=%s workers=%s", args.mode, len(store_ids), args.max_workers)

        started_at = iso_now()
        t0 = time.time()

        results: List[Dict[str, object]] = []
        failures: List[Dict[str, object]] = []
        skipped: List[Dict[str, object]] = []

        with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
            futures = {
                executor.submit(
                    run_branch_job,
                    args.python_bin,
                    branch_script_path,
                    links_map,
                    output_dir,
                    store_id,
                    args.mode,
                    args.timeout,
                    args.retries,
                    args.debug,
                    logger,
                ): store_id
                for store_id in store_ids
            }

            completed_count = 0
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                completed_count += 1

                if result["status"] != "ok":
                    if args.skip_missing_price_files and is_missing_price_file_result(result):
                        result["status"] = "skipped"
                        skipped.append(result)
                        logger.warning(
                            "Skipping store_id=%s: no price file currently available in links-map",
                            result.get("store_id"),
                        )
                    else:
                        failures.append(result)
                        if not args.continue_on_error:
                            for pending in futures:
                                pending.cancel()
                            break

                if args.debug and (completed_count % 20 == 0 or completed_count == len(store_ids)):
                    logger.info("Completed %s/%s", completed_count, len(store_ids))

        total_duration = round(time.time() - t0, 3)
        successes = [r for r in results if r["status"] == "ok"]
        skipped_count = len(skipped)

        summary = {
            "schema_version": 1,
            "chain_name": "VICTORY",
            "mode": args.mode,
            "started_at": started_at,
            "finished_at": iso_now(),
            "duration_sec": total_duration,
            "branches_file": str(branches_file),
            "links_map": str(links_map),
            "output_dir": str(output_dir),
            "total_requested": len(store_ids),
            "total_completed": len(results),
            "success_count": len(successes),
            "skipped_count": skipped_count,
            "failure_count": len(failures),
            "skipped": skipped,
            "failures": failures,
            "results": results,
        }

        summary_file = output_dir / f"run-summary-{args.mode}.json"
        summary_file.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")

        logger.info(
            "Completed %s/%s branches, success=%s, skipped=%s, failed=%s. Summary: %s",
            len(results),
            len(store_ids),
            len(successes),
            skipped_count,
            len(failures),
            summary_file,
        )
        logger.info("Chain log: %s | Script log: %s", chain_log, script_log)

        if failures:
            return 1
        return 0
    except Exception:
        logger.exception("get_all_branches_prices failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
