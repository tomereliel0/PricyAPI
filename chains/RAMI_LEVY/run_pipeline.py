#!/usr/bin/env python3
"""Run RAMI_LEVY pipeline: optional scrape links -> branches -> all branch prices."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import List

from chain_logging import configure_chain_logger, get_log_paths


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(description="Run RAMI_LEVY full pipeline")
    parser.add_argument("--mode", default="full", choices=["full", "refresh"], help="Pricing mode for all branches")
    parser.add_argument("--python-bin", default=sys.executable, help="Python executable")
    parser.add_argument("--max-workers", type=int, default=8, help="Workers for all-branches step")
    parser.add_argument("--max-branches", type=int, default=None, help="Optional branch limit for testing")
    parser.add_argument("--timeout", type=int, default=5, help="HTTP timeout for branch price fetches")
    parser.add_argument("--retries", type=int, default=3, help="Retries for each branch fetch")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS certificate verification (debug only)")
    parser.add_argument(
        "--scrape-links",
        action="store_true",
        help="Run scrape_links.py before downstream steps (default: skipped)",
    )
    parser.add_argument("--scrape-max-pages", type=int, default=None, help="Optional page cap for scrape_links.py")
    parser.add_argument("--debug", action="store_true", help="Verbose child scripts")

    parser.add_argument("--links-map", default=str(script_dir / "links-map.json"), help="Path to links-map.json")
    parser.add_argument("--branches-file", default=str(script_dir / "branches.json"), help="Path to branches.json")
    parser.add_argument("--output-dir", default=str(script_dir / "prices"), help="Output directory for prices")

    return parser.parse_args()


def run_step(cmd: List[str], name: str, logger: logging.Logger) -> None:
    logger.info("Step %s: %s", name, " ".join(cmd))
    completed = subprocess.run(cmd)
    if completed.returncode != 0:
        logger.error("Step failed: %s with code %s", name, completed.returncode)
        raise SystemExit(f"Step failed ({name}) with code {completed.returncode}")


def main() -> int:
    args = parse_args()
    logger = configure_chain_logger("run_pipeline", debug=args.debug)
    chain_log, script_log = get_log_paths("run_pipeline")
    script_dir = Path(__file__).resolve().parent

    try:
        logger.info("Starting run_pipeline mode=%s", args.mode)
        if args.scrape_links:
            scrape_cmd = [args.python_bin, str(script_dir / "scrape_links.py"), "--output", args.links_map]
            if args.scrape_max_pages is not None:
                scrape_cmd.extend(["--max-pages", str(args.scrape_max_pages)])
            if args.debug:
                scrape_cmd.append("--debug")
            if args.insecure:
                scrape_cmd.append("--insecure")
            run_step(scrape_cmd, "scrape_links", logger)
        else:
            logger.info("Skipping scrape_links step (use --scrape-links to enable)")

        branches_cmd = [
            args.python_bin,
            str(script_dir / "get_branches.py"),
            "--links-map",
            args.links_map,
            "--output",
            args.branches_file,
        ]
        if args.debug:
            branches_cmd.append("--debug")
        if args.insecure:
            branches_cmd.append("--insecure")
        run_step(branches_cmd, "get_branches", logger)

        all_prices_cmd = [
            args.python_bin,
            str(script_dir / "get_all_branches_prices.py"),
            "--branches-file",
            args.branches_file,
            "--links-map",
            args.links_map,
            "--mode",
            args.mode,
            "--output-dir",
            args.output_dir,
            "--max-workers",
            str(args.max_workers),
            "--timeout",
            str(args.timeout),
            "--retries",
            str(args.retries),
        ]
        if args.max_branches is not None:
            all_prices_cmd.extend(["--max-branches", str(args.max_branches)])
        if args.debug:
            all_prices_cmd.append("--debug")
        if args.insecure:
            all_prices_cmd.append("--insecure")
        run_step(all_prices_cmd, "get_all_branches_prices", logger)

        logger.info("Pipeline completed successfully")
        logger.info("Chain log: %s | Script log: %s", chain_log, script_log)
        return 0
    except Exception:
        logger.exception("run_pipeline failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
