#!/usr/bin/env python3
"""Scrape SHUFERSAL transparency index pages and build a normalized links map."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import URLError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

from chain_logging import configure_chain_logger, get_log_paths

BASE_URL = "https://prices.shufersal.co.il/"
USER_AGENT = "PricyAPI/0.1 (+https://github.com/)"
ROW_CLASSES = {"webgrid-row-style", "webgrid-alternating-row"}
FILE_NAME_RE = re.compile(
    r"^(?P<prefix>[A-Za-z]+)(?P<chain_id>\d+)-(?P<branch_id>\d{3})-(?P<ts>\d{12})$"
)
BRANCH_LABEL_RE = re.compile(r"^\s*(?P<branch_id>\d+)\s*-\s*(?P<label>.+?)\s*$")


@dataclass
class RawRow:
    download_url: str
    updated_at: str
    size: str
    file_ext: str
    category: str
    branch_label: str
    file_name: str
    row_number: str
    page: int


class ShufersalPageParser(HTMLParser):
    def __init__(self, base_url: str, page_number: int) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.page_number = page_number
        self.max_page = 1
        self.rows: List[RawRow] = []

        self._in_tbody = False
        self._in_row = False
        self._in_td = False
        self._td_index = -1
        self._td_buffer: List[str] = []
        self._cells: List[str] = []
        self._download_url: Optional[str] = None

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        attrs_dict = dict(attrs)

        if tag == "a":
            href = attrs_dict.get("href", "")
            self._capture_page_from_href(href)

        if tag == "tbody":
            self._in_tbody = True
            return

        if not self._in_tbody:
            return

        if tag == "tr":
            classes = set((attrs_dict.get("class") or "").split())
            if classes.intersection(ROW_CLASSES):
                self._in_row = True
                self._in_td = False
                self._td_index = -1
                self._cells = []
                self._td_buffer = []
                self._download_url = None
            return

        if tag == "td" and self._in_row:
            self._in_td = True
            self._td_index += 1
            self._td_buffer = []
            return

        if tag == "a" and self._in_row and self._in_td and self._td_index == 0:
            href = attrs_dict.get("href")
            if href:
                self._download_url = urljoin(self.base_url, href)

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        if tag == "tbody":
            self._in_tbody = False
            return

        if tag == "td" and self._in_row and self._in_td:
            cell_text = " ".join("".join(self._td_buffer).split())
            self._cells.append(cell_text)
            self._in_td = False
            self._td_buffer = []
            return

        if tag == "tr" and self._in_row:
            self._in_row = False
            if len(self._cells) >= 7 and self._download_url:
                row_number = self._cells[7] if len(self._cells) > 7 else ""
                self.rows.append(
                    RawRow(
                        download_url=self._download_url,
                        updated_at=self._cells[1],
                        size=self._cells[2],
                        file_ext=self._cells[3],
                        category=self._cells[4],
                        branch_label=self._cells[5],
                        file_name=self._cells[6],
                        row_number=row_number,
                        page=self.page_number,
                    )
                )
            self._cells = []
            self._download_url = None

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        if self._in_row and self._in_td:
            self._td_buffer.append(data)

    def _capture_page_from_href(self, href: str) -> None:
        if not href:
            return
        parsed = urlparse(href)
        query = parse_qs(parsed.query)
        page_values = query.get("page")
        if not page_values:
            return
        try:
            page_num = int(page_values[0])
        except ValueError:
            return
        if page_num > self.max_page:
            self.max_page = page_num


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_updated_at(raw_value: str) -> Optional[str]:
    if not raw_value:
        return None
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(raw_value, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return None


def parse_file_timestamp(file_name: str) -> Optional[str]:
    match = FILE_NAME_RE.match(file_name)
    if not match:
        return None
    ts = match.group("ts")
    try:
        return datetime.strptime(ts, "%Y%m%d%H%M").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return None


def normalize_row(row: RawRow) -> Dict[str, object]:
    file_match = FILE_NAME_RE.match(row.file_name)
    branch_match = BRANCH_LABEL_RE.match(row.branch_label)

    file_type = row.category.strip().lower() if row.category else ""
    file_timestamp_iso = parse_file_timestamp(row.file_name)
    published_at_iso = parse_updated_at(row.updated_at)

    chain_id: Optional[str] = None
    branch_id: Optional[str] = None
    file_prefix: Optional[str] = None

    if file_match:
        file_prefix = file_match.group("prefix")
        chain_id = file_match.group("chain_id")
        branch_id = file_match.group("branch_id")
        if not file_type:
            file_type = file_prefix.lower()

    branch_name: Optional[str] = None
    if branch_match:
        branch_id = branch_id or branch_match.group("branch_id")
        branch_name = branch_match.group("label")

    return {
        "download_url": row.download_url,
        "file_name": row.file_name,
        "file_ext": row.file_ext.lower(),
        "file_type": file_type,
        "file_prefix": file_prefix,
        "chain_id": chain_id,
        "branch_id": branch_id,
        "branch_label": row.branch_label,
        "branch_name": branch_name,
        "size": row.size,
        "published_at": row.updated_at,
        "published_at_iso": published_at_iso,
        "file_timestamp_iso": file_timestamp_iso,
        "row_number": row.row_number,
        "source_page": row.page,
    }


def effective_timestamp(item: Dict[str, object]) -> str:
    return str(item.get("published_at_iso") or item.get("file_timestamp_iso") or "")


def get_html(url: str, timeout: int, retries: int, debug: bool, logger: Optional[logging.Logger] = None) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        request = Request(url=url, headers={"User-Agent": USER_AGENT})
        try:
            with urlopen(request, timeout=timeout) as response:
                payload = response.read()
                return payload.decode("utf-8", errors="replace")
        except (URLError, TimeoutError, OSError) as error:
            last_error = error
            if debug:
                if logger:
                    logger.warning("Retry %s/%s fetch failed for %s: %s", attempt, retries, url, error)
                else:
                    print(f"[retry {attempt}/{retries}] fetch failed: {url} ({error})", file=sys.stderr)
            if attempt < retries:
                time.sleep(min(1.5 * attempt, 5.0))
    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def scrape_pages(
    max_pages: Optional[int],
    timeout: int,
    retries: int,
    sleep_ms: int,
    max_workers: int,
    debug: bool,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, object]:
    first_url = BASE_URL
    if logger:
        logger.info("Scraping page 1 (discovering total pages)")
    first_html = get_html(first_url, timeout=timeout, retries=retries, debug=debug, logger=logger)
    parser = ShufersalPageParser(base_url=BASE_URL, page_number=1)
    parser.feed(first_html)

    discovered_max_page = parser.max_page
    target_max_page = discovered_max_page if max_pages is None else min(max_pages, discovered_max_page)

    if logger:
        logger.info("Discovered %s pages, will scrape %s with %s workers", discovered_max_page, target_max_page, max_workers)

    raw_rows: List[RawRow] = list(parser.rows)

    if target_max_page >= 2:
        def fetch_single_page(page_num: int) -> Tuple[int, List[RawRow]]:
            page_url = f"{BASE_URL}?page={page_num}"
            if logger:
                logger.info("Scraping page %s/%s", page_num, target_max_page)
            elif debug:
                print(f"[info] scraping page {page_num}/{target_max_page}", file=sys.stderr)
            html = get_html(page_url, timeout=timeout, retries=retries, debug=debug, logger=logger)
            page_parser = ShufersalPageParser(base_url=BASE_URL, page_number=page_num)
            page_parser.feed(html)
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)
            return page_num, page_parser.rows

        worker_count = max(1, max_workers)
        page_rows: Dict[int, List[RawRow]] = {}
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {executor.submit(fetch_single_page, page_num): page_num for page_num in range(2, target_max_page + 1)}
            for future in as_completed(futures):
                page_num, rows = future.result()
                page_rows[page_num] = rows

        for page_num in range(2, target_max_page + 1):
            raw_rows.extend(page_rows.get(page_num, []))

    dedup: Dict[str, Dict[str, object]] = {}
    for row in raw_rows:
        normalized = normalize_row(row)
        key = f"{normalized['file_name']}|{normalized['download_url']}"
        dedup[key] = normalized

    all_files = list(dedup.values())
    all_files.sort(key=lambda item: (effective_timestamp(item), str(item.get("file_name", ""))), reverse=True)

    by_type: Dict[str, Dict[str, object]] = {}
    for file_info in all_files:
        file_type = str(file_info.get("file_type") or "unknown")
        type_bucket = by_type.setdefault(file_type, {"files": []})
        type_bucket["files"].append(file_info)

    for type_bucket in by_type.values():
        files = type_bucket["files"]
        files.sort(key=lambda item: (effective_timestamp(item), str(item.get("file_name", ""))), reverse=True)
        type_bucket["count"] = len(files)
        type_bucket["latest_file"] = files[0] if files else None

        latest_by_branch: Dict[str, Dict[str, object]] = {}
        for item in files:
            branch_id = item.get("branch_id")
            if not branch_id:
                continue
            branch_id_str = str(branch_id)
            if branch_id_str not in latest_by_branch:
                latest_by_branch[branch_id_str] = item
        type_bucket["latest_by_branch"] = latest_by_branch

    return {
        "schema_version": 1,
        "chain_name": "SHUFERSAL",
        "source_url": BASE_URL,
        "generated_at": iso_now(),
        "max_page_discovered": discovered_max_page,
        "pages_crawled": target_max_page,
        "total_files": len(all_files),
        "all_files": all_files,
        "by_type": by_type,
    }


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Scrape SHUFERSAL file links map")
    parser.add_argument("--output", default=str(script_dir / "links-map.json"), help="Output JSON path")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional max pages to crawl")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Retries per request")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Delay between page requests in milliseconds")
    parser.add_argument("--max-workers", type=int, default=8, help="Parallel workers for page scraping")
    parser.add_argument("--debug", action="store_true", help="Verbose logging to stderr")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = configure_chain_logger("scrape_links", debug=args.debug)
    chain_log, script_log = get_log_paths("scrape_links")

    try:
        logger.info("Starting scrape_links run")
        result = scrape_pages(
            max_pages=args.max_pages,
            timeout=args.timeout,
            retries=args.retries,
            sleep_ms=args.sleep_ms,
            max_workers=args.max_workers,
            debug=args.debug,
            logger=logger,
        )

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=True, indent=2), encoding="utf-8")

        logger.info(
            "Wrote %s files from %s pages to %s",
            result["total_files"],
            result["pages_crawled"],
            output_path,
        )
        logger.info("Chain log: %s | Script log: %s", chain_log, script_log)
        return 0
    except Exception:
        logger.exception("scrape_links failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
