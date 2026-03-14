#!/usr/bin/env python3
"""Scrape VICTORY transparency index pages and build a normalized links map."""

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
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from chain_logging import configure_chain_logger, get_log_paths

BASE_URL = "https://laibcatalog.co.il/"
USER_AGENT = "PricyAPI/0.1 (+https://github.com/)"
FILE_NAME_RE = re.compile(
    r"^(?P<prefix>[A-Za-z]+)(?P<chain_id>\d+)-(?P<branch_id>\d{3})-(?P<ts>\d{12})$"
)
BRANCH_LABEL_RE = re.compile(r"^\s*(?P<branch_id>\d+)\s*-\s*(?P<label>.+?)\s*$")
GENERIC_FILE_RE = re.compile(r"^(?P<prefix>[A-Za-z]+)(?P<chain_id>\d+)-(?P<rest>.+)$")


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


class LaibPageParser(HTMLParser):
    def __init__(self, base_url: str, page_number: int) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.page_number = page_number
        self.max_page = 1
        self.rows: List[RawRow] = []

        self._in_row = False
        self._in_td = False
        self._td_index = -1
        self._td_buffer: List[str] = []
        self._cells: List[str] = []
        self._download_url: Optional[str] = None
        self._row_number = 0

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        attrs_dict = dict(attrs)

        if tag == "tr":
            self._in_row = True
            self._in_td = False
            self._td_index = -1
            self._cells = []
            self._td_buffer = []
            self._download_url = None
            self._row_number += 1
            return

        if tag == "td" and self._in_row:
            self._in_td = True
            self._td_index += 1
            self._td_buffer = []
            return

        if tag == "a" and self._in_row:
            href = attrs_dict.get("href")
            if href and ".xml.gz" in href.lower() and "competitionregulationsfiles" in href.lower():
                normalized_href = href.replace("\\", "/")
                self._download_url = urljoin(self.base_url, normalized_href)

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        if tag == "td" and self._in_row and self._in_td:
            cell_text = " ".join("".join(self._td_buffer).split())
            self._cells.append(cell_text)
            self._in_td = False
            self._td_buffer = []
            return

        if tag == "tr" and self._in_row:
            self._in_row = False
            if len(self._cells) >= 7 and self._download_url:
                file_name = Path(self._download_url).name
                self.rows.append(
                    RawRow(
                        download_url=self._download_url,
                        updated_at=self._cells[6],
                        size=self._cells[5],
                        file_ext=self._cells[4],
                        category=self._cells[3],
                        branch_label=self._cells[2],
                        file_name=file_name,
                        row_number=str(self._row_number),
                        page=self.page_number,
                    )
                )
            self._cells = []
            self._download_url = None

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        if self._in_row and self._in_td:
            self._td_buffer.append(data)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_updated_at(raw_value: str) -> Optional[str]:
    if not raw_value:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%H:%M %d-%m-%Y"):
        try:
            return datetime.strptime(raw_value, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return None


def parse_file_timestamp(file_name: str) -> Optional[str]:
    stem = Path(file_name).name
    if "." in stem:
        stem = stem.split(".", 1)[0]

    tokens = stem.split("-")
    if len(tokens) >= 2 and re.fullmatch(r"\d{12}", tokens[-2]):
        try:
            return datetime.strptime(tokens[-2], "%Y%m%d%H%M").replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            pass

    if len(tokens) >= 2 and re.fullmatch(r"\d{8}", tokens[-2]) and re.fullmatch(r"\d{6}", tokens[-1]):
        try:
            return datetime.strptime(tokens[-2] + tokens[-1], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            pass

    match = FILE_NAME_RE.match(file_name)
    if not match:
        return None
    ts = match.group("ts")
    try:
        return datetime.strptime(ts, "%Y%m%d%H%M").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return None


def extract_file_parts(file_name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    match = FILE_NAME_RE.match(file_name)
    if match:
        return match.group("prefix"), match.group("chain_id"), str(int(match.group("branch_id")))

    stem = Path(file_name).name
    if "." in stem:
        stem = stem.split(".", 1)[0]

    generic_match = GENERIC_FILE_RE.match(stem)
    if not generic_match:
        return None, None, None

    prefix = generic_match.group("prefix")
    chain_id = generic_match.group("chain_id")
    rest = generic_match.group("rest")
    tokens = rest.split("-")

    branch_token: Optional[str] = None
    if len(tokens) >= 3 and re.fullmatch(r"\d{12}", tokens[-2]) and re.fullmatch(r"\d+", tokens[-1]):
        branch_token = tokens[-3]
    if len(tokens) >= 2 and re.fullmatch(r"\d{12}", tokens[-1]):
        branch_token = tokens[-2]
    elif len(tokens) >= 3 and re.fullmatch(r"\d{8}", tokens[-2]) and re.fullmatch(r"\d{6}", tokens[-1]):
        branch_token = tokens[-3]

    branch_id: Optional[str] = None
    if branch_token and branch_token.isdigit():
        branch_id = str(int(branch_token))

    return prefix, chain_id, branch_id


def normalize_row(row: RawRow) -> Dict[str, object]:
    file_prefix, chain_id, branch_id = extract_file_parts(row.file_name)
    branch_match = BRANCH_LABEL_RE.match(row.branch_label)

    file_type = row.category.strip().lower() if row.category else ""
    file_timestamp_iso = parse_file_timestamp(row.file_name)
    published_at_iso = parse_updated_at(row.updated_at)

    if file_prefix:
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
        logger.info("Scraping laibcatalog listing page")
    first_html = get_html(first_url, timeout=timeout, retries=retries, debug=debug, logger=logger)
    parser = LaibPageParser(base_url=BASE_URL, page_number=1)
    parser.feed(first_html)

    discovered_max_page = parser.max_page
    target_max_page = 1

    if logger:
        logger.info("Discovered %s rows from laibcatalog listing", len(parser.rows))

    raw_rows: List[RawRow] = list(parser.rows)

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
        "chain_name": "VICTORY",
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
    parser = argparse.ArgumentParser(description="Scrape VICTORY file links map")
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
