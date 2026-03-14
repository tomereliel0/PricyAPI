#!/usr/bin/env python3
"""Scrape FRESHMARKET Cerberus file listings and build links-map.json."""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import logging
import re
import ssl
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse
from urllib.request import HTTPCookieProcessor, HTTPSHandler, OpenerDirector, Request, build_opener as urllib_build_opener

from chain_logging import configure_chain_logger, get_log_paths

LOGIN_URL = "https://url.publishedprices.co.il/login"
FILES_URL = "https://url.publishedprices.co.il/file"
USERNAME = "freshmarket"
USER_AGENT = "PricyAPI/0.1 (+https://github.com/)"
FILE_PATH_RE = re.compile(r"/file/d/([^/?#]+)")
FILE_JSON_DIR_URL = "https://url.publishedprices.co.il/file/json/dir"
FILE_NAME_RE = re.compile(
    r"^(?P<prefix>[A-Za-z]+)(?P<chain_id>\d+)-(?:((?P<sub_chain_id>\d{3})-))?(?P<branch_id>\d{3})-(?P<ts>\d{8}(?:-\d{6})?|\d{12})(?:\.(?P<ext>[A-Za-z0-9]+))?$"
)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def effective_timestamp(item: Dict[str, object]) -> str:
    return str(item.get("published_at_iso") or item.get("file_timestamp_iso") or "")


def parse_file_timestamp(file_name: str) -> Optional[str]:
    match = FILE_NAME_RE.match(file_name)
    if not match:
        return None
    raw_ts = match.group("ts")
    for fmt in ("%Y%m%d-%H%M%S", "%Y%m%d%H%M"):
        try:
            return datetime.strptime(raw_ts, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return None


def parse_cerberus_updated_at(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None
    for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(raw_value, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return None


class LoginPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.form_action: Optional[str] = None
        self.csrf_token: Optional[str] = None
        self.hidden_fields: Dict[str, str] = {}
        self.username_field: Optional[str] = None
        self.password_field: Optional[str] = None

        self._in_form = False

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        attrs_dict = dict(attrs)

        if tag == "meta":
            meta_name = (attrs_dict.get("name") or "").strip().lower()
            if meta_name == "csrftoken":
                token = (attrs_dict.get("content") or "").strip()
                if token:
                    self.csrf_token = token

        if tag == "form" and not self._in_form:
            self._in_form = True
            self.form_action = attrs_dict.get("action")
            return

        if tag != "input" or not self._in_form:
            return

        name = (attrs_dict.get("name") or "").strip()
        input_type = (attrs_dict.get("type") or "text").strip().lower()
        value = attrs_dict.get("value") or ""

        if not name:
            return

        if input_type == "hidden":
            self.hidden_fields[name] = value
            return

        lowered = name.lower()
        if input_type in {"text", "email"} and self.username_field is None:
            if "user" in lowered or "login" in lowered or "email" in lowered:
                self.username_field = name
        if input_type == "password" and self.password_field is None:
            self.password_field = name

        if self.username_field is None and input_type in {"text", "email"}:
            self.username_field = name
        if self.password_field is None and "pass" in lowered:
            self.password_field = name

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        if tag == "form" and self._in_form:
            self._in_form = False


class FilesPageParser(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.download_urls: Set[str] = set()
        self.page_urls: Set[str] = set()

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href")
        if not href:
            return

        absolute = urljoin(self.page_url, href)
        parsed = urlparse(absolute)

        if "/file/d/" in parsed.path:
            self.download_urls.add(absolute)
            return

        if parsed.path.rstrip("/") == "/file":
            query = parse_qs(parsed.query)
            page = query.get("page", [None])[0]
            if page is not None and str(page).isdigit():
                self.page_urls.add(absolute)


def build_opener_with_cookies(insecure: bool = False) -> OpenerDirector:
    jar = http.cookiejar.CookieJar()
    handlers = [HTTPCookieProcessor(jar)]
    if insecure:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        handlers.append(HTTPSHandler(context=context))
    return urllib_build_opener(*handlers)


def fetch_text(opener: OpenerDirector, url: str, timeout: int, retries: int, logger: logging.Logger) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        request = Request(url=url, headers={"User-Agent": USER_AGENT})
        try:
            with opener.open(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            last_error = error
            logger.warning("Retry %s/%s fetch failed for %s: %s", attempt, retries, url, error)
            if attempt < retries:
                time.sleep(min(1.5 * attempt, 5.0))
    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def post_form(
    opener: OpenerDirector,
    url: str,
    payload: Dict[str, str],
    timeout: int,
    retries: int,
    logger: logging.Logger,
) -> str:
    encoded = urlencode(payload).encode("utf-8")
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        request = Request(
            url=url,
            data=encoded,
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        try:
            with opener.open(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            last_error = error
            logger.warning("Retry %s/%s login POST failed for %s: %s", attempt, retries, url, error)
            if attempt < retries:
                time.sleep(min(1.5 * attempt, 5.0))
    raise RuntimeError(f"Failed login POST to {url}: {last_error}")


def login(opener: OpenerDirector, timeout: int, retries: int, logger: logging.Logger) -> str:
    login_html = fetch_text(opener, LOGIN_URL, timeout=timeout, retries=retries, logger=logger)
    parser = LoginPageParser()
    parser.feed(login_html)

    user_field = parser.username_field or "username"
    pass_field = parser.password_field or "password"

    payload = dict(parser.hidden_fields)
    if parser.csrf_token:
        payload["csrftoken"] = parser.csrf_token
    payload[user_field] = USERNAME
    payload[pass_field] = ""

    action_url = urljoin(LOGIN_URL, parser.form_action or LOGIN_URL)
    post_form(opener, action_url, payload, timeout=timeout, retries=retries, logger=logger)

    probe = fetch_text(opener, FILES_URL, timeout=timeout, retries=retries, logger=logger)
    if "id=\"login-form\"" in probe or "action=\"/login/user\"" in probe:
        raise RuntimeError("Login did not complete successfully (received login page after auth POST)")

    probe_parser = LoginPageParser()
    probe_parser.feed(probe)
    csrf_token = probe_parser.csrf_token or parser.csrf_token
    if not csrf_token:
        raise RuntimeError("Failed to resolve CSRF token after login")

    if "/file/d/" not in probe:
        logger.warning("No direct file links found on initial /file response; continuing with page crawl")

    return csrf_token


def post_text(
    opener: OpenerDirector,
    url: str,
    payload: Dict[str, str],
    timeout: int,
    retries: int,
    logger: logging.Logger,
) -> str:
    encoded = urlencode(payload).encode("utf-8")
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        request = Request(
            url=url,
            data=encoded,
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        try:
            with opener.open(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            last_error = error
            logger.warning("Retry %s/%s POST failed for %s: %s", attempt, retries, url, error)
            if attempt < retries:
                time.sleep(min(1.5 * attempt, 5.0))
    raise RuntimeError(f"Failed POST to {url}: {last_error}")


def extract_file_name(download_url: str) -> Optional[str]:
    parsed = urlparse(download_url)
    match = FILE_PATH_RE.search(parsed.path)
    if not match:
        return None
    return match.group(1)


def normalize_link(download_url: str, page_url: str) -> Optional[Dict[str, object]]:
    file_name = extract_file_name(download_url)
    if not file_name:
        return None

    file_match = FILE_NAME_RE.match(file_name)
    prefix = file_match.group("prefix") if file_match else None
    chain_id = file_match.group("chain_id") if file_match else None
    sub_chain_id = file_match.group("sub_chain_id") if file_match else None
    branch_id = file_match.group("branch_id") if file_match else None
    file_ext = (file_match.group("ext") if file_match else None) or file_name.split(".")[-1].lower()
    file_type = (prefix or "unknown").lower()

    return {
        "download_url": download_url,
        "file_name": file_name,
        "file_ext": file_ext.lower(),
        "file_type": file_type,
        "file_prefix": prefix,
        "chain_id": chain_id,
        "sub_chain_id": sub_chain_id,
        "branch_id": branch_id,
        "branch_label": branch_id or "",
        "branch_name": None,
        "size": None,
        "published_at": None,
        "published_at_iso": None,
        "file_timestamp_iso": parse_file_timestamp(file_name),
        "row_number": None,
        "source_page": page_url,
    }


def normalize_json_row(row: Dict[str, object], source_page: str) -> Optional[Dict[str, object]]:
    file_name = str(row.get("name") or row.get("fname") or row.get("value") or "").strip()
    if not file_name:
        return None

    quoted = quote(file_name, safe="-_.~")
    download_url = f"{FILES_URL}/d/{quoted}"

    normalized = normalize_link(download_url, page_url=source_page)
    if not normalized:
        return None

    size_value = row.get("size")
    size_int: Optional[int] = None
    if isinstance(size_value, (int, float)):
        size_int = int(size_value)
    elif isinstance(size_value, str) and size_value.strip().isdigit():
        size_int = int(size_value.strip())

    published_at = str(row.get("ftime") or "").strip() or None
    published_at_iso = str(row.get("time") or "").strip() or parse_cerberus_updated_at(published_at)

    normalized["size"] = size_int
    normalized["published_at"] = published_at
    normalized["published_at_iso"] = published_at_iso
    normalized["source_page"] = source_page
    return normalized


def crawl_files_via_json_dir(
    opener: OpenerDirector,
    csrf_token: str,
    timeout: int,
    retries: int,
    logger: logging.Logger,
) -> List[Dict[str, object]]:
    page_size = 500
    offset = 0
    total_records: Optional[int] = None
    normalized: Dict[str, Dict[str, object]] = {}

    while total_records is None or offset < total_records:
        payload = {
            "cd": "/",
            "csrftoken": csrf_token,
            "sEcho": "1",
            "iDisplayStart": str(offset),
            "iDisplayLength": str(page_size),
        }
        body = post_text(opener, FILE_JSON_DIR_URL, payload, timeout=timeout, retries=retries, logger=logger)
        response = json.loads(body)

        error = response.get("error")
        if error:
            raise RuntimeError(f"Cerberus dir API error: {error}")

        rows = response.get("aaData")
        if not isinstance(rows, list):
            rows = []

        raw_total = response.get("iTotalRecords") or response.get("iTotalDisplayRecords") or 0
        try:
            total_records = int(raw_total)
        except (TypeError, ValueError):
            total_records = len(rows)

        logger.info("Fetched %s rows from dir API (offset=%s, total=%s)", len(rows), offset, total_records)

        source_page = f"json_dir:{offset}"
        for row in rows:
            if not isinstance(row, dict):
                continue
            item = normalize_json_row(row, source_page=source_page)
            if item:
                normalized[item["download_url"]] = item

        if not rows:
            break

        offset += len(rows)

    items = list(normalized.values())
    items.sort(key=lambda i: (effective_timestamp(i), str(i.get("file_name") or "")), reverse=True)
    return items


def crawl_files_page(
    opener: OpenerDirector,
    csrf_token: str,
    start_url: str,
    timeout: int,
    retries: int,
    max_pages: Optional[int],
    sleep_ms: int,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, object]], int]:
    queue: List[str] = [start_url]
    visited: Set[str] = set()
    normalized: Dict[str, Dict[str, object]] = {}

    while queue:
        page_url = queue.pop(0)
        if page_url in visited:
            continue
        visited.add(page_url)
        if max_pages is not None and len(visited) > max_pages:
            break

        logger.info("Scraping page %s", page_url)
        html = fetch_text(opener, page_url, timeout=timeout, retries=retries, logger=logger)
        parser = FilesPageParser(page_url=page_url)
        parser.feed(html)

        for download_url in parser.download_urls:
            item = normalize_link(download_url, page_url=page_url)
            if item:
                normalized[item["download_url"]] = item

        for next_page_url in sorted(parser.page_urls):
            if next_page_url not in visited and next_page_url not in queue:
                queue.append(next_page_url)

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    items = list(normalized.values())
    items.sort(key=lambda i: (effective_timestamp(i), str(i.get("file_name") or "")), reverse=True)

    if not items:
        logger.info("HTML crawl returned no links, trying JSON dir API fallback")
        items = crawl_files_via_json_dir(
            opener=opener,
            csrf_token=csrf_token,
            timeout=timeout,
            retries=retries,
            logger=logger,
        )

    return items, len(visited)


def build_links_map(all_files: List[Dict[str, object]], pages_crawled: int) -> Dict[str, object]:
    by_type: Dict[str, Dict[str, object]] = {}

    for file_info in all_files:
        file_type = str(file_info.get("file_type") or "unknown")
        bucket = by_type.setdefault(file_type, {"files": []})
        bucket["files"].append(file_info)

    for bucket in by_type.values():
        files = bucket["files"]
        files.sort(key=lambda i: (effective_timestamp(i), str(i.get("file_name") or "")), reverse=True)
        bucket["count"] = len(files)
        bucket["latest_file"] = files[0] if files else None

        latest_by_branch: Dict[str, Dict[str, object]] = {}
        for item in files:
            branch_id = item.get("branch_id")
            if not branch_id:
                continue
            branch_id_str = str(branch_id)
            if branch_id_str not in latest_by_branch:
                latest_by_branch[branch_id_str] = item
        bucket["latest_by_branch"] = latest_by_branch

    return {
        "schema_version": 1,
        "chain_name": "FRESHMARKET",
        "source_url": FILES_URL,
        "generated_at": iso_now(),
        "max_page_discovered": pages_crawled,
        "pages_crawled": pages_crawled,
        "total_files": len(all_files),
        "all_files": all_files,
        "by_type": by_type,
    }


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Scrape FRESHMARKET file links map")
    parser.add_argument("--output", default=str(script_dir / "links-map.json"), help="Output JSON path")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional max pages to crawl")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Retries per request")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Delay between page requests in milliseconds")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS certificate verification (debug only)")
    parser.add_argument("--debug", action="store_true", help="Verbose logging")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = configure_chain_logger("scrape_links", debug=args.debug)
    chain_log, script_log = get_log_paths("scrape_links")

    try:
        logger.info("Starting scrape_links run")
        opener = build_opener_with_cookies(insecure=args.insecure)
        if args.insecure:
            logger.warning("TLS certificate verification is disabled (--insecure)")
        csrf_token = login(opener, timeout=args.timeout, retries=args.retries, logger=logger)

        files, pages_crawled = crawl_files_page(
            opener=opener,
            csrf_token=csrf_token,
            start_url=FILES_URL,
            timeout=args.timeout,
            retries=args.retries,
            max_pages=args.max_pages,
            sleep_ms=args.sleep_ms,
            logger=logger,
        )
        if not files:
            raise RuntimeError("No downloadable files found after login")

        links_map = build_links_map(files, pages_crawled=max(1, pages_crawled))

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(links_map, ensure_ascii=True, indent=2), encoding="utf-8")

        logger.info("Wrote %s files to %s", links_map["total_files"], output_path)
        logger.info("Chain log: %s | Script log: %s", chain_log, script_log)
        return 0
    except Exception:
        logger.exception("scrape_links failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
