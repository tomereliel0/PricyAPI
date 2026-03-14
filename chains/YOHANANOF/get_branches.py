#!/usr/bin/env python3
"""Build YOHANANOF branches.json from the latest Stores file in links-map.json."""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import re
import ssl
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

from cerberus_auth import create_logged_in_opener
from chain_logging import configure_chain_logger, get_log_paths

USER_AGENT = "PricyAPI/0.1 (+https://github.com/)"
STORES_FILE_RE = re.compile(
	r"^Stores(?P<chain_id>\d+)-(?P<branch_id>\d{3})-(?P<ts>\d{12})$", re.IGNORECASE
)


def iso_now() -> str:
	return datetime.now(timezone.utc).isoformat()


def normalize_key(raw_key: str) -> str:
	return "".join(ch for ch in raw_key.lower() if ch.isalnum())


def parse_ts(file_info: Dict[str, object]) -> str:
	return str(file_info.get("published_at_iso") or file_info.get("file_timestamp_iso") or "")


def parse_args() -> argparse.Namespace:
	script_dir = Path(__file__).resolve().parent
	parser = argparse.ArgumentParser(description="Fetch and normalize YOHANANOF branches")
	parser.add_argument("--links-map", default=str(script_dir / "links-map.json"), help="Path to links-map.json")
	parser.add_argument("--output", default=str(script_dir / "branches.json"), help="Output JSON path")
	parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
	parser.add_argument("--retries", type=int, default=3, help="Retries for stores download")
	parser.add_argument("--insecure", action="store_true", help="Disable TLS certificate verification (debug only)")
	parser.add_argument("--force-refresh", action="store_true", help="Rebuild even if output already exists")
	parser.add_argument("--debug", action="store_true", help="Verbose logging to stderr")
	return parser.parse_args()


def read_links_map(path: Path) -> Dict[str, object]:
	if not path.exists():
		raise FileNotFoundError(f"Links map was not found: {path}")
	return json.loads(path.read_text(encoding="utf-8"))


def resolve_latest_stores_file(links_map: Dict[str, object]) -> Dict[str, object]:
	by_type = links_map.get("by_type") or {}
	candidates: List[Dict[str, object]] = []

	if isinstance(by_type, dict):
		for key in ("stores", "store"):
			bucket = by_type.get(key)
			if isinstance(bucket, dict):
				files = bucket.get("files")
				if isinstance(files, list):
					candidates.extend([item for item in files if isinstance(item, dict)])

	if not candidates:
		all_files = links_map.get("all_files") or []
		if isinstance(all_files, list):
			for item in all_files:
				if not isinstance(item, dict):
					continue
				file_name = str(item.get("file_name") or "")
				file_type = str(item.get("file_type") or "").lower()
				if file_type in {"stores", "store"} or file_name.lower().startswith("stores"):
					candidates.append(item)

	if not candidates:
		raise RuntimeError("No Stores files found in links map")

	candidates.sort(key=lambda item: (parse_ts(item), str(item.get("file_name") or "")), reverse=True)

	latest = candidates[0]
	if not latest.get("download_url"):
		raise RuntimeError("Latest Stores file has no download_url")
	return latest


def fetch_bytes(
	url: str,
	timeout: int,
	retries: int,
	debug: bool,
	insecure: bool = False,
	opener=None,
	logger: Optional[logging.Logger] = None,
) -> bytes:
	last_error: Optional[Exception] = None
	context = None
	if insecure:
		context = ssl.create_default_context()
		context.check_hostname = False
		context.verify_mode = ssl.CERT_NONE
	for attempt in range(1, retries + 1):
		request = Request(url=url, headers={"User-Agent": USER_AGENT})
		try:
			if opener is not None:
				with opener.open(request, timeout=timeout) as response:
					return response.read()
			with urlopen(request, timeout=timeout, context=context) as response:
				return response.read()
		except (URLError, TimeoutError, OSError) as error:
			last_error = error
			if debug:
				if logger:
					logger.warning("Retry %s/%s stores download failed for %s: %s", attempt, retries, url, error)
				else:
					print(f"[retry {attempt}/{retries}] download failed: {url} ({error})", file=sys.stderr)
			if attempt < retries:
				time.sleep(min(1.5 * attempt, 5.0))
	raise RuntimeError(f"Failed to download stores file: {last_error}")


def decode_payload(raw: bytes) -> str:
	try:
		decompressed = gzip.decompress(raw)
	except OSError:
		decompressed = raw

	for encoding in ("utf-8-sig", "utf-16", "cp1255", "iso-8859-8"):
		try:
			text = decompressed.decode(encoding)
		except UnicodeDecodeError:
			continue
		if "<" in text or "," in text:
			return text

	return decompressed.decode("utf-8-sig", errors="replace")


def local_name(tag: str) -> str:
	return tag.split("}")[-1]


def iter_by_local_name(root: ET.Element, tag_name: str) -> Iterable[ET.Element]:
	lowered = tag_name.lower()
	for node in root.iter():
		if local_name(node.tag).lower() == lowered:
			yield node


def first_child_text(parent: ET.Element, *candidate_names: str) -> Optional[str]:
	candidates = {normalize_key(name) for name in candidate_names}
	for child in parent:
		key = normalize_key(local_name(child.tag))
		if key in candidates:
			text = (child.text or "").strip()
			if text:
				return text
	return None


def parse_xml_stores(xml_text: str, fallback_chain_id: Optional[str]) -> List[Dict[str, object]]:
	root = ET.fromstring(xml_text)
	stores = list(iter_by_local_name(root, "Store"))
	records: List[Dict[str, object]] = []

	for store in stores:
		chain_id = first_child_text(store, "ChainId", "ChainID") or fallback_chain_id
		sub_chain_id = first_child_text(store, "SubChainId", "SubChainID")
		store_id = first_child_text(store, "StoreId", "StoreID", "BikoretNo")
		store_name = first_child_text(store, "StoreName", "StoreNm", "Name")

		if not store_id or not store_name:
			continue

		records.append(
			{
				"chain_id": chain_id,
				"sub_chain_id": sub_chain_id,
				"store_id": store_id,
				"store_name": store_name,
				"address": first_child_text(store, "Address"),
				"city": first_child_text(store, "City", "Town"),
				"zip_code": first_child_text(store, "ZipCode", "Zipcode"),
				"phone": first_child_text(store, "Phone", "Telephone"),
			}
		)

	return records


def parse_csv_stores(csv_text: str, fallback_chain_id: Optional[str]) -> List[Dict[str, object]]:
	records: List[Dict[str, object]] = []
	sample = "\n".join(csv_text.splitlines()[:5])
	dialect = csv.excel
	try:
		dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
	except csv.Error:
		pass

	reader = csv.DictReader(io.StringIO(csv_text), dialect=dialect)
	for row in reader:
		normalized = {normalize_key(str(k)): (v or "").strip() for k, v in row.items() if k}
		store_id = normalized.get("storeid") or normalized.get("bikoretno")
		store_name = normalized.get("storename") or normalized.get("storenm") or normalized.get("name")
		if not store_id or not store_name:
			continue

		records.append(
			{
				"chain_id": normalized.get("chainid") or fallback_chain_id,
				"sub_chain_id": normalized.get("subchainid"),
				"store_id": store_id,
				"store_name": store_name,
				"address": normalized.get("address"),
				"city": normalized.get("city") or normalized.get("town"),
				"zip_code": normalized.get("zipcode"),
				"phone": normalized.get("phone") or normalized.get("telephone"),
			}
		)

	return records


def parse_stores_payload(payload_text: str, fallback_chain_id: Optional[str]) -> List[Dict[str, object]]:
	stripped = payload_text.lstrip()
	if stripped.startswith("<"):
		try:
			return parse_xml_stores(payload_text, fallback_chain_id)
		except ET.ParseError:
			pass
	return parse_csv_stores(payload_text, fallback_chain_id)


def branch_sort_key(item: Dict[str, object]):
	store_id = str(item.get("store_id") or "")
	return (0, int(store_id)) if store_id.isdigit() else (1, store_id)


def clean_nullable(value: Optional[str]) -> Optional[str]:
	if value is None:
		return None
	value = value.strip()
	return value if value else None


def normalize_records(records: List[Dict[str, object]]) -> List[Dict[str, object]]:
	normalized: List[Dict[str, object]] = []
	for row in records:
		store_id = clean_nullable(str(row.get("store_id") or ""))
		store_name = clean_nullable(str(row.get("store_name") or ""))
		chain_id = clean_nullable(str(row.get("chain_id") or ""))
		if not store_id or not store_name:
			continue

		normalized.append(
			{
				"chain_id": chain_id,
				"sub_chain_id": clean_nullable(row.get("sub_chain_id")),
				"store_id": store_id,
				"store_name": store_name,
				"address": clean_nullable(row.get("address")),
				"city": clean_nullable(row.get("city")),
				"zip_code": clean_nullable(row.get("zip_code")),
				"phone": clean_nullable(row.get("phone")),
			}
		)
	normalized.sort(key=branch_sort_key)
	return normalized


def main() -> int:
	args = parse_args()
	logger = configure_chain_logger("get_branches", debug=args.debug)
	chain_log, script_log = get_log_paths("get_branches")
	links_map_path = Path(args.links_map)
	output_path = Path(args.output)

	try:
		logger.info("Starting get_branches run")
		if args.insecure:
			logger.warning("TLS certificate verification is disabled (--insecure)")
		if output_path.exists() and not args.force_refresh and args.debug:
			logger.info("Output exists and will be rebuilt: %s", output_path)

		links_map = read_links_map(links_map_path)
		stores_file = resolve_latest_stores_file(links_map)

		stores_file_name = str(stores_file.get("file_name") or "")
		download_url = str(stores_file.get("download_url") or "")
		if not download_url:
			raise RuntimeError("Stores file download URL is missing")

		file_match = STORES_FILE_RE.match(stores_file_name)
		fallback_chain_id = file_match.group("chain_id") if file_match else None

		auth_opener = None
		if download_url.startswith("https://url.publishedprices.co.il/file/d/"):
			auth_opener = create_logged_in_opener(
				timeout=args.timeout,
				retries=args.retries,
				insecure=args.insecure,
			)

		raw_bytes = fetch_bytes(
			download_url,
			timeout=args.timeout,
			retries=args.retries,
			debug=args.debug,
			insecure=args.insecure,
			opener=auth_opener,
			logger=logger,
		)
		payload_text = decode_payload(raw_bytes)
		parsed_records = parse_stores_payload(payload_text, fallback_chain_id=fallback_chain_id)
		records = normalize_records(parsed_records)

		if not records:
			raise RuntimeError("Parsed zero branches from stores payload")

		result = {
			"schema_version": 1,
			"chain_name": "YOHANANOF",
			"generated_at": iso_now(),
			"source": {
				"links_map_path": str(links_map_path),
				"stores_file_name": stores_file_name,
				"stores_download_url": download_url,
				"stores_published_at_iso": stores_file.get("published_at_iso"),
			},
			"record_count": len(records),
			"records": records,
		}

		output_path.parent.mkdir(parents=True, exist_ok=True)
		output_path.write_text(json.dumps(result, ensure_ascii=True, indent=2), encoding="utf-8")

		logger.info("Wrote %s branches to %s", len(records), output_path)
		logger.info("Chain log: %s | Script log: %s", chain_log, script_log)
		return 0
	except Exception:
		logger.exception("get_branches failed")
		return 1


if __name__ == "__main__":
	raise SystemExit(main())
