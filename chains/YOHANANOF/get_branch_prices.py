#!/usr/bin/env python3
"""Fetch and normalize YOHANANOF prices for a single store_id."""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import ssl
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.error import URLError
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from cerberus_auth import create_logged_in_opener
from chain_logging import configure_chain_logger, get_log_paths

USER_AGENT = "PricyAPI/0.1 (+https://github.com/)"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_key(raw_key: str) -> str:
    return "".join(ch for ch in raw_key.lower() if ch.isalnum())


def clean_nullable(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def clean_store_id(value: str) -> str:
    value = value.strip()
    if value.isdigit():
        return str(int(value))
    return value


def branch_keys_for_lookup(store_id: str) -> List[str]:
    cleaned = clean_store_id(store_id)
    keys = {store_id.strip(), cleaned}
    if cleaned.isdigit():
        keys.add(cleaned.zfill(3))
    return [k for k in keys if k]


def parse_ts(file_info: Dict[str, object]) -> str:
    return str(file_info.get("published_at_iso") or file_info.get("file_timestamp_iso") or "")


def parse_float(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    raw = raw.strip().replace(",", ".")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def local_name(tag: str) -> str:
    return tag.split("}")[-1]


def iter_by_local_name(root: ET.Element, tag_name: str) -> Iterable[ET.Element]:
    lowered = tag_name.lower()
    for node in root.iter():
        if local_name(node.tag).lower() == lowered:
            yield node


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Fetch and normalize YOHANANOF branch prices")
    parser.add_argument("--store-id", required=True, help="Store ID (e.g. 1 or 001)")
    parser.add_argument("--links-map", default=str(script_dir / "links-map.json"), help="Path to links-map.json")
    parser.add_argument("--output", default=None, help="Output JSON path (default: chains/YOHANANOF/prices/{store_id}.json)")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Retries for file download")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS certificate verification (debug only)")
    parser.add_argument(
        "--mode",
        default="full",
        choices=["full", "refresh"],
        help="Data mode: full=PriceFull (default), refresh=Price",
    )
    parser.add_argument("--debug", action="store_true", help="Verbose logging to stderr")
    return parser.parse_args()


def read_links_map(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Links map not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def pick_latest_file_for_store(
    links_map: Dict[str, object], store_id: str, prefer_type: str
) -> Tuple[str, Dict[str, object], str]:
    by_type = links_map.get("by_type")
    if not isinstance(by_type, dict):
        raise RuntimeError("Invalid links map: missing by_type")

    type_order = [prefer_type]
    fallback = "pricefull" if prefer_type == "price" else "price"
    type_order.append(fallback)

    lookup_keys = branch_keys_for_lookup(store_id)

    for file_type in type_order:
        bucket = by_type.get(file_type)
        if not isinstance(bucket, dict):
            continue

        latest_by_branch = bucket.get("latest_by_branch")
        if isinstance(latest_by_branch, dict):
            for key in lookup_keys:
                item = latest_by_branch.get(key)
                if isinstance(item, dict) and item.get("download_url"):
                    resolved_store_id = clean_store_id(str(item.get("branch_id") or key))
                    return file_type, item, resolved_store_id

        files = bucket.get("files")
        candidates: List[Dict[str, object]] = []
        if isinstance(files, list):
            for item in files:
                if not isinstance(item, dict):
                    continue
                branch_id = str(item.get("branch_id") or "")
                if branch_id in lookup_keys and item.get("download_url"):
                    candidates.append(item)

        if candidates:
            candidates.sort(key=lambda x: (parse_ts(x), str(x.get("file_name") or "")), reverse=True)
            item = candidates[0]
            resolved_store_id = clean_store_id(str(item.get("branch_id") or store_id))
            return file_type, item, resolved_store_id

    raise RuntimeError(f"No price file found for store_id={store_id}")


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
        except HTTPError as error:
            last_error = error
            if debug:
                if logger:
                    logger.warning("Retry %s/%s price download failed for %s: HTTP %s", attempt, retries, url, error.code)
                else:
                    print(f"[retry {attempt}/{retries}] download failed: {url} (HTTP {error.code})", file=sys.stderr)
            # HTTP 403 here is usually an expired SAS token in the signed URL.
            if error.code == 403:
                raise RuntimeError(
                    "Failed to download price file: HTTP 403 (signed download URL likely expired; refresh links-map)"
                ) from error
            if attempt < retries:
                time.sleep(min(1.5 * attempt, 5.0))
        except (URLError, TimeoutError, OSError) as error:
            last_error = error
            if debug:
                if logger:
                    logger.warning("Retry %s/%s price download failed for %s: %s", attempt, retries, url, error)
                else:
                    print(f"[retry {attempt}/{retries}] download failed: {url} ({error})", file=sys.stderr)
            if attempt < retries:
                time.sleep(min(1.5 * attempt, 5.0))
    raise RuntimeError(f"Failed to download price file: {last_error}")


def decode_payload(raw: bytes) -> str:
    try:
        decompressed = gzip.decompress(raw)
    except OSError:
        decompressed = raw

    # Some branches publish price payloads as ZIP archives containing XML/CSV.
    if decompressed.startswith(b"PK"):
        try:
            with zipfile.ZipFile(io.BytesIO(decompressed)) as zf:
                names = [n for n in zf.namelist() if not n.endswith("/")]
                if names:
                    names.sort(key=lambda n: (0 if n.lower().endswith((".xml", ".csv")) else 1, len(n)))
                    decompressed = zf.read(names[0])
        except zipfile.BadZipFile:
            pass

    # Cerberus files are sometimes UTF-16; detect that before trying UTF-8.
    if decompressed.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return decompressed.decode("utf-16")
        except UnicodeDecodeError:
            pass
    if decompressed.count(b"\x00") > max(4, len(decompressed) // 12):
        try:
            return decompressed.decode("utf-16")
        except UnicodeDecodeError:
            pass

    for encoding in ("utf-8-sig", "utf-16", "cp1255", "iso-8859-8"):
        try:
            text = decompressed.decode(encoding)
        except UnicodeDecodeError:
            continue
        if "<" in text or "," in text:
            return text

    return decompressed.decode("utf-8-sig", errors="replace")


def parse_xml_prices(xml_text: str, fallback_store_id: str) -> Tuple[Dict[str, Optional[str]], List[Dict[str, object]], int]:
    root = ET.fromstring(xml_text)

    root_fields: Dict[str, Optional[str]] = {}
    for child in root:
        if len(child):
            continue
        root_fields[normalize_key(local_name(child.tag))] = clean_nullable((child.text or ""))

    chain_id = clean_nullable(root_fields.get("chainid"))
    sub_chain_id = clean_nullable(root_fields.get("subchainid"))
    store_id = clean_store_id(root_fields.get("storeid") or fallback_store_id)

    item_nodes = list(iter_by_local_name(root, "Item"))
    records: List[Dict[str, object]] = []
    skipped = 0

    for item in item_nodes:
        item_map: Dict[str, str] = {}
        for child in item:
            key = normalize_key(local_name(child.tag))
            item_map[key] = (child.text or "").strip()

        item_code = clean_nullable(item_map.get("itemcode"))
        price = parse_float(item_map.get("itemprice") or item_map.get("price"))
        if not item_code or price is None:
            skipped += 1
            continue

        record = {
            "chain_id": chain_id,
            "sub_chain_id": sub_chain_id,
            "store_id": store_id,
            "item_code": item_code,
            "item_name": clean_nullable(item_map.get("itemname")),
            "manufacturer_name": clean_nullable(item_map.get("manufacturername")),
            "manufacturer_item_description": clean_nullable(item_map.get("manufactureritemdescription")),
            "unit_of_measure": clean_nullable(item_map.get("unitofmeasure")),
            "unit_qty": clean_nullable(item_map.get("unitqty")),
            "quantity": parse_float(item_map.get("quantity")),
            "qty_in_package": parse_float(item_map.get("qtyinpackage")),
            "price": price,
            "unit_of_measure_price": parse_float(item_map.get("unitofmeasureprice")),
            "allow_discount": clean_nullable(item_map.get("allowdiscount")),
            "item_status": clean_nullable(item_map.get("itemstatus")),
            "price_update_date": clean_nullable(item_map.get("priceupdatedate")),
        }
        records.append(record)

    return {
        "chain_id": chain_id,
        "sub_chain_id": sub_chain_id,
        "store_id": store_id,
    }, records, skipped


def parse_csv_prices(csv_text: str, fallback_store_id: str) -> Tuple[Dict[str, Optional[str]], List[Dict[str, object]], int]:
    sample = "\n".join(csv_text.splitlines()[:5])
    dialect = csv.excel
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        pass

    # newline='' keeps CSV parser in control of newline handling for mixed payloads.
    reader = csv.DictReader(io.StringIO(csv_text, newline=""), dialect=dialect)
    records: List[Dict[str, object]] = []
    skipped = 0
    chain_id: Optional[str] = None
    sub_chain_id: Optional[str] = None
    store_id: Optional[str] = None

    try:
        for row in reader:
            normalized = {normalize_key(str(k)): (v or "").strip() for k, v in row.items() if k}
            row_chain_id = clean_nullable(normalized.get("chainid"))
            row_sub_chain_id = clean_nullable(normalized.get("subchainid"))
            row_store_id = clean_nullable(normalized.get("storeid") or normalized.get("branchid"))

            chain_id = chain_id or row_chain_id
            sub_chain_id = sub_chain_id or row_sub_chain_id
            store_id = store_id or row_store_id

            item_code = clean_nullable(normalized.get("itemcode"))
            price = parse_float(normalized.get("itemprice") or normalized.get("price"))
            if not item_code or price is None:
                skipped += 1
                continue

            records.append(
                {
                    "chain_id": row_chain_id,
                    "sub_chain_id": row_sub_chain_id,
                    "store_id": clean_store_id(row_store_id or fallback_store_id),
                    "item_code": item_code,
                    "item_name": clean_nullable(normalized.get("itemname")),
                    "manufacturer_name": clean_nullable(normalized.get("manufacturername")),
                    "manufacturer_item_description": clean_nullable(normalized.get("manufactureritemdescription")),
                    "unit_of_measure": clean_nullable(normalized.get("unitofmeasure")),
                    "unit_qty": clean_nullable(normalized.get("unitqty")),
                    "quantity": parse_float(normalized.get("quantity")),
                    "qty_in_package": parse_float(normalized.get("qtyinpackage")),
                    "price": price,
                    "unit_of_measure_price": parse_float(normalized.get("unitofmeasureprice")),
                    "allow_discount": clean_nullable(normalized.get("allowdiscount")),
                    "item_status": clean_nullable(normalized.get("itemstatus")),
                    "price_update_date": clean_nullable(normalized.get("priceupdatedate")),
                }
            )
    except csv.Error:
        # Fallback for malformed CSV rows: normalize line endings and strip NULs.
        sanitized = csv_text.replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n")
        if sanitized != csv_text:
            return parse_csv_prices(sanitized, fallback_store_id)
        raise

    metadata = {
        "chain_id": chain_id,
        "sub_chain_id": sub_chain_id,
        "store_id": clean_store_id(store_id or fallback_store_id),
    }
    return metadata, records, skipped


def parse_prices_payload(payload_text: str, fallback_store_id: str) -> Tuple[str, Dict[str, Optional[str]], List[Dict[str, object]], int]:
    stripped = payload_text.lstrip()
    if stripped.startswith("<"):
        try:
            metadata, records, skipped = parse_xml_prices(payload_text, fallback_store_id)
            return "xml", metadata, records, skipped
        except ET.ParseError:
            pass

    metadata, records, skipped = parse_csv_prices(payload_text, fallback_store_id)
    return "csv", metadata, records, skipped


def item_sort_key(item: Dict[str, object]) -> Tuple[int, object]:
    code = str(item.get("item_code") or "")
    return (0, int(code)) if code.isdigit() else (1, code)


def main() -> int:
    args = parse_args()
    logger = configure_chain_logger("get_branch_prices", debug=args.debug)
    chain_log, script_log = get_log_paths("get_branch_prices")

    try:
        logger.info("Starting get_branch_prices run for store_id=%s mode=%s", args.store_id, args.mode)
        if args.insecure:
            logger.warning("TLS certificate verification is disabled (--insecure)")
        links_map = read_links_map(Path(args.links_map))

        prefer_type = "pricefull" if args.mode == "full" else "price"

        used_type, source_file, resolved_store_id = pick_latest_file_for_store(
            links_map=links_map,
            store_id=args.store_id,
            prefer_type=prefer_type,
        )

        download_url = str(source_file.get("download_url") or "")
        if not download_url:
            raise RuntimeError("Selected source file has no download_url")

        auth_opener = None
        if download_url.startswith("https://url.publishedprices.co.il/file/d/"):
            auth_opener = create_logged_in_opener(
                timeout=args.timeout,
                retries=args.retries,
                insecure=args.insecure,
            )

        raw_bytes = fetch_bytes(
            url=download_url,
            timeout=args.timeout,
            retries=args.retries,
            debug=args.debug,
            insecure=args.insecure,
            opener=auth_opener,
            logger=logger,
        )
        payload_text = decode_payload(raw_bytes)
        payload_format, metadata, records, skipped_count = parse_prices_payload(payload_text, fallback_store_id=resolved_store_id)

        if not records:
            raise RuntimeError("Parsed zero price records for selected store")

        for record in records:
            record["chain_id"] = record.get("chain_id") or metadata.get("chain_id")
            record["sub_chain_id"] = record.get("sub_chain_id") or metadata.get("sub_chain_id")
            record["store_id"] = clean_store_id(str(record.get("store_id") or metadata.get("store_id") or resolved_store_id))

        records.sort(key=item_sort_key)

        output_path: Path
        if args.output:
            output_path = Path(args.output)
        else:
            script_dir = Path(__file__).resolve().parent
            output_path = script_dir / "prices" / f"{clean_store_id(str(metadata.get('store_id') or resolved_store_id))}.json"

        result = {
            "schema_version": 1,
            "chain_name": "YOHANANOF",
            "generated_at": iso_now(),
            "source": {
                "links_map_path": str(Path(args.links_map).resolve()),
                "mode": args.mode,
                "file_type_used": used_type,
                "file_name": source_file.get("file_name"),
                "download_url": download_url,
                "published_at_iso": source_file.get("published_at_iso"),
                "payload_format": payload_format,
            },
            "store": {
                "chain_id": metadata.get("chain_id"),
                "sub_chain_id": metadata.get("sub_chain_id"),
                "store_id": clean_store_id(str(metadata.get("store_id") or resolved_store_id)),
            },
            "record_count": len(records),
            "skipped_count": skipped_count,
            "records": records,
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=True, indent=2), encoding="utf-8")

        logger.info(
            "Wrote %s price records for store %s to %s",
            len(records),
            result["store"]["store_id"],
            output_path,
        )
        logger.info("Chain log: %s | Script log: %s", chain_log, script_log)
        return 0
    except Exception:
        logger.exception("get_branch_prices failed for store_id=%s mode=%s", args.store_id, args.mode)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
