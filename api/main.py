#!/usr/bin/env python3
"""Pricy API server for item search and cross-branch prices."""

from __future__ import annotations

import json
import os
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ROOT_DIR = Path(__file__).resolve().parents[1]
CHAINS_DIR = ROOT_DIR / "chains"
STATIC_DIR = Path(__file__).resolve().parent / "web" / "static"
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
DEFAULT_MODE = "full"
SUPPORTED_MODES = {"full", "refresh"}


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


# Load root .env once at startup. Explicit shell env vars still override these values.
load_dotenv(ROOT_DIR / ".env")


@dataclass
class IndexedRecord:
    chain_key: str
    chain_id: Optional[str]
    sub_chain_id: Optional[str]
    store_id: str
    store_name: Optional[str]
    city: Optional[str]
    item_code: str
    item_name: str
    manufacturer_name: Optional[str]
    price: float
    unit_of_measure_price: Optional[float]
    price_update_date: Optional[str]
    allow_discount: Optional[str]
    item_status: Optional[str]


class PriceIndexStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._mode_cache: Dict[str, Dict[str, Any]] = {}

    def _normalize_store_id(self, value: Any) -> str:
        text = str(value or "").strip()
        if text.isdigit():
            return str(int(text))
        return text

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _get_chain_dirs(self) -> List[Path]:
        if not CHAINS_DIR.exists():
            return []
        return sorted([p for p in CHAINS_DIR.iterdir() if p.is_dir()])

    def _load_mode(self, mode: str) -> Dict[str, Any]:
        now_iso = datetime.now(timezone.utc).isoformat()
        records: List[IndexedRecord] = []

        for chain_dir in self._get_chain_dirs():
            chain_key = chain_dir.name
            branches_map: Dict[str, Dict[str, Any]] = {}

            branches_path = chain_dir / "branches.json"
            if branches_path.exists():
                try:
                    branches_payload = json.loads(branches_path.read_text(encoding="utf-8"))
                    for row in branches_payload.get("records", []):
                        store_id = self._normalize_store_id(row.get("store_id"))
                        if store_id:
                            branches_map[store_id] = row
                except Exception:
                    pass

            prices_base = chain_dir / "prices"
            prices_mode_dir = prices_base / mode

            price_files: List[Path] = []
            if prices_mode_dir.exists():
                price_files.extend(sorted(prices_mode_dir.glob("*.json")))
            if not price_files and prices_base.exists():
                price_files.extend(sorted(prices_base.glob("*.json")))

            for price_path in price_files:
                if price_path.name.startswith("run-summary"):
                    continue
                try:
                    payload = json.loads(price_path.read_text(encoding="utf-8"))
                except Exception:
                    continue

                file_records = payload.get("records") or []
                if not isinstance(file_records, list):
                    continue

                for row in file_records:
                    item_code = str(row.get("item_code") or "").strip()
                    item_name = str(row.get("item_name") or "").strip()
                    if not item_code or not item_name:
                        continue

                    price = self._safe_float(row.get("price"))
                    if price is None:
                        continue

                    store_id = self._normalize_store_id(row.get("store_id"))
                    branch_meta = branches_map.get(store_id, {})

                    records.append(
                        IndexedRecord(
                            chain_key=chain_key,
                            chain_id=str(row.get("chain_id") or branch_meta.get("chain_id") or "") or None,
                            sub_chain_id=str(row.get("sub_chain_id") or branch_meta.get("sub_chain_id") or "") or None,
                            store_id=store_id,
                            store_name=(str(branch_meta.get("store_name") or "").strip() or None),
                            city=(str(branch_meta.get("city") or "").strip() or None),
                            item_code=item_code,
                            item_name=item_name,
                            manufacturer_name=(str(row.get("manufacturer_name") or "").strip() or None),
                            price=price,
                            unit_of_measure_price=self._safe_float(row.get("unit_of_measure_price")),
                            price_update_date=(str(row.get("price_update_date") or "").strip() or None),
                            allow_discount=(str(row.get("allow_discount") or "").strip() or None),
                            item_status=(str(row.get("item_status") or "").strip() or None),
                        )
                    )

        barcode_index: Dict[str, List[IndexedRecord]] = {}
        name_index: List[IndexedRecord] = records
        for record in records:
            barcode_index.setdefault(record.item_code, []).append(record)

        meta = {
            "mode": mode,
            "loaded_at": now_iso,
            "records": len(records),
            "barcodes": len(barcode_index),
            "chains": sorted({r.chain_key for r in records}),
        }

        return {
            "records": records,
            "barcode_index": barcode_index,
            "name_index": name_index,
            "meta": meta,
        }

    def ensure_mode(self, mode: str) -> Dict[str, Any]:
        if mode not in SUPPORTED_MODES:
            raise HTTPException(status_code=400, detail=f"Unsupported mode '{mode}'")
        with self._lock:
            if mode not in self._mode_cache:
                self._mode_cache[mode] = self._load_mode(mode)
            return self._mode_cache[mode]

    def reload_mode(self, mode: str) -> Dict[str, Any]:
        if mode not in SUPPORTED_MODES:
            raise HTTPException(status_code=400, detail=f"Unsupported mode '{mode}'")
        with self._lock:
            self._mode_cache[mode] = self._load_mode(mode)
            return self._mode_cache[mode]

    def get_meta(self) -> Dict[str, Any]:
        with self._lock:
            return {mode: data["meta"] for mode, data in self._mode_cache.items()}


store = PriceIndexStore()
app = FastAPI(title="Pricy API", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _admin_token() -> str:
    return os.getenv("PRICY_ADMIN_TOKEN", "dev-admin-token")


def _available_chains() -> List[str]:
    if not CHAINS_DIR.exists():
        return []
    return sorted([p.name for p in CHAINS_DIR.iterdir() if p.is_dir()])


def require_admin(x_admin_token: Optional[str] = Header(default=None), token: Optional[str] = Query(default=None)) -> None:
    candidate = x_admin_token or token
    if candidate != _admin_token():
        raise HTTPException(status_code=401, detail="Unauthorized")


def _score_name(query: str, item_name: str) -> Optional[float]:
    q = query.strip().lower()
    n = item_name.strip().lower()
    if not q or not n:
        return None
    contains = q in n
    ratio = SequenceMatcher(None, q, n).ratio()
    if not contains and ratio < 0.42:
        return None
    bonus = 0.4 if contains else 0.0
    return ratio + bonus


def _group_records(rows: List[IndexedRecord], query: Optional[str] = None) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}

    for row in rows:
        key = (row.chain_key, row.item_code)
        entry = grouped.get(key)
        if entry is None:
            entry = {
                "chain": row.chain_key,
                "chain_id": row.chain_id,
                "item_code": row.item_code,
                "item_name": row.item_name,
                "manufacturer_name": row.manufacturer_name,
                "prices": [],
                "match_score": _score_name(query or "", row.item_name) if query else None,
            }
            grouped[key] = entry

        entry["prices"].append(
            {
                "store_id": row.store_id,
                "store_name": row.store_name,
                "city": row.city,
                "sub_chain_id": row.sub_chain_id,
                "price": row.price,
                "unit_of_measure_price": row.unit_of_measure_price,
                "price_update_date": row.price_update_date,
                "allow_discount": row.allow_discount,
                "item_status": row.item_status,
            }
        )

    items = list(grouped.values())
    for item in items:
        item["prices"].sort(key=lambda p: (p.get("price", 0), p.get("store_id", "")))
        price_values = [p["price"] for p in item["prices"] if p.get("price") is not None]
        item["min_price"] = min(price_values) if price_values else None
        item["max_price"] = max(price_values) if price_values else None

    items.sort(key=lambda i: (-(i.get("match_score") or 0), i.get("item_name") or "", i.get("chain") or ""))
    return items


@app.on_event("startup")
def startup_load_default() -> None:
    store.ensure_mode(DEFAULT_MODE)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "default_mode": DEFAULT_MODE,
        "loaded": store.get_meta(),
    }


@app.get("/meta")
def meta() -> Dict[str, Any]:
    return {"modes": store.get_meta()}


@app.get("/prices/by-barcode")
def prices_by_barcode(
    barcode: str = Query(..., min_length=2),
    mode: str = Query(DEFAULT_MODE),
) -> Dict[str, Any]:
    data = store.ensure_mode(mode)
    matches = data["barcode_index"].get(barcode.strip(), [])
    grouped = _group_records(matches)
    return {
        "query": {"barcode": barcode, "mode": mode},
        "total_items": len(grouped),
        "items": grouped,
    }


@app.get("/prices/by-name")
def prices_by_name(
    q: str = Query(..., min_length=1),
    mode: str = Query(DEFAULT_MODE),
    limit: int = Query(50, ge=1, le=500),
) -> Dict[str, Any]:
    data = store.ensure_mode(mode)
    query = q.strip()

    scored: List[tuple[float, IndexedRecord]] = []
    for row in data["name_index"]:
        score = _score_name(query, row.item_name)
        if score is not None:
            scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)

    top_rows = [row for _, row in scored[: max(limit * 15, 500)]]
    grouped = _group_records(top_rows, query=query)

    return {
        "query": {"q": query, "mode": mode, "limit": limit},
        "total_items": len(grouped),
        "items": grouped[:limit],
    }


@app.get("/")
def ui_home() -> FileResponse:
    return FileResponse(str(TEMPLATES_DIR / "search.html"))


@app.get("/search")
def ui_search() -> FileResponse:
    return FileResponse(str(TEMPLATES_DIR / "search.html"))


@app.get("/admin")
def ui_admin() -> FileResponse:
    return FileResponse(str(TEMPLATES_DIR / "admin.html"))


@app.post("/admin/reload")
def admin_reload(
    mode: str = Query(DEFAULT_MODE),
    _: None = Depends(require_admin),
) -> Dict[str, Any]:
    result = store.reload_mode(mode)
    return {
        "status": "reloaded",
        "mode": mode,
        "meta": result["meta"],
    }


@app.post("/admin/pipeline")
def admin_pipeline(
    chain: str = Query("SHUFERSAL", min_length=1),
    mode: str = Query(DEFAULT_MODE),
    max_branches: int = Query(0, ge=0),
    max_workers: int = Query(6, ge=1, le=32),
    insecure: bool = Query(False),
    _: None = Depends(require_admin),
) -> JSONResponse:
    python_bin = str(ROOT_DIR / ".venv" / "bin" / "python")
    chain_key = chain.strip().upper()
    available = _available_chains()
    if chain_key not in available:
        raise HTTPException(status_code=400, detail=f"Unsupported chain '{chain_key}'. Available: {available}")

    pipeline = ROOT_DIR / "chains" / chain_key / "run_pipeline.py"
    if not pipeline.exists():
        raise HTTPException(status_code=400, detail=f"Pipeline script not found for chain '{chain_key}'")

    cmd = [python_bin, str(pipeline), "--mode", mode, "--max-workers", str(max_workers)]
    if max_branches > 0:
        cmd.extend(["--max-branches", str(max_branches)])
    if insecure:
        cmd.append("--insecure")

    first = subprocess.run(cmd, capture_output=True, text=True)
    retried = False
    if first.returncode != 0:
        blob = f"{first.stdout}\n{first.stderr}".lower()
        if "http 403" in blob or "signed" in blob or "expired" in blob:
            retry_cmd = list(cmd)
            retry_cmd.append("--scrape-links")
            retried = True
            first = subprocess.run(retry_cmd, capture_output=True, text=True)

    payload = {
        "chain": chain_key,
        "mode": mode,
        "max_branches": max_branches,
        "max_workers": max_workers,
        "insecure": insecure,
        "retried_with_scrape": retried,
        "return_code": first.returncode,
        "stdout": first.stdout[-4000:],
        "stderr": first.stderr[-4000:],
    }

    status = 200 if first.returncode == 0 else 500
    return JSONResponse(status_code=status, content=payload)


if __name__ == "__main__":
    import uvicorn

    # Run the loaded app object directly so launch works from different cwd contexts.
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=False)
