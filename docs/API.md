# API Documentation

## Overview

The API service is implemented in `api/main.py` using FastAPI.

It provides:

- public read endpoints for price lookup
- user-facing search UI
- token-protected admin operations for reload, single-chain ingestion, all-chains background ingestion, and graceful shutdown

Search behavior notes:

- Items are grouped by barcode (`item_code`) across chains.
- Each grouped item includes `chains`, `chain_names`, and a merged `prices` list with per-row `chain` and `chain_name`.

## Server Startup

From project root:

```bash
cd /Users/tomereliel/Documents/Projects/PricyAPI
source .venv/bin/activate
python api/main.py
```

Alternative startup:

```bash
python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Base URL:

- `http://127.0.0.1:8000`

## Configuration

Environment variables:

- `PRICY_ADMIN_TOKEN`
  - default: `dev-admin-token`

Configuration file:

- root `.env` is loaded automatically at API startup

Set token in `.env`:

```bash
PRICY_ADMIN_TOKEN=your-strong-token
```

Notes:

- shell environment variables still override `.env` values
- `.env` is git-ignored
- use `.env.example` as a template

## Public Endpoints

### GET /health

Returns service status and loaded mode metadata.

Example:

```bash
curl "http://127.0.0.1:8000/health"
```

### GET /meta

Returns currently loaded metadata for each cached mode.

Example:

```bash
curl "http://127.0.0.1:8000/meta"
```

### GET /prices/by-barcode

Query parameters:

- `barcode` (required)
- `mode` (`full` or `refresh`, default `full`)

Example:

```bash
curl -G "http://127.0.0.1:8000/prices/by-barcode" \
  --data "barcode=7290110116316" \
  --data "mode=full"
```

Response highlights:

- grouped item result(s)
- per-branch prices sorted by price and store_id
- min/max item price in returned group

### GET /prices/by-name

Query parameters:

- `q` (required)
- `mode` (`full` or `refresh`, default `full`)
- `limit` (`1..500`, default `50`)

Matching strategy:

- contains match + fuzzy score (SequenceMatcher)
- results sorted by descending score

Example:

```bash
curl -G "http://127.0.0.1:8000/prices/by-name" \
  --data-urlencode "q=קיט" \
  --data "mode=full" \
  --data "limit=10"
```

## UI Routes

### GET /

Serves search page.

### GET /search

Serves search page.

### GET /admin

Serves admin page.

## Admin Endpoints

All admin endpoints require token auth via either:

- `X-Admin-Token` request header
- `token` query parameter

### POST /admin/reload

Query parameters:

- `mode` (`full` or `refresh`)

Purpose:

- reload in-memory indexes from files without restarting the server

Example:

```bash
curl -X POST "http://127.0.0.1:8000/admin/reload?mode=full" \
  -H "X-Admin-Token: dev-admin-token"
```

### POST /admin/pipeline

Query parameters:

- `chain` (default `SHUFERSAL`, must be an available folder under `chains/`)
- `mode` (`full` or `refresh`)
- `max_branches` (default `0`, no cap)
- `max_workers` (`1..32`, default `6`)
- `insecure` (`true|false`, default `false`)

Purpose:

- execute a selected chain ingestion pipeline from API

Behavior:

- if first execution fails with 403/expired/signed hint, retries once with `--scrape-links`

Example:

```bash
curl -X POST "http://127.0.0.1:8000/admin/pipeline?chain=VICTORY&mode=full&max_workers=8" \
  -H "X-Admin-Token: dev-admin-token"
```

Cerberus chain example:

```bash
curl -X POST "http://127.0.0.1:8000/admin/pipeline?chain=RAMI_LEVY&mode=full&max_workers=8&insecure=true" \
  -H "X-Admin-Token: dev-admin-token"
```

### POST /admin/pipeline/all

Query parameters:

- `mode` (`full` or `refresh`)
- `max_branches` (default `0`, no cap)
- `max_workers` (`1..32`, default `6`)
- `insecure` (`true|false`, default `false`)
- `reload_after` (`true|false`, default `true`)

Purpose:

- start a background worker that runs all available chain pipelines sequentially

Behavior:

- returns `202` when a new worker starts
- returns `409` if another all-chains worker is already running
- tracks per-chain success/failure, durations, and output tail

Example:

```bash
curl -X POST "http://127.0.0.1:8000/admin/pipeline/all?mode=full&max_workers=8&reload_after=true" \
  -H "X-Admin-Token: dev-admin-token"
```

### GET /admin/pipeline/all/status

Purpose:

- read current all-chains worker state (running flag, current chain, progress, results)

Example:

```bash
curl "http://127.0.0.1:8000/admin/pipeline/all/status" \
  -H "X-Admin-Token: dev-admin-token"
```

### POST /admin/shutdown

Query parameters:

- `delay_sec` (`0..10`, default `0.2`)

Purpose:

- request graceful API process shutdown after the HTTP response is returned

Example:

```bash
curl -X POST "http://127.0.0.1:8000/admin/shutdown?delay_sec=0.2" \
  -H "X-Admin-Token: dev-admin-token"
```

## Common Response Shapes

### Name search response

```json
{
  "query": {"q": "קיט", "mode": "full", "limit": 10},
  "total_items": 10,
  "items": [
    {
      "item_code": "7290110115364",
      "item_name": "...",
      "manufacturer_name": "...",
      "chains": ["SHUFERSAL", "VICTORY"],
      "chain_names": ["שופרסל", "ויקטורי"],
      "prices": [
        {
          "chain": "SHUFERSAL",
          "chain_name": "שופרסל",
          "chain_id": "7290027600007",
          "store_id": "1",
          "store_name": "...",
          "city": "...",
          "sub_chain_id": "001",
          "price": 38.9,
          "unit_of_measure_price": 4.3222,
          "price_update_date": "2026-03-14 12:30",
          "allow_discount": "1",
          "item_status": "1"
        }
      ],
      "min_price": 38.9,
      "max_price": 39.9,
      "match_score": 0.91
    }
  ]
}
```

### All-chains worker status response

```json
{
  "worker": {
    "running": true,
    "job_id": "9800c0ed482f4e6895bb506abd581c4c",
    "started_at": "2026-03-14T13:33:46.878235+00:00",
    "finished_at": null,
    "mode": "refresh",
    "total_chains": 9,
    "completed_chains": 1,
    "success_count": 1,
    "failure_count": 0,
    "current_chain": "FRESHMARKET",
    "reload_after": false,
    "reload": {
      "attempted": false,
      "success": null,
      "mode": "refresh"
    },
    "results": [
      {
        "chain": "CARREFOUR",
        "status_code": 200,
        "success": true,
        "duration_sec": 1.366,
        "return_code": 0,
        "retried_with_scrape": false,
        "stdout": "",
        "stderr": "...last log tail..."
      }
    ],
    "error": null
  }
}
```

### Reload response

```json
{
  "status": "reloaded",
  "mode": "full",
  "meta": {
    "mode": "full",
    "records": 0,
    "barcodes": 0,
    "chains": []
  }
}
```

## Troubleshooting

### ModuleNotFoundError: No module named api

Use project-root launch:

```bash
cd /Users/tomereliel/Documents/Projects/PricyAPI
source .venv/bin/activate
python api/main.py
```

Do not run `python main.py` from repo root.

### Port 8000 already in use

```bash
lsof -i :8000 -n -P
kill <PID>
```

### Process exits with code 143

Exit 143 usually means SIGTERM (manual termination or task stop). Restart in foreground:

```bash
python api/main.py
```
