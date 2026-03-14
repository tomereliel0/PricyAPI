# API Documentation

## Overview

The API service is implemented in `api/main.py` using FastAPI.

It provides:

- public read endpoints for price lookup
- user-facing search UI
- token-protected admin operations for reload and ingestion trigger

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

## Common Response Shapes

### Name search response

```json
{
  "query": {"q": "קיט", "mode": "full", "limit": 10},
  "total_items": 10,
  "items": [
    {
      "chain": "SHUFERSAL",
      "item_code": "...",
      "item_name": "...",
      "prices": [...],
      "min_price": 1.0,
      "max_price": 3.0
    }
  ]
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
