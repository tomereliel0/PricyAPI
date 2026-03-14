# PricyAPI

PricyAPI is a chain-oriented grocery data ingestion and serving workspace.

Current implementation includes:

- SHUFERSAL scraping and ingestion pipeline
- RAMI_LEVY Cerberus scraping and ingestion pipeline
- OSHER_AD Cerberus scraping and ingestion pipeline
- YOHANANOF Cerberus scraping and ingestion pipeline
- TIV_TAAM Cerberus scraping and ingestion pipeline
- FRESHMARKET Cerberus scraping and ingestion pipeline
- SUPER_YUDA Cerberus scraping and ingestion pipeline
- CARREFOUR scraping and ingestion pipeline
- VICTORY scraping and ingestion pipeline
- FastAPI backend
- User search page
- Token-protected admin page

## Recent Additions

- Cross-chain product merge in search by barcode (`item_code`) so one product appears once with all chain/store prices.
- Search table chain name column, plus clickable header sorting per product table.
- Graceful API shutdown support for Ctrl+C/SIGTERM and admin-triggered shutdown endpoint.
- Admin all-chains pipeline background worker:
  - `POST /admin/pipeline/all` starts full-chain sequential refresh in background
  - `GET /admin/pipeline/all/status` reports progress and per-chain results
  - concurrent all-chain start requests are guarded with `409 already_running`

## Documentation Split

- API documentation: [docs/API.md](docs/API.md)
- Chains scraping and ingestion documentation: [docs/CHAINS_SCRAPING.md](docs/CHAINS_SCRAPING.md)
- Cerberus chain onboarding playbook: [docs/CERBERUS_CHAIN_ONBOARDING.md](docs/CERBERUS_CHAIN_ONBOARDING.md)
- JSON data formats reference: [docs/DATA_FORMATS.md](docs/DATA_FORMATS.md)

## Run From Fresh GitHub Clone

### 1. Prerequisites

- macOS/Linux shell (examples below use `bash`/`zsh`)
- Python 3.10+

### 2. Clone and enter repo

```bash
git clone <YOUR_FORK_OR_REPO_URL>
cd PricyAPI
```

### 3. Create virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 4. Install dependencies

```bash
pip install fastapi uvicorn
```

### 5. Configure admin token

The API loads root `.env` automatically.

```bash
cp .env.example .env
```

Then set:

```dotenv
PRICY_ADMIN_TOKEN=your-strong-token
```

If no token is provided, API default is `dev-admin-token`.

### 6. Run one chain pipeline (smoke)

```bash
python chains/SHUFERSAL/run_pipeline.py --scrape-links --mode full --max-branches 3 --max-workers 4 --debug
```

Cerberus chain smoke example:

```bash
python chains/RAMI_LEVY/run_pipeline.py --scrape-links --mode full --max-branches 3 --max-workers 4 --insecure --debug
```

### 7. Start API

```bash
python api/main.py
```

Open:

- <http://127.0.0.1:8000/search>
- <http://127.0.0.1:8000/admin>

## Quick Start

```bash
cd ./PricyAPI
source .venv/bin/activate
python chains/SHUFERSAL/run_pipeline.py --scrape-links --mode full --max-workers 8 --debug
python chains/RAMI_LEVY/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
python chains/OSHER_AD/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
python chains/YOHANANOF/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
python chains/TIV_TAAM/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
python chains/FRESHMARKET/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
python chains/SUPER_YUDA/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
python chains/CARREFOUR/run_pipeline.py --scrape-links --mode full --max-workers 8 --debug
python chains/VICTORY/run_pipeline.py --scrape-links --mode full --max-workers 8 --debug
python api/main.py
```

Open:

- `http://127.0.0.1:8000/search`
- `http://127.0.0.1:8000/admin`

## Notes

- Use `python api/main.py` from project root.
- Do not run `python main.py` from repo root.
- Cerberus-based chains (`RAMI_LEVY`, `OSHER_AD`, `YOHANANOF`, `TIV_TAAM`, `FRESHMARKET`, `SUPER_YUDA`) may require `--insecure` in environments with certificate trust issues.
- Admin pipeline endpoint supports per-request chain selection via `chain=<CHAIN_KEY>`.
- Admin all-chains worker endpoint is `POST /admin/pipeline/all` with live status via `GET /admin/pipeline/all/status`.
- CARREFOUR is non-Cerberus and follows the SHUFERSAL-style public paginated listing flow.
- VICTORY is non-Cerberus and uses laibcatalog table links, including multiple `StoresFull` files (one per sub-chain) merged into one branches set.
- For ingestion and API details, use the split docs above.
