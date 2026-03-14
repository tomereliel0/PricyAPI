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

## Documentation Split

- API documentation: [docs/API.md](docs/API.md)
- Chains scraping and ingestion documentation: [docs/CHAINS_SCRAPING.md](docs/CHAINS_SCRAPING.md)
- Cerberus chain onboarding playbook: [docs/CERBERUS_CHAIN_ONBOARDING.md](docs/CERBERUS_CHAIN_ONBOARDING.md)

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
- CARREFOUR is non-Cerberus and follows the SHUFERSAL-style public paginated listing flow.
- VICTORY is non-Cerberus and uses laibcatalog table links, including multiple `StoresFull` files (one per sub-chain) merged into one branches set.
- For ingestion and API details, use the split docs above.
