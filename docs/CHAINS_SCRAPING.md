# Chains Scraping Documentation

## Overview

Current implemented chain pipelines:

- SHUFERSAL under `chains/SHUFERSAL`
- RAMI_LEVY under `chains/RAMI_LEVY` (Cerberus)
- OSHER_AD under `chains/OSHER_AD` (Cerberus)
- YOHANANOF under `chains/YOHANANOF` (Cerberus)
- TIV_TAAM under `chains/TIV_TAAM` (Cerberus)
- FRESHMARKET under `chains/FRESHMARKET` (Cerberus)
- SUPER_YUDA under `chains/SUPER_YUDA` (Cerberus, folder-backed source)
- CARREFOUR under `chains/CARREFOUR` (public paginated, non-Cerberus)
- VICTORY under `chains/VICTORY` (laibcatalog table links, non-Cerberus, multi-sub-chain StoresFull)

Recent chain-specific notes:

- VICTORY stores ingestion merges latest `StoresFull` files across sub-chains by `chain_id`.
- VICTORY price payload parsing supports both XML `Item` and XML `Product` node variants.
- Cerberus chains support skip behavior in batch mode for stores without currently published price files.

Pipeline flow:

1. Scrape links index (`scrape_links.py`)
2. Build branches metadata (`get_branches.py`)
3. Fetch branch prices (`get_all_branches_prices.py` / `get_branch_prices.py`)
4. Orchestrate via `run_pipeline.py`

For adding new Cerberus chains, use the step-by-step guide:

- [docs/CERBERUS_CHAIN_ONBOARDING.md](docs/CERBERUS_CHAIN_ONBOARDING.md)

## Scripts

### scrape_links.py

Purpose:

- crawl SHUFERSAL transparency pages
- normalize discovered file rows
- write `links-map.json`

Common usage:

```bash
python chains/SHUFERSAL/scrape_links.py --debug
```

Performance usage:

```bash
python chains/SHUFERSAL/scrape_links.py --max-workers 12 --sleep-ms 0
```

Key args:

- `--output PATH`
- `--max-pages N`
- `--max-workers N`
- `--sleep-ms N`
- `--timeout N`
- `--retries N`

### get_branches.py

Purpose:

- read `links-map.json`
- pick latest `Stores` file
- parse XML/CSV branches payload
- write `branches.json`

Usage:

```bash
python chains/SHUFERSAL/get_branches.py --debug
```

Key args:

- `--links-map PATH`
- `--output PATH`
- `--timeout N`
- `--retries N`
- `--force-refresh`

### get_branch_prices.py

Purpose:

- fetch one store price file and normalize into JSON

Modes:

- `full` -> PriceFull
- `refresh` -> Price

Usage:

```bash
python chains/SHUFERSAL/get_branch_prices.py --store-id 179 --mode full --debug
python chains/SHUFERSAL/get_branch_prices.py --store-id 179 --mode refresh --debug
```

Key args:

- `--store-id STORE_ID`
- `--mode {full,refresh}`
- `--links-map PATH`
- `--output PATH`
- `--timeout N`
- `--retries N`

### get_all_branches_prices.py

Purpose:

- parallel fetch for all store IDs from `branches.json`
- write one output file per store
- write run summary file

Usage:

```bash
python chains/SHUFERSAL/get_all_branches_prices.py --mode full --max-workers 8 --debug
python chains/SHUFERSAL/get_all_branches_prices.py --mode refresh --max-workers 8 --debug
```

Key args:

- `--mode {full,refresh}`
- `--branches-file PATH`
- `--links-map PATH`
- `--output-dir PATH`
- `--max-workers N`
- `--max-branches N`
- `--timeout N`
- `--retries N`
- `--continue-on-error`

### run_pipeline.py

Purpose:

- chain orchestration entrypoint:
  1. optional scrape
  2. branches build
  3. all-branches prices fetch

Important default:

- scrape is skipped unless `--scrape-links` is explicitly passed.

Full mode run:

```bash
python chains/SHUFERSAL/run_pipeline.py --scrape-links --mode full --max-workers 8 --debug
```

Refresh mode run:

```bash
python chains/SHUFERSAL/run_pipeline.py --scrape-links --mode refresh --max-workers 8 --debug
```

Quick local smoke run:

```bash
python chains/SHUFERSAL/run_pipeline.py \
  --scrape-links \
  --scrape-max-pages 5 \
  --max-branches 2 \
  --max-workers 2 \
  --mode full \
  --debug
```

Key args:

- `--scrape-links`
- `--scrape-max-pages N`
- `--mode {full,refresh}`
- `--max-workers N`
- `--max-branches N`
- `--timeout N`
- `--retries N`
- `--links-map PATH`
- `--branches-file PATH`
- `--output-dir PATH`

## Output Files

Schema version currently: `1`

For full field-by-field JSON schema details and examples, see:

- [docs/DATA_FORMATS.md](docs/DATA_FORMATS.md)

### links-map

Path:

- `chains/SHUFERSAL/links-map.json`

Contains:

- all discovered files
- grouping by file type
- latest file selectors per branch where available
- key sections: `all_files`, `by_type`, `latest_by_branch`, `latest_by_type`

### branches

Path:

- `chains/SHUFERSAL/branches.json`

Contains:

- normalized branch metadata records
- key sections: `source`, `record_count`, `records`

### prices per store

Path pattern:

- `chains/SHUFERSAL/prices/{mode}/{store_id}.json`

Contains:

- normalized item price records for one store
- key sections: `source`, `store`, `record_count`, `records`

### run summary

Path pattern:

- `chains/SHUFERSAL/prices/{mode}/run-summary-{mode}.json`

Contains:

- execution metadata
- success/failure counts
- per-store subprocess status details
- key sections: `total_requested`, `total_completed`, `success_count`, `failure_count`, `results`, `failures`

## Logging

Shared logging helper:

- `chains/SHUFERSAL/chain_logging.py`

Logs:

- chain-wide: `chains/SHUFERSAL/logs/shufersal.log`
- per-script: `chains/SHUFERSAL/logs/*.log`

## Operations

### Daily refresh

```bash
source .venv/bin/activate
python chains/SHUFERSAL/run_pipeline.py --scrape-links --mode refresh --max-workers 8
```

### Cerberus chain smoke run (RAMI_LEVY)

```bash
python chains/RAMI_LEVY/run_pipeline.py --scrape-links --mode full --max-branches 10 --max-workers 4 --insecure --debug
```

### Cerberus chain full run (RAMI_LEVY)

```bash
python chains/RAMI_LEVY/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
```

### Full rebuild

```bash
python chains/SHUFERSAL/run_pipeline.py --scrape-links --mode full --max-workers 8 --debug
```

### Cerberus chain smoke run (OSHER_AD)

```bash
python chains/OSHER_AD/run_pipeline.py --scrape-links --mode full --max-branches 10 --max-workers 4 --insecure --debug
```

### Cerberus chain full run (OSHER_AD)

```bash
python chains/OSHER_AD/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
```

### Cerberus chain smoke run (YOHANANOF)

```bash
python chains/YOHANANOF/run_pipeline.py --scrape-links --mode full --max-branches 10 --max-workers 4 --insecure --debug
```

### Cerberus chain full run (YOHANANOF)

```bash
python chains/YOHANANOF/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
```

### Cerberus chain smoke run (TIV_TAAM)

```bash
python chains/TIV_TAAM/run_pipeline.py --scrape-links --mode full --max-branches 10 --max-workers 4 --insecure --debug
```

### Cerberus chain full run (TIV_TAAM)

```bash
python chains/TIV_TAAM/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
```

### Cerberus chain smoke run (FRESHMARKET)

```bash
python chains/FRESHMARKET/run_pipeline.py --scrape-links --mode full --max-branches 10 --max-workers 4 --insecure --debug
```

### Cerberus chain full run (FRESHMARKET)

```bash
python chains/FRESHMARKET/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
```

### Cerberus chain smoke run (SUPER_YUDA)

```bash
python chains/SUPER_YUDA/run_pipeline.py --scrape-links --mode full --max-branches 10 --max-workers 4 --insecure --debug
```

### Cerberus chain full run (SUPER_YUDA)

```bash
python chains/SUPER_YUDA/run_pipeline.py --scrape-links --mode full --max-workers 8 --insecure --debug
```

### Non-Cerberus chain smoke run (CARREFOUR)

```bash
python chains/CARREFOUR/run_pipeline.py --scrape-links --mode full --max-branches 10 --max-workers 4 --debug
```

### Non-Cerberus chain full run (CARREFOUR)

```bash
python chains/CARREFOUR/run_pipeline.py --scrape-links --mode full --max-workers 8 --debug
```

### Non-Cerberus chain smoke run (VICTORY)

```bash
python chains/VICTORY/run_pipeline.py --scrape-links --mode full --max-branches 10 --max-workers 4 --debug
```

### Non-Cerberus chain full run (VICTORY)

```bash
python chains/VICTORY/run_pipeline.py --scrape-links --mode full --max-workers 8 --debug
```

## Troubleshooting

### 403 on stores/prices download

Root cause is typically expired signed URL in links map.

Fix:

```bash
python chains/SHUFERSAL/scrape_links.py --debug
```

Then rerun branches/prices step or full pipeline.

### No price file found for store_id

Some branches may not have a currently published price file in the chain links map.

Current behavior in batch runs:

- branch is marked as `skipped`
- run continues
- summary reports `success_count`, `skipped_count`, and `failure_count`

### Scraper appears stuck

Recent logs include per-page progress. Check:

- `chains/SHUFERSAL/logs/scrape_links.log`

### refresh mode has few/zero records

Run refresh explicitly and verify output folder:

```bash
python chains/SHUFERSAL/run_pipeline.py --scrape-links --mode refresh --max-workers 8
ls chains/SHUFERSAL/prices/refresh
```

### Slow ingestion

Increase workers carefully:

```bash
python chains/SHUFERSAL/scrape_links.py --max-workers 12 --sleep-ms 0
python chains/SHUFERSAL/get_all_branches_prices.py --mode full --max-workers 12
```
