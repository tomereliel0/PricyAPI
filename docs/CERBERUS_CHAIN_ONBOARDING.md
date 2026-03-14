# Cerberus Chain Onboarding Guide

This guide is the repeatable playbook for adding a new Cerberus-based chain.

## Scope

Use this for chains hosted behind Cerberus login flows (username/password session auth) where files are exposed from:

- standard listing roots like `/file`
- or folder-backed roots like `/file/d/<FolderName>/`

## 1. Create Chain Folder

Create a new chain folder under `chains/<CHAIN_KEY>/` (uppercase with underscores), for example:

- `chains/NEW_CHAIN/`

Recommended approach:

- copy from the latest working Cerberus chain that is closest in behavior
- if folder-backed (like SUPER_YUDA), use `chains/SUPER_YUDA` as template
- otherwise use one of `chains/FRESHMARKET`, `chains/TIV_TAAM`, `chains/YOHANANOF`

Required files in the new folder:

- `cerberus_auth.py`
- `scrape_links.py`
- `get_branches.py`
- `get_branch_prices.py`
- `get_all_branches_prices.py`
- `run_pipeline.py`
- `chain_logging.py`
- folders: `logs/`, `prices/full/`, `prices/refresh/`

## 2. Set Chain Identity

Update all chain constants and names:

- script/module docstrings
- `chain_name` fields in JSON output payloads
- logging namespace in `chain_logging.py` (`CHAIN_NAME`)
- output paths in parser help text

Use consistent naming:

- chain key: `NEW_CHAIN`
- logger key: `new_chain` (or chain-specific convention)

## 3. Configure Cerberus Auth

Edit credentials and auth URLs in both:

- `cerberus_auth.py`
- `scrape_links.py`

Set:

- `LOGIN_URL`
- `FILES_URL`
- `USERNAME`
- `PASSWORD` (when the chain requires non-empty password)

Notes:

- Some chains use empty password.
- SUPER_YUDA requires explicit password and uses `publishedprices.co.il`.

## 4. Configure Listing Source Behavior

### Standard root listing

For chains where files are under root listing:

- start URL = `/file`
- JSON dir `cd` = `/`

### Folder-backed listing (special case)

For chains where files live in a specific folder path:

- start URL = `/file/d/<Folder>/`
- JSON dir `cd` = `/<Folder>`
- skip folder/up rows from `aaData`
- skip empty folder entries (for example `Stores` when it is just a folder)

SUPER_YUDA pattern:

- start URL: `/file/d/Yuda/`
- JSON dir cd: `/Yuda`

## 5. Ensure Filename Parsing Works

In `scrape_links.py`:

- verify nested `/file/d/<folder>/<filename>` path extraction resolves the real file name
- verify `FILE_NAME_RE` matches chain file naming
- if chain includes optional sub-chain segment, use optional capture group in regex

Expected outputs in `links-map.json`:

- `all_files`
- `by_type` buckets
- `latest_by_branch`

## 6. Register Chain Globally

Update:

- `chains-resources.json` with a new object
- `api/templates/admin.html` to include chain option in selector

Optional if you add chain-specific resources file later:

- point `chain_resources_path` to expected path in `chains-resources.json`

## 7. Document the Chain

Update:

- `README.md` supported chain list
- `README.md` quick-start command examples
- `README.md` Cerberus notes list
- `docs/CHAINS_SCRAPING.md` overview list
- `docs/CHAINS_SCRAPING.md` smoke/full command sections

## 8. Smoke Validation (Required)

Run capped smoke test:

```bash
source .venv/bin/activate
python chains/<CHAIN_KEY>/run_pipeline.py \
  --scrape-links \
  --scrape-max-pages 3 \
  --mode full \
  --max-branches 2 \
  --max-workers 2 \
  --insecure \
  --debug
```

What to verify:

- `scrape_links` writes a non-empty `links-map.json`
- `get_branches` writes non-empty `branches.json`
- `get_all_branches_prices` completes with expected `success/skipped/failed`
- summary file exists:
  - `chains/<CHAIN_KEY>/prices/full/run-summary-full.json`

## 9. API/Admin Validation (Recommended)

If API is running:

- reload index:

```bash
curl -s -X POST "http://127.0.0.1:8000/admin/reload?mode=full"
```

- run chain pipeline via admin endpoint:

```bash
curl -s -X POST "http://127.0.0.1:8000/admin/pipeline?chain=<CHAIN_KEY>&mode=full&max_branches=2&max_workers=2&insecure=true"
```

## 10. Troubleshooting Quick Notes

- HTTP 403 on downloads: refresh links map and rerun.
- Empty root listing but data exists: use JSON dir API fallback.
- Folder-backed chains: verify `cd` value and start URL are folder-specific.
- Unknown file type: adjust filename regex for that chain’s naming format.
- Missing price file for a store: should be marked `skipped` in batch runs.

## Current Known Cerberus Patterns

- Standard/root listing: RAMI_LEVY, OSHER_AD, YOHANANOF, TIV_TAAM, FRESHMARKET
- Folder-backed listing: SUPER_YUDA

## Operational Recommendation

When adding a new chain, always do this sequence:

1. clone closest template chain
2. set credentials + listing mode
3. run capped smoke validation
4. wire registry/admin/docs
5. run one API/admin integration check
