# JSON Data Formats

This document describes the JSON files produced and consumed by PricyAPI, plus API response shapes.

## 1. Chain Metadata Registry

File: `chains-resources.json`

Purpose:

- Registry of supported chains and display metadata.

Typical record shape:

```json
{
  "chain_name": "SHUFERSAL",
  "chain_name_he": "שופרסל",
  "site_url": "https://prices.shufersal.co.il/"
}
```

Notes:

- `chain_name` is the technical key used in folder names and API params.
- `chain_name_he` is used as display label where available.

## 2. Chain Scraper Output: links-map

Path pattern:

- `chains/<CHAIN>/links-map.json`

Purpose:

- Snapshot of discovered downloadable source files.

Top-level fields:

- `schema_version`: integer format version (currently `1`)
- `chain_name`: chain key
- `source_url`: base catalog/listing URL
- `generated_at`: ISO-8601 timestamp
- `max_page_discovered`: highest crawled page/index (when applicable)
- `pages_crawled`: pages actually visited
- `total_files`: total discovered files
- `all_files`: array of normalized file rows
- `by_type`: map from file type (`price`, `pricefull`, `promo`, `stores`, etc.) to arrays of file rows
- `latest_by_branch`: map of `{file_type -> {store_id -> latest_file_row}}`
- `latest_by_type`: map of `{file_type -> latest_file_row}`

`all_files[]` row fields (common):

- `download_url`
- `file_name`
- `file_ext`
- `file_type`
- `file_prefix`
- `chain_id`
- `branch_id`
- `branch_label`
- `branch_name`
- `published_at`
- `published_at_iso`
- `file_timestamp_iso`
- additional source-specific metadata such as `source_page`, `size`, `row_number`

## 3. Branch Catalog Output: branches.json

Path pattern:

- `chains/<CHAIN>/branches.json`

Purpose:

- Canonical list of stores/branches used for branch-level price fetch runs.

Top-level fields:

- `schema_version`
- `chain_name`
- `generated_at`
- `source`: metadata for the Stores file used
- `record_count`: total branch records
- `records`: array of normalized branch records

`source` fields (typical):

- `links_map_path`
- `stores_file_name`
- `stores_download_url`
- `stores_published_at_iso`

`records[]` fields:

- `chain_id`
- `sub_chain_id`
- `store_id`
- `store_name`
- `address`
- `city`
- `zip_code`
- `phone`

Notes:

- Some chains may omit or partially populate optional fields.
- `store_id` is the filename key for per-store price files.

## 4. Per-Store Price Output

Path pattern:

- `chains/<CHAIN>/prices/<mode>/<store_id>.json`

Where:

- `mode=full` uses `PriceFull` source files
- `mode=refresh` uses `Price` source files

Top-level fields:

- `schema_version`
- `chain_name`
- `generated_at`
- `source`: metadata on the source price file
- `store`: `{chain_id, sub_chain_id, store_id}`
- `record_count`
- `skipped_count`
- `records`: array of normalized item price rows

`source` fields (typical):

- `links_map_path`
- `mode`
- `file_type_used`
- `file_name`
- `download_url`
- `published_at_iso`
- `payload_format` (`xml`, `csv`, `zip` variant depending on chain/source)

`records[]` fields (typical):

- `chain_id`
- `sub_chain_id`
- `store_id`
- `item_code` (barcode key used by API grouping)
- `item_name`
- `manufacturer_name`
- `manufacturer_item_description`
- `unit_of_measure`
- `unit_qty`
- `quantity`
- `qty_in_package`
- `price`
- `unit_of_measure_price`
- `allow_discount`
- `item_status`
- `price_update_date`

Notes:

- Some fields can be null/missing by source.
- Numeric values are normalized where parsing succeeds, otherwise may remain string/null.

## 5. Batch Run Summary Output

Path pattern:

- `chains/<CHAIN>/prices/<mode>/run-summary-<mode>.json`

Purpose:

- Execution result summary for `get_all_branches_prices.py`.

Top-level fields:

- `schema_version`
- `chain_name`
- `mode`
- `started_at`
- `finished_at`
- `duration_sec`
- `branches_file`
- `links_map`
- `output_dir`
- `total_requested`
- `total_completed`
- `success_count`
- `failure_count`
- `failures`: high-level failures array
- `results`: per-store execution details

`results[]` fields:

- `store_id`
- `started_at`
- `finished_at`
- `duration_sec`
- `output_file`
- `return_code`
- `stdout`
- `stderr`
- `status` (`ok`, `failed`, or `skipped` depending on chain behavior)

## 6. API Search Response Formats

Endpoints:

- `GET /prices/by-barcode`
- `GET /prices/by-name`

Top-level response fields:

- `query`
- `total_items`
- `items`

Grouped `items[]` format (merged by barcode):

- `item_code`
- `item_name`
- `manufacturer_name`
- `chains`: technical chain keys in this item group
- `chain_names`: display names for chains in this item group
- `prices`: merged per-store rows across all chains
- `min_price`
- `max_price`
- `match_score` (mainly relevant for name search)

`prices[]` fields:

- `chain`
- `chain_name`
- `chain_id`
- `store_id`
- `store_name`
- `city`
- `sub_chain_id`
- `price`
- `unit_of_measure_price`
- `price_update_date`
- `allow_discount`
- `item_status`

## 7. Admin All-Chains Worker Status Format

Endpoint:

- `GET /admin/pipeline/all/status`

Response:

- `worker`: snapshot object

`worker` fields:

- `running`: boolean
- `job_id`: unique run id
- `started_at`
- `finished_at`
- `mode`
- `total_chains`
- `completed_chains`
- `success_count`
- `failure_count`
- `current_chain`
- `reload_after`
- `reload`: `{attempted, success, mode, meta|error}`
- `results`: per-chain execution details
- `error`: worker-level fatal error string/null

`worker.results[]` fields:

- `chain`
- `status_code`
- `success`
- `duration_sec`
- `return_code`
- `retried_with_scrape`
- `stdout`
- `stderr`

Notes:

- `stdout` and `stderr` are intentionally tail-truncated for bounded payload size.
- Only one all-chains worker is active at a time.
