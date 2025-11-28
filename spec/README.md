# Data access & sampling (T-ECD)

This document explains where the T-ECD data lives, how to pull a small subset for local development or tests, and what schema/format to expect. Follow the privacy notes below before downloading anything.

## Storage locations

- **Primary bucket:** `s3://t-ecd-prod` (read-only).
- **Layout:** Parquet files partitioned by domain and event date: `s3://t-ecd-prod/<domain>/year=<YYYY>/month=<MM>/day=<DD>/`. Domains include `marketplace`, `payments`, `offers`, and `reviews`.
- **Curated small slice:** `s3://t-ecd-prod-small` contains a maintained ~1% sample (by user_id hash) mirrored from production partitions for fast experimentation.

Access assumes the AWS CLI is configured with the `psb-data` profile and network access to the S3 endpoints (VPN/privileged VPC). Use `AWS_PROFILE=psb-data` in examples if it is not your default profile.

## Quick checks

```bash
# List top-level domains
aws s3 ls s3://t-ecd-prod --profile psb-data

# Inspect a specific day under payments
aws s3 ls s3://t-ecd-prod/payments/year=2024/month=11/day=30/ --profile psb-data
```

## Sampling for development/tests

Use the curated bucket whenever possible; it is already sampled and cleansed. If you need a custom sample, sample by user hash to keep cross-domain consistency.

```bash
# Download the prebuilt small slice (fast path)
aws s3 sync s3://t-ecd-prod-small/payments/ ./data/payments_small --profile psb-data --no-sign-request

# Custom 10k-user sample from payments (requires Parquet + Python)
python scripts/sample_t_ecd.py \
  --source s3://t-ecd-prod/payments/year=2024/month=12/day=01/ \
  --dest ./data/payments_sampled \
  --user-count 10000 \
  --profile psb-data
```

`scripts/sample_t_ecd.py` should:
1) list candidate Parquet objects under the source prefix,
2) stream-read row groups with `pyarrow` or `pandas` using S3 URLs,
3) keep users whose `hash(user_id) % 1000 == 0` (≈0.1%), and
4) write Parquet with the same partition columns preserved.

## Schemas and formats

All domains are stored as **Parquet** with Snappy compression. Columns are UTF-8 unless noted.

### Common fields
- `user_id` (string): stable, anonymized cross-domain user identifier.
- `event_ts` (timestamp, UTC): event time.
- `event_date` (date): derived partition key; duplicates year/month/day folder.

### Domain-specific fields
- **Marketplace:** `item_id` (string), `brand_id` (string), `action` (enum: view, add_to_cart, purchase), `price` (decimal(18,2)), `currency` (string), `device` (string), `traffic_source` (string).
- **Payments:** `merchant_mcc` (string), `amount` (decimal(18,2)), `currency` (string), `channel` (enum: pos, online, p2p), `city` (string), `txn_id` (string).
- **Offers:** `campaign_id` (string), `placement` (string), `impression_id` (string), `action` (enum: impression, click, redirect), `product_code` (string), `ab_group` (string).
- **Reviews:** `item_id` (string), `rating` (int32), `review_len` (int32, character count), `sentiment` (enum: neg, neu, pos), `lang` (string).

## Privacy and handling

- Data is already anonymized, but treat **all** T-ECD exports as confidential: do not copy outside approved buckets or local encrypted disks.
- Disable cloud sync tools (e.g., Dropbox, iCloud) in working directories.
- Strip or mask `user_id` before sharing derived aggregates externally; never publish raw event rows.
- For bug reports, share row counts or aggregate stats, not raw samples.
