# PSB Recommendation System Overview

This document summarizes the target architecture described in [DREAM_ARCHITECTURE.MD](DREAM_ARCHITECTURE.MD) for building a production-ready recommender platform on the T-ECD dataset.

## System Goal
- Deliver personalized and segment-based banking product recommendations (Next Best Offer, cross-sell, upsell, contextual promos) across mobile, web, CRM, and outbound channels.
- Use multi-domain behavioral data to raise relevance, conversion, and marketing efficiency while meeting banking-grade security and SLA targets.

## Data Sources
- **T-ECD dataset** (~44M users, >135B interactions) spanning marketplace, retail delivery, payments, ad offers, reviews.
- **Bank operational events**: transactions, product lifecycle events, campaign interactions, digital channel telemetry.
- **DWH enrichments**: demographic attributes, credit profile, lifecycle markers, periodic aggregates.
- **Reference data**: product catalog, allowed combinations/constraints, fallback popular items for cold start.

## Sampling T-ECD data for development
See [spec/README.md](spec/README.md) for detailed storage layout and privacy notes. Use the curated bucket when possible; for a
custom sample filtered by user hash, use the helper script below (requires `pyarrow` and `s3fs`):

```bash
python scripts/sample_t_ecd.py \
  --source s3://t-ecd-prod/payments/year=2024/month=12/day=01/ \
  --dest ./data/payments_sampled \
  --user-count 10000 \
  --profile psb-data

# Adjust sampling rate (e.g., ~0.2% of users)
python scripts/sample_t_ecd.py \
  --source s3://t-ecd-prod/payments/year=2024/month=12/day=01/ \
  --dest ./data/payments_sampled_dense \
  --hash-mod 500 \
  --profile psb-data
```

## Components & Responsibilities
- **Data ingestion**
  - Batch exports from core systems/DWH; streaming via Kafka topics (e.g., `bank.transactions`, `bank.product_events`, `campaign.interactions`).
  - Schema governance with Avro/Protobuf, versioned contracts, PII handling via internal identifiers.
- **Feature store**
  - Central store for user/product profiles, temporal aggregates, embeddings; supports batch (daily) and streaming updates with recency weighting.
- **Model training**
  - Two-stage pipeline: retrieval (factorization/dual-encoder embeddings) + ranking (GBDT or Wide & Deep with contextual signals).
  - MLflow/registry for versioning; offline evaluation with AUC/CTR/precision@k; periodic retraining + backfill.
- **Model serving**
  - Real-time scoring service with A/B versioning; SLA ≤100 ms P95, availability ≥99.5%.
  - Fallback logic for cold start or missing data; safety filters for prohibited product mixes.
- **API layer**
  - REST/gRPC endpoints for recommendations and feedback; auth via OAuth2/mTLS; rate limiting and observability (metrics, logs, traces).
- **Orchestration/ops**
  - Airflow for pipelines; Spark/Flink for large-scale processing; monitoring of data freshness and drift.

## Expected Interfaces
- **REST**
  - `GET /api/v1/recommendations?user_id=<id>&channel=<mobile|web|crm>&top_k=<n>` → JSON `{ user_id, timestamp, recommendations: [ {product_id, product_name, score, reasons?} ] }`.
  - `POST /api/v1/recommendations/feedback` with JSON `{ rec_id, user_id, event_type (click|dismiss|purchase), timestamp, context }`.
- **gRPC (protobuf)**
  - `RecommendationService.GetRecommendations(Request { user_id, channel, top_k, context }) returns (Response { repeated Recommendation items, generated_at })`.
  - `RecommendationService.SendFeedback(FeedbackEvent { rec_id, user_id, event_type, timestamp, attributes }) returns (Ack { status })`.
- **Streaming/Kafka**
  - Topic schemas for transactions, product events, and feedback events; contracts include idempotency keys and versioned message schemas.

## Deployment Targets (Yandex Cloud)
- **Compute**: Managed Kubernetes (Yandex Managed Service for Kubernetes) for API and online serving; serverless functions optional for lightweight endpoints.
- **Data**: Yandex Managed Kafka for streams; Managed ClickHouse or Yandex Data Proc (Spark) for batch processing; Object Storage for raw/parquet dumps; YDB/PostgreSQL for feature store/metastore.
- **MLOps**: MLflow on Kubernetes or Data Proc; CI/CD via Yandex Cloud Deploy; secrets via Lockbox; monitoring via Yandex Monitoring/Logging.

## Traceability
- For detailed rationale, design variants, and narrative context, see [DREAM_ARCHITECTURE.MD](DREAM_ARCHITECTURE.MD).
