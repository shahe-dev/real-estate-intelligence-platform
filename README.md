# Real Estate Intelligence Platform

Data pipeline and API that aggregates Dubai property market data, validates it
deterministically, and serves pre-computed metrics for AI-driven SEO content generation.

## What It Does

Collects property transaction data, floor plan records, and market indicators from public
and licensed data sources. Python validation layers enforce plausibility rules before any
data is exposed to the Claude API for content generation. Output feeds production websites
across three markets.

## Architecture

```
Data Sources (DLD transactions, property portals, GSC, Ahrefs)
  --> Ingestion (Python scrapers + BigQuery load jobs)
  --> Validation (DuckDB, deterministic Python rules)
  --> API (FastAPI, pre-computed metrics endpoints)
  --> Content Generation (Claude API narrates validated data)
  --> Re-validation (Python checks generated content against source data)
```

Anti-hallucination pattern: Python computes all metrics deterministically. Claude narrates
pre-validated numbers. Python re-validates the output before any page is published.

## Tech Stack

- Python 3.11 / FastAPI
- DuckDB (local analytics) / BigQuery (production data warehouse)
- Claude API (claude-sonnet-4-6) for content generation
- Google Cloud Storage / BigQuery for data storage
- Ahrefs API / Google Search Console API for SEO signals

## Setup

### Prerequisites

- Python 3.11+
- Google Cloud project with BigQuery enabled
- Anthropic API key
- Ahrefs API key (optional, for keyword data)

### Installation

```bash
git clone https://github.com/shahe-dev/real-estate-intelligence-platform
cd real-estate-intelligence-platform
cp .env.example .env
# Edit .env with your credentials
pip install -r requirements.txt
```

### Running

```bash
uvicorn src.api.main:app --reload
```

## Project Structure

```
config/              # Configuration and validation rules
data/
    raw/             # Raw CSV files (not in git)
    database/        # DuckDB files (not in git)
    generated_content/
src/
    etl/             # Data loading and validation
    metrics/         # Pre-calculated metrics
    api/             # FastAPI endpoints
    content/         # AI content generation
    analytics/       # Keyword and citation intelligence
    dashboard/       # Admin interface
    utils/           # Shared utilities
scripts/             # One-off analysis and maintenance scripts
tests/               # Unit tests
```

## Environment Variables

See `.env.example` for the full list. Required variables:

- `ANTHROPIC_API_KEY` - Anthropic API key
- `GOOGLE_PROJECT_ID` - GCP project ID for BigQuery
- `GOOGLE_CLIENT_EMAIL` - Service account email
- `GOOGLE_PRIVATE_KEY` - Service account private key (or use `GOOGLE_SERVICE_ACCOUNT_FILE`)

## Project Status

Active. Serving data to production content pipelines across multiple markets.

## License

[PolyForm Noncommercial 1.0.0](LICENSE) -- free for personal and research use, not for commercial use.
