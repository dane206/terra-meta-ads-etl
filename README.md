# terra-meta-ads-etl

Pulls daily Meta Ads performance data across all Terra ad accounts → `terra-analytics-dev.sources.*`

## Ad accounts

| ID | Name |
|---|---|
| act_994866890890084 | Terra 003 |
| act_2219077071728671 | Terra 005 |
| act_461423467875645 | Terra 002 |

## Tables produced

| Table | Granularity |
|---|---|
| `meta_ads_campaigns_daily` | account × campaign × day |
| `meta_ads_adsets_daily` | account × adset × day |
| `meta_ads_ads_daily` | account × ad × day |

## Key metrics captured

Impressions, clicks, spend, reach, frequency, CPM, CPC, CTR, purchases, purchase value, adds to cart, checkouts, link clicks, post engagement, video plays, video thruplays.

Attribution window: **1-day click, 1-day view** (matches Terra Triple Whale settings).

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Add credentials to `config.ini`:

```ini
[meta_ads]
access_token = YOUR_LONG_LIVED_ACCESS_TOKEN
app_id       = YOUR_APP_ID
app_secret   = YOUR_APP_SECRET
account_ids  = act_994866890890084,act_2219077071728671,act_461423467875645

[bigquery]
project = terra-analytics-dev
dataset = sources
```

To generate a long-lived access token: Facebook Developer Console → Your App → Tools → Graph API Explorer → generate token with `ads_read` permission, then exchange for long-lived token.

## Modes

| Mode | What it does |
|---|---|
| `incremental` | Fetches the last 7 days and appends to BQ. Runs daily via scheduler. |
| `backfill` | Fetches a full date range month-by-month and replaces BQ data for each month. Resumes automatically from BQ max date on restart. |
| `catchup` | Fetches the single next missing calendar month after the current BQ max date. |
| `verify` | Compares monthly spend and revenue totals between Meta API and BQ. Flags any month with >1% discrepancy. |

## Usage

```bash
# Daily incremental (last 7 days)
python3 meta_ads_to_bigquery.py --mode incremental

# Backfill a specific range
python3 meta_ads_to_bigquery.py --mode backfill --start 2024-10-01 --end 2024-10-31

# Catch up one missing month
python3 meta_ads_to_bigquery.py --mode catchup

# Verify data integrity against Meta API
python3 meta_ads_to_bigquery.py --mode verify --start 2024-04-01 --end 2026-05-09
```

## ETL schedule (America/Los_Angeles)

| Time | ETL |
|---|---|
| 6:00am | Google Ads |
| 6:15am | Meta Ads |
| 6:30am | Triple Whale |
| 6:45am | Metorik |
| 7:00am | Smartrr |
| 7:15am | Shopify |

## Auth

Long-lived access token stored in `config.ini`. Token expires every ~60 days and must be refreshed. BigQuery uses Application Default Credentials (ADC).

## Cloud Run deployment

Secrets required in Secret Manager:
- `meta-access-token`
- `meta-app-id`
- `meta-app-secret`

```bash
./deploy.sh        # → terra-analytics-dev (default)
./deploy.sh dev    # → terra-analytics-dev
./deploy.sh prod   # → terra-analytics-prod
```
