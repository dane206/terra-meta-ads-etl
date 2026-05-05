# terra-meta-ads-etl

Pulls daily Meta Ads performance data across all Terra ad accounts → `terra-analytics-dev.sources.*`

## Ad accounts

| ID | Name |
|---|---|
| act_994866890890084 | Terra 003 |
| act_466216000727046 | Terra 001 |
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
account_ids  = act_994866890890084,act_466216000727046,act_2219077071728671,act_461423467875645

[bigquery]
project = terra-analytics-dev
dataset = sources
```

To generate a long-lived access token: Facebook Developer Console → Your App → Tools → Graph API Explorer → generate token with `ads_read` permission, then exchange for long-lived token.

## Usage

```bash
# Incremental — last 7 days, APPEND
python meta_ads_to_bigquery.py --mode incremental

# Backfill — 2024-04-01 → yesterday, TRUNCATE
python meta_ads_to_bigquery.py --mode backfill

# Backfill from specific date
python meta_ads_to_bigquery.py --mode backfill --start 2024-10-01
```

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
