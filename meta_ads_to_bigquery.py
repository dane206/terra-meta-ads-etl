#!/usr/bin/env python3
"""
Terra Health Essentials — Meta Ads API → BigQuery ETL
=====================================================
Pulls daily performance data at campaign / ad set / ad level
across all Terra Meta ad accounts.

Ad accounts:
  act_994866890890084  Terra 003
  act_466216000727046  Terra 001
  act_2219077071728671 Terra 005
  act_461423467875645  Terra 002

Tables produced:
  terra-analytics-dev.sources.meta_ads_campaigns_daily
  terra-analytics-dev.sources.meta_ads_adsets_daily
  terra-analytics-dev.sources.meta_ads_ads_daily

Run modes:
  python meta_ads_to_bigquery.py --mode backfill --start 2024-04-01
  python meta_ads_to_bigquery.py --mode incremental   # last 7 days

Attribution window: 1-day click, 1-day view (matches Terra TW settings)

Config (config.ini):
  [meta_ads]
  access_token    = YOUR_LONG_LIVED_ACCESS_TOKEN
  app_id          = YOUR_APP_ID
  app_secret      = YOUR_APP_SECRET
  account_ids     = act_994866890890084,act_466216000727046,act_2219077071728671,act_461423467875645
"""

import os, sys, argparse, traceback, time
from datetime import datetime, timedelta, date, timezone

import configparser
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

# ── Config ────────────────────────────────────────────────────────────────────
ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN") or config.get("meta_ads", "access_token", fallback=None)
APP_ID       = os.environ.get("META_APP_ID")       or config.get("meta_ads", "app_id", fallback=None)
APP_SECRET   = os.environ.get("META_APP_SECRET")   or config.get("meta_ads", "app_secret", fallback=None)
ACCOUNT_IDS  = (
    os.environ.get("META_ACCOUNT_IDS") or
    config.get("meta_ads", "account_ids", fallback="")
).split(",")

BQ_PROJECT   = os.environ.get("BQ_PROJECT") or config.get("bigquery", "project", fallback="terra-analytics-dev")
BQ_DATASET   = config.get("bigquery", "dataset", fallback="sources")

EARLIEST_DATE = "2024-04-01"
INSIGHTS_CHUNK_DAYS = int(os.environ.get("META_INSIGHTS_CHUNK_DAYS", "28"))

# ── Fields ────────────────────────────────────────────────────────────────────
CAMPAIGN_FIELDS = [
    "date_start",
    "account_id",
    "account_name",
    "campaign_id",
    "campaign_name",
    "objective",
    "impressions",
    "clicks",
    "spend",
    "reach",
    "frequency",
    "cpm",
    "cpc",
    "ctr",
    "actions",
    "action_values",
    "conversions",
    "conversion_values",
    "cost_per_action_type",
    "video_play_actions",
    "video_thruplay_watched_actions",
]

ADSET_FIELDS = [
    "date_start",
    "account_id",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "optimization_goal",
    "impressions",
    "clicks",
    "spend",
    "reach",
    "frequency",
    "cpm",
    "cpc",
    "ctr",
    "actions",
    "action_values",
    "conversions",
    "conversion_values",
    "cost_per_action_type",
]

AD_FIELDS = [
    "date_start",
    "account_id",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "impressions",
    "clicks",
    "spend",
    "reach",
    "cpm",
    "cpc",
    "ctr",
    "actions",
    "action_values",
    "conversions",
    "conversion_values",
]

# ── Schemas ───────────────────────────────────────────────────────────────────
SF = bigquery.SchemaField

BASE_SCHEMA = [
    SF("date",              "DATE"),
    SF("account_id",        "STRING"),
    SF("account_name",      "STRING"),
    SF("impressions",       "INTEGER"),
    SF("clicks",            "INTEGER"),
    SF("spend",             "FLOAT"),
    SF("reach",             "INTEGER"),
    SF("frequency",         "FLOAT"),
    SF("cpm",               "FLOAT"),
    SF("cpc",               "FLOAT"),
    SF("ctr",               "FLOAT"),
    SF("purchases",         "INTEGER"),
    SF("purchase_value",    "FLOAT"),
    SF("adds_to_cart",      "INTEGER"),
    SF("checkouts",         "INTEGER"),
    SF("link_clicks",       "INTEGER"),
    SF("post_engagement",   "INTEGER"),
    SF("video_plays",       "INTEGER"),
    SF("video_thruplays",   "INTEGER"),
    SF("_loaded_at",        "TIMESTAMP"),
]

CAMPAIGNS_SCHEMA = [
    SF("campaign_id",   "STRING"),
    SF("campaign_name", "STRING"),
    SF("objective",     "STRING"),
] + BASE_SCHEMA

ADSETS_SCHEMA = [
    SF("campaign_id",       "STRING"),
    SF("campaign_name",     "STRING"),
    SF("adset_id",          "STRING"),
    SF("adset_name",        "STRING"),
    SF("optimization_goal", "STRING"),
] + BASE_SCHEMA

ADS_SCHEMA = [
    SF("campaign_id",   "STRING"),
    SF("campaign_name", "STRING"),
    SF("adset_id",      "STRING"),
    SF("adset_name",    "STRING"),
    SF("ad_id",         "STRING"),
    SF("ad_name",       "STRING"),
] + BASE_SCHEMA

# ── Helpers ───────────────────────────────────────────────────────────────────
bq = bigquery.Client(project=BQ_PROJECT)

def get_action(actions, action_type):
    """Extract a specific action type value from the actions list."""
    if not actions:
        return 0
    for a in actions:
        if a.get("action_type") == action_type:
            return int(float(a.get("value", 0)))
    return 0

def get_action_value(action_values, action_type):
    """Extract a specific action value from the action_values list."""
    if not action_values:
        return 0.0
    for a in action_values:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0))
    return 0.0

def parse_base(row, loaded_at):
    actions       = row.get("actions", [])
    action_values = row.get("action_values", [])
    return {
        "date":            row.get("date_start"),
        "account_id":      row.get("account_id"),
        "account_name":    row.get("account_name", ""),
        "impressions":     int(row.get("impressions", 0)),
        "clicks":          int(row.get("clicks", 0)),
        "spend":           float(row.get("spend", 0)),
        "reach":           int(row.get("reach", 0)),
        "frequency":       float(row.get("frequency", 0)),
        "cpm":             float(row.get("cpm", 0)),
        "cpc":             float(row.get("cpc", 0)),
        "ctr":             float(row.get("ctr", 0)),
        "purchases":       get_action(actions, "purchase"),
        "purchase_value":  get_action_value(action_values, "purchase"),
        "adds_to_cart":    get_action(actions, "add_to_cart"),
        "checkouts":       get_action(actions, "initiate_checkout"),
        "link_clicks":     get_action(actions, "link_click"),
        "post_engagement": get_action(actions, "post_engagement"),
        "video_plays":     get_action(actions, "video_play"),
        "video_thruplays": int(float((row.get("video_thruplay_watched_actions") or [{}])[0].get("value", 0))) if row.get("video_thruplay_watched_actions") else 0,
        "_loaded_at":      loaded_at,
    }

def schema_columns(schema):
    return ", ".join(f"`{field.name}`" for field in schema)

def append_with_date_replace(table_id, table, rows, schema, start_date, end_date):
    bq.get_table(table_id)

    temp_table_id = f"{BQ_PROJECT}.{BQ_DATASET}._tmp_{table}_{int(time.time() * 1000)}"
    try:
        load_job = bq.load_table_from_json(rows, temp_table_id, job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        ))
        load_job.result()

        columns = schema_columns(schema)
        replace_sql = f"""
        BEGIN TRANSACTION;

        DELETE FROM `{table_id}`
        WHERE date BETWEEN @start_date AND @end_date;

        INSERT INTO `{table_id}` ({columns})
        SELECT {columns}
        FROM `{temp_table_id}`;

        COMMIT TRANSACTION;
        """
        replace_job = bq.query(
            replace_sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]),
        )
        replace_job.result()
    finally:
        bq.delete_table(temp_table_id, not_found_ok=True)

def delete_date_range(table_id, start_date, end_date):
    bq.get_table(table_id)
    delete_job = bq.query(
        f"DELETE FROM `{table_id}` WHERE date BETWEEN @start_date AND @end_date",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]),
    )
    delete_job.result()

def load_to_bq(table, rows, schema, mode, start_date=None, end_date=None):
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"

    if not rows:
        if mode == bigquery.WriteDisposition.WRITE_APPEND and start_date and end_date:
            try:
                delete_date_range(table_id, start_date, end_date)
                print(f"  ✅ {table_id} — cleared {start_date} → {end_date}; no fresh rows")
                return
            except NotFound:
                pass
        print(f"  ⚠️  {table} — no rows")
        return

    if mode == bigquery.WriteDisposition.WRITE_APPEND and start_date and end_date:
        try:
            append_with_date_replace(table_id, table, rows, schema, start_date, end_date)
            print(f"  ✅ {table_id} — replaced {start_date} → {end_date}; {bq.get_table(table_id).num_rows:,} rows")
            return
        except NotFound:
            pass

    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=mode,
        ignore_unknown_values=True,
    ))
    job.result()
    print(f"  ✅ {table_id} — {bq.get_table(table_id).num_rows:,} rows")

def get_max_date(table):
    try:
        result = list(bq.query(
            f"SELECT CAST(MAX(date) AS STRING) AS max_date FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`"
        ).result())
        val = result[0].max_date if result else None
        if val:
            print(f"  Resuming from: {val}")
        return val
    except Exception:
        return None

def date_windows(start_date, end_date, chunk_days):
    """Yield inclusive date windows no larger than chunk_days."""
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    final = datetime.strptime(end_date, "%Y-%m-%d").date()
    while current <= final:
        window_end = min(current + timedelta(days=chunk_days - 1), final)
        yield str(current), str(window_end)
        current = window_end + timedelta(days=1)

# ── Insights fetcher ──────────────────────────────────────────────────────────
def fetch_insights_window(account_id, fields, level, start_date, end_date):
    """Fetch insights for one already-sized date window."""
    account = AdAccount(account_id)
    params = {
        "level":           level,
        "time_range":      {"since": start_date, "until": end_date},
        "time_increment":  1,  # daily
        "fields":          fields,
        "use_unified_attribution_setting": True,
        "action_attribution_windows": ["1d_click", "1d_view"],
        "limit":           100,
    }
    rows = []
    cursor = account.get_insights(params=params)
    while cursor:
        for row in cursor:
            rows.append(dict(row))
        try:
            cursor = cursor.load_next_page()
        except Exception:
            break
    return rows

def fetch_insights(account_id, fields, level, start_date, end_date):
    """Fetch insights for a given account and level in API-friendly date chunks."""
    rows = []
    windows = list(date_windows(start_date, end_date, INSIGHTS_CHUNK_DAYS))
    for window_start, window_end in windows:
        if len(windows) > 1:
            print(f"\n      {level} window {window_start} → {window_end}...", end="", flush=True)
        try:
            window_rows = fetch_insights_window(account_id, fields, level, window_start, window_end)
        except Exception:
            start = datetime.strptime(window_start, "%Y-%m-%d").date()
            end = datetime.strptime(window_end, "%Y-%m-%d").date()
            if start == end:
                raise

            midpoint = start + ((end - start) // 2)
            left_rows = fetch_insights(account_id, fields, level, str(start), str(midpoint))
            right_rows = fetch_insights(account_id, fields, level, str(midpoint + timedelta(days=1)), str(end))
            window_rows = left_rows + right_rows

        rows.extend(window_rows)
        if len(windows) > 1:
            print(f" {len(window_rows):,}", end="", flush=True)
    return rows

# ── Main runner ───────────────────────────────────────────────────────────────
def run(start_date, end_date, write_mode, loaded_at):
    all_campaigns, all_adsets, all_ads = [], [], []
    fetch_errors = []
    _start = time.time()

    for account_id in ACCOUNT_IDS:
        account_id = account_id.strip()
        if not account_id:
            continue
        print(f"\n  Account: {account_id}")

        try:
            # Campaigns
            print("    Fetching campaigns...", end="", flush=True)
            rows = fetch_insights(account_id, CAMPAIGN_FIELDS, "campaign", start_date, end_date)
            print(f" {len(rows):,} campaign-days")
            for row in rows:
                r = parse_base(row, loaded_at)
                r.update({
                    "campaign_id":   row.get("campaign_id"),
                    "campaign_name": row.get("campaign_name"),
                    "objective":     row.get("objective"),
                })
                all_campaigns.append(r)

            # Ad sets
            print("    Fetching ad sets...", end="", flush=True)
            rows = fetch_insights(account_id, ADSET_FIELDS, "adset", start_date, end_date)
            print(f" {len(rows):,} adset-days")
            for row in rows:
                r = parse_base(row, loaded_at)
                r.update({
                    "campaign_id":       row.get("campaign_id"),
                    "campaign_name":     row.get("campaign_name"),
                    "adset_id":          row.get("adset_id"),
                    "adset_name":        row.get("adset_name"),
                    "optimization_goal": row.get("optimization_goal"),
                })
                all_adsets.append(r)

            # Ads
            print("    Fetching ads...", end="", flush=True)
            rows = fetch_insights(account_id, AD_FIELDS, "ad", start_date, end_date)
            print(f" {len(rows):,} ad-days")
            for row in rows:
                r = parse_base(row, loaded_at)
                r.update({
                    "campaign_id":   row.get("campaign_id"),
                    "campaign_name": row.get("campaign_name"),
                    "adset_id":      row.get("adset_id"),
                    "adset_name":    row.get("adset_name"),
                    "ad_id":         row.get("ad_id"),
                    "ad_name":       row.get("ad_name"),
                })
                all_ads.append(r)

        except Exception as e:
            fetch_errors.append(account_id)
            print(f"  ❌ {account_id} — {e}")
            continue

        time.sleep(1)  # rate limit courtesy

    print(f"\n  campaigns: {len(all_campaigns):,} | adsets: {len(all_adsets):,} | ads: {len(all_ads):,}")
    elapsed = int(time.time() - _start)
    print(f"  Elapsed: {elapsed//60}m {elapsed%60}s")
    if fetch_errors:
        failed_accounts = ", ".join(fetch_errors)
        raise RuntimeError(f"Fetch failed for {failed_accounts}; BigQuery load skipped to avoid partial data")

    print("\n💾 Loading to BigQuery...")
    load_to_bq("meta_ads_campaigns_daily", all_campaigns, CAMPAIGNS_SCHEMA, write_mode, start_date, end_date)
    load_to_bq("meta_ads_adsets_daily",    all_adsets,    ADSETS_SCHEMA,    write_mode, start_date, end_date)
    load_to_bq("meta_ads_ads_daily",       all_ads,       ADS_SCHEMA,       write_mode, start_date, end_date)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not ACCESS_TOKEN:
        print("\u274c access_token not set in config.ini [meta_ads] or META_ACCESS_TOKEN env var")
        sys.exit(1)

    FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--start", default=EARLIEST_DATE, help="Backfill start date (YYYY-MM-DD)")
    args = parser.parse_args()

    yesterday = date.today() - timedelta(days=1)

    if args.mode == "incremental":
        max_date   = get_max_date("meta_ads_campaigns_daily")
        start_date = str((datetime.strptime(max_date, "%Y-%m-%d").date() - timedelta(days=2))) if max_date else str(date.today() - timedelta(days=7))
        loaded_at  = datetime.now(timezone.utc).isoformat()
        write_mode = bigquery.WriteDisposition.WRITE_APPEND
        print(f"\U0001f680 Meta Ads incremental: {start_date} \u2192 {yesterday}")
        print(f"   Accounts: {', '.join(a.strip() for a in ACCOUNT_IDS)}")
        print(f"   Project:  {BQ_PROJECT}.{BQ_DATASET}")
        run(start_date, str(yesterday), write_mode, loaded_at)
    else:
        # Backfill in monthly chunks to avoid Meta API 500s
        chunk_start = datetime.strptime(args.start, "%Y-%m-%d").date()
        print(f"\U0001f680 Meta Ads backfill: {chunk_start} \u2192 {yesterday} (monthly chunks)")
        print(f"   Accounts: {', '.join(a.strip() for a in ACCOUNT_IDS)}")
        print(f"   Project:  {BQ_PROJECT}.{BQ_DATASET}")
        first = True
        while chunk_start <= yesterday:
            # End of month
            if chunk_start.month == 12:
                chunk_end = date(chunk_start.year + 1, 1, 1) - timedelta(days=1)
            else:
                chunk_end = date(chunk_start.year, chunk_start.month + 1, 1) - timedelta(days=1)
            chunk_end = min(chunk_end, yesterday)
            write_mode = bigquery.WriteDisposition.WRITE_TRUNCATE if first else bigquery.WriteDisposition.WRITE_APPEND
            loaded_at  = datetime.now(timezone.utc).isoformat()
            print(f"\n\U0001f4c5 Chunk: {chunk_start} \u2192 {chunk_end}")
            run(str(chunk_start), str(chunk_end), write_mode, loaded_at)
            chunk_start = chunk_end + timedelta(days=1)
            first = False

    print("\n\u2705 Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
