#!/usr/bin/env python3
"""
Terra Health Essentials — Meta Ads API → BigQuery ETL
=====================================================
Pulls daily performance data at campaign / ad set / ad level
across all Terra Meta ad accounts.

Ad accounts:
  act_994866890890084  Terra 003
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
  account_ids     = act_994866890890084,act_2219077071728671,act_461423467875645
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
)
ACCOUNT_IDS = [account_id.strip() for account_id in ACCOUNT_IDS.split(",") if account_id.strip()]
ACCOUNT_NAMES = {
    "act_994866890890084": "Terra 003",
    "act_2219077071728671": "Terra 005",
    "act_461423467875645": "Terra 002",
}

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

def account_label(account_id):
    return f"{ACCOUNT_NAMES.get(account_id, 'Unknown')} ({account_id})"

def month_end(value):
    if value.month == 12:
        return date(value.year + 1, 1, 1) - timedelta(days=1)
    return date(value.year, value.month + 1, 1) - timedelta(days=1)

def next_month_start(value):
    return month_end(value) + timedelta(days=1)

def get_coverage_max_date():
    table_max_dates = []
    for table in ("meta_ads_campaigns_daily", "meta_ads_adsets_daily", "meta_ads_ads_daily"):
        max_date = get_max_date(table)
        if not max_date:
            return None
        table_max_dates.append(datetime.strptime(max_date, "%Y-%m-%d").date())
    return min(table_max_dates)

def catchup_window(end_date):
    max_date = get_coverage_max_date()
    if max_date:
        start = next_month_start(max_date)
    else:
        start = datetime.strptime(EARLIEST_DATE, "%Y-%m-%d").date()
    if start > end_date:
        return None, None
    return start, min(month_end(start), end_date)

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

def fetch_insights(
    account_id,
    fields,
    level,
    start_date,
    end_date,
    progress_offset=0,
    total_progress=None,
    account_progress=None,
    level_progress=None,
):
    """Fetch insights for a given account and level in API-friendly date chunks."""
    rows = []
    windows = list(date_windows(start_date, end_date, INSIGHTS_CHUNK_DAYS))
    for window_index, (window_start, window_end) in enumerate(windows, start=1):
        progress_number = progress_offset + window_index
        progress_parts = []
        if total_progress:
            progress_parts.append(f"progress {progress_number}/{total_progress}")
        if account_progress:
            progress_parts.append(f"account {account_progress[0]}/{account_progress[1]}")
        if level_progress:
            progress_parts.append(f"level {level_progress[0]}/{level_progress[1]}")
        progress_parts.append(f"window {window_index}/{len(windows)}")
        progress_label = ", ".join(progress_parts)
        print(f"\n      [{progress_label}] {level} {window_start} → {window_end}...", end="", flush=True)
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
        print(f" {len(window_rows):,}", end="", flush=True)
    return rows

# ── Verify ───────────────────────────────────────────────────────────────────
def verify_mode(start_date, end_date):
    """Compare monthly spend totals between Meta API and BigQuery."""
    FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)
    accounts = [a.strip() for a in ACCOUNT_IDS if a.strip()]

    print(f"\n🔍 Verifying Meta API vs BigQuery: {start_date} → {end_date}")
    print(f"   Accounts: {', '.join(account_label(a) for a in accounts)}\n")

    # ── Fetch from Meta API (chunked to avoid 500s) ──────────────────────────
    api_rows = {}  # (month, account_id) → {spend, purchase_value}
    for account_id in accounts:
        print(f"  Fetching from Meta API: {account_label(account_id)}...")
        rows = fetch_insights(account_id, ["date_start", "account_id", "spend", "action_values"], "campaign", start_date, end_date)
        for row in rows:
            month = row.get("date_start", "")[:7]
            norm_acct = account_id.replace("act_", "")
            key = (month, norm_acct)
            if key not in api_rows:
                api_rows[key] = {"spend": 0.0, "purchase_value": 0.0}
            api_rows[key]["spend"] += float(row.get("spend", 0))
            api_rows[key]["purchase_value"] += get_action_value(row.get("action_values", []), "purchase")

    # ── Fetch from BigQuery ──────────────────────────────────────────────────
    bq_result = bq.query(f"""
        SELECT
          FORMAT_DATE('%Y-%m', date) AS month,
          account_id,
          ROUND(SUM(spend), 2) AS spend,
          ROUND(SUM(purchase_value), 2) AS purchase_value
        FROM `{BQ_PROJECT}.{BQ_DATASET}.meta_ads_campaigns_daily`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY month, account_id
        ORDER BY month, account_id
    """).result()

    bq_rows = {}
    for row in bq_result:
        bq_rows[(row.month, row.account_id)] = {
            "spend": float(row.spend or 0),
            "purchase_value": float(row.purchase_value or 0),
        }

    # ── Diff ─────────────────────────────────────────────────────────────────
    all_keys = sorted(set(api_rows) | set(bq_rows))
    has_issue = False

    print(f"\n  {'Month':<8} {'Account':<12} {'API Spend':>12} {'BQ Spend':>12} {'Delta':>10} {'Delta%':>8}  {'API Rev':>12} {'BQ Rev':>12}")
    print(f"  {'-'*8} {'-'*12} {'-'*12} {'-'*12} {'-'*10} {'-'*8}  {'-'*12} {'-'*12}")

    monthly_api, monthly_bq = {}, {}
    for key in all_keys:
        month, acct = key
        api = api_rows.get(key, {"spend": 0.0, "purchase_value": 0.0})
        bq_  = bq_rows.get(key, {"spend": 0.0, "purchase_value": 0.0})

        api_spend = round(api["spend"], 2)
        bq_spend  = round(bq_["spend"], 2)
        delta     = round(bq_spend - api_spend, 2)
        if api_spend:
            pct = round((delta / api_spend * 100), 1)
        elif bq_spend:
            pct = 100.0
        else:
            pct = 0.0
        flag      = " ⚠️" if abs(pct) > 1 else ""
        if flag:
            has_issue = True

        acct_short = ACCOUNT_NAMES.get(f"act_{acct}", ACCOUNT_NAMES.get(acct, acct))[-3:]
        print(f"  {month:<8} {acct_short:<12} {api_spend:>12,.2f} {bq_spend:>12,.2f} {delta:>+10,.2f} {pct:>7.1f}%{flag}"
              f"  {round(api['purchase_value'],2):>12,.2f} {round(bq_['purchase_value'],2):>12,.2f}")

        for d, src in [(monthly_api, api), (monthly_bq, bq_)]:
            if month not in d:
                d[month] = {"spend": 0.0, "purchase_value": 0.0}
            d[month]["spend"]          += src["spend"]
            d[month]["purchase_value"] += src["purchase_value"]

    print(f"\n  {'Month':<8} {'API Spend':>12} {'BQ Spend':>12} {'Delta':>10} {'Delta%':>8}  {'API Rev':>12} {'BQ Rev':>12}  (all accounts)")
    print(f"  {'-'*8} {'-'*12} {'-'*12} {'-'*10} {'-'*8}  {'-'*12} {'-'*12}")
    for month in sorted(set(monthly_api) | set(monthly_bq)):
        a = monthly_api.get(month, {"spend": 0.0, "purchase_value": 0.0})
        b = monthly_bq.get(month, {"spend": 0.0, "purchase_value": 0.0})
        delta = round(b["spend"] - a["spend"], 2)
        if a["spend"]:
            pct = round((delta / a["spend"] * 100), 1)
        elif b["spend"]:
            pct = 100.0
        else:
            pct = 0.0
        flag  = " ⚠️" if abs(pct) > 1 else ""
        print(f"  {month:<8} {round(a['spend'],2):>12,.2f} {round(b['spend'],2):>12,.2f} {delta:>+10,.2f} {pct:>7.1f}%{flag}"
              f"  {round(a['purchase_value'],2):>12,.2f} {round(b['purchase_value'],2):>12,.2f}")

    print(f"\n{'⚠️  Discrepancies found (>1% delta).' if has_issue else '✅ All months within 1% tolerance.'}")

# ── Main runner ───────────────────────────────────────────────────────────────
def run(start_date, end_date, write_mode, loaded_at):
    all_campaigns, all_adsets, all_ads = [], [], []
    fetch_errors = []
    _start = time.time()
    accounts = [account_id.strip() for account_id in ACCOUNT_IDS if account_id.strip()]
    levels = [
        ("campaigns", CAMPAIGN_FIELDS, "campaign", "campaign-days"),
        ("ad sets", ADSET_FIELDS, "adset", "adset-days"),
        ("ads", AD_FIELDS, "ad", "ad-days"),
    ]
    window_count = len(list(date_windows(start_date, end_date, INSIGHTS_CHUNK_DAYS)))
    total_fetch_units = len(accounts) * len(levels) * window_count

    print(f"  Fetch plan: {len(accounts)} accounts × {len(levels)} levels × {window_count} windows = {total_fetch_units} fetch units")

    for account_index, account_id in enumerate(accounts, start=1):
        print(f"\n  Account {account_index}/{len(accounts)}: {account_label(account_id)}")

        try:
            for level_index, (label, fields, level, day_label) in enumerate(levels, start=1):
                print(f"    Fetching {label}...", end="", flush=True)
                progress_offset = (
                    ((account_index - 1) * len(levels) + (level_index - 1))
                    * window_count
                )
                rows = fetch_insights(
                    account_id,
                    fields,
                    level,
                    start_date,
                    end_date,
                    progress_offset=progress_offset,
                    total_progress=total_fetch_units,
                    account_progress=(account_index, len(accounts)),
                    level_progress=(level_index, len(levels)),
                )
                print(f" {len(rows):,} {day_label}")

                for row in rows:
                    r = parse_base(row, loaded_at)
                    if level == "campaign":
                        r.update({
                            "campaign_id":   row.get("campaign_id"),
                            "campaign_name": row.get("campaign_name"),
                            "objective":     row.get("objective"),
                        })
                        all_campaigns.append(r)
                    elif level == "adset":
                        r.update({
                            "campaign_id":       row.get("campaign_id"),
                            "campaign_name":     row.get("campaign_name"),
                            "adset_id":          row.get("adset_id"),
                            "adset_name":        row.get("adset_name"),
                            "optimization_goal": row.get("optimization_goal"),
                        })
                        all_adsets.append(r)
                    else:
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
    if not ACCOUNT_IDS:
        print("\u274c account_ids not set in config.ini [meta_ads] or META_ACCOUNT_IDS env var")
        sys.exit(1)

    FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "catchup", "incremental", "verify"], default="incremental")
    parser.add_argument("--start", default=EARLIEST_DATE, help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="Optional end date (YYYY-MM-DD); defaults to yesterday")
    args = parser.parse_args()

    yesterday = date.today() - timedelta(days=1)
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else yesterday
    end_date = min(end_date, yesterday)

    if args.mode == "verify":
        verify_mode(args.start, str(end_date))
        return

    if args.mode == "incremental":
        max_date   = get_max_date("meta_ads_campaigns_daily")
        start_date = str((datetime.strptime(max_date, "%Y-%m-%d").date() - timedelta(days=2))) if max_date else str(date.today() - timedelta(days=7))
        if datetime.strptime(start_date, "%Y-%m-%d").date() > end_date:
            print(f"\u2705 Meta Ads incremental already current through {end_date}")
            return
        loaded_at  = datetime.now(timezone.utc).isoformat()
        write_mode = bigquery.WriteDisposition.WRITE_APPEND
        print(f"\U0001f680 Meta Ads incremental: {start_date} \u2192 {end_date}")
        print(f"   Accounts: {', '.join(account_label(a) for a in ACCOUNT_IDS)}")
        print(f"   Project:  {BQ_PROJECT}.{BQ_DATASET}")
        run(start_date, str(end_date), write_mode, loaded_at)
    elif args.mode == "catchup":
        start_date, catchup_end = catchup_window(end_date)
        if not start_date:
            print(f"✅ Meta Ads catchup already current through {end_date}")
            return
        loaded_at = datetime.now(timezone.utc).isoformat()
        write_mode = bigquery.WriteDisposition.WRITE_APPEND
        print(f"🚀 Meta Ads catchup: {start_date} → {catchup_end}")
        print(f"   Accounts: {', '.join(account_label(a) for a in ACCOUNT_IDS)}")
        print(f"   Project:  {BQ_PROJECT}.{BQ_DATASET}")
        run(str(start_date), str(catchup_end), write_mode, loaded_at)
    else:
        # Backfill in monthly chunks to avoid Meta API 500s
        chunk_start = datetime.strptime(args.start, "%Y-%m-%d").date()

        # Resume from last loaded month if already partially backfilled
        max_date = get_max_date("meta_ads_campaigns_daily")
        if max_date:
            bq_max = datetime.strptime(max_date, "%Y-%m-%d").date()
            # Advance to start of the next month after max loaded date
            if bq_max >= chunk_start:
                if bq_max.month == 12:
                    resume_from = date(bq_max.year + 1, 1, 1)
                else:
                    resume_from = date(bq_max.year, bq_max.month + 1, 1)
                if resume_from > chunk_start:
                    print(f"   Resuming from {resume_from} (BQ already has data through {bq_max})")
                    chunk_start = resume_from

        print(f"\U0001f680 Meta Ads backfill: {chunk_start} \u2192 {end_date} (monthly chunks)")
        print(f"   Accounts: {', '.join(account_label(a) for a in ACCOUNT_IDS)}")
        print(f"   Project:  {BQ_PROJECT}.{BQ_DATASET}")
        while chunk_start <= end_date:
            chunk_end = min(month_end(chunk_start), end_date)
            write_mode = bigquery.WriteDisposition.WRITE_APPEND
            loaded_at  = datetime.now(timezone.utc).isoformat()
            print(f"\n\U0001f4c5 Chunk: {chunk_start} \u2192 {chunk_end}")
            run(str(chunk_start), str(chunk_end), write_mode, loaded_at)
            chunk_start = chunk_end + timedelta(days=1)

    print("\n\u2705 Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
