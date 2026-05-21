#!/usr/bin/env python3
"""
Download historical A-share stock data and store it in DuckDB.

Data sources (auto-fallback on failure):
  - baostock (primary): daily + 5/15/30/60min data, no token needed
  - akshare  (fallback): daily + 1min data, no token needed

Architecture:
  - ProcessPoolExecutor with chunked stock lists (50 stocks/chunk)
  - Each worker process logs into baostock ONCE per chunk → big overhead reduction
  - Main process writes to DuckDB serially as results stream in

Usage:
  source .venv/bin/activate
  python scripts/download_stock_data.py --years 15 --workers 8
  python scripts/download_stock_data.py --years 10 --codes 000001,600519 --interval daily
  python scripts/download_stock_data.py --years 15 --start-from 301227 --workers 8
"""

import argparse
import os
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

CHUNK_SIZE = 50  # stocks per process task (reuses one baostock session)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Download A-share historical data into DuckDB")
    p.add_argument("--years", type=int, default=15)
    p.add_argument("--db-path", type=str, default=None)
    p.add_argument("--codes", type=str, default=None)
    p.add_argument("--interval", type=str, default="all",
                   choices=["daily", "minute", "all"])
    p.add_argument("--start-from", type=str, default=None)
    p.add_argument("--workers", type=int, default=6,
                   help="Concurrent download processes (default: 6, max recommended: 8)")
    p.add_argument("--delay", type=float, default=0.05)
    p.add_argument("--akshare-delay", type=float, default=0.5)
    p.add_argument("--chunk-size", type=int, default=CHUNK_SIZE,
                   help=f"Stocks per process task (default: {CHUNK_SIZE})")
    p.add_argument("--akshare-min-days", type=int, default=60,
                   help="Max days of 1-min data from akshare (default: 60, max: 730)")
    p.add_argument("--no-skip-existing", action="store_true",
                   help="Re-download all data even if already in database")
    return p.parse_args()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_config_db_path():
    try:
        from src.utils.config_loader import ConfigLoader
        cfg = ConfigLoader.reload()
        path = str(cfg.get("data_provider.duckdb_path", "")).strip()
        if path:
            return path
    except Exception:
        pass
    return os.path.join(PROJECT_ROOT, "data", "quantifydata.duckdb")


def resolve_db_path(raw_path):
    path = str(raw_path or "").strip()
    if not path:
        return None
    if path == ":memory:" or os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


def ensure_dir(db_path):
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)


TABLE_DDL = """
CREATE TABLE IF NOT EXISTS "{table}" (
    code VARCHAR NOT NULL,
    trade_time TIMESTAMP NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    vol BIGINT NOT NULL,
    amount DOUBLE NOT NULL,
    PRIMARY KEY (code, trade_time)
)
"""

UPSERT_SQL = """
INSERT INTO "{table}" (code, trade_time, open, high, low, close, vol, amount)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (code, trade_time) DO UPDATE SET
    open=EXCLUDED.open,
    high=EXCLUDED.high,
    low=EXCLUDED.low,
    close=EXCLUDED.close,
    vol=EXCLUDED.vol,
    amount=EXCLUDED.amount
"""

ALL_TABLES = ["dat_day", "dat_1mins", "dat_5mins", "dat_10mins",
              "dat_15mins", "dat_30mins", "dat_60mins"]

BAOSTOCK_MINUTE_FREQ = {"5": "dat_5mins", "15": "dat_15mins", "30": "dat_30mins", "60": "dat_60mins"}

# baostock datetime format: "2024-01-15 093000"
BS_DT_FORMAT = "%Y-%m-%d %H%M%S"


# ---------------------------------------------------------------------------
# Code conversion
# ---------------------------------------------------------------------------

def raw_to_system(code):
    c = str(code).strip()
    if c.startswith("sh.") or c.startswith("sz."):
        c = c[3:]
    if "." in c:
        return c.upper()
    if c.startswith("6") or c.startswith("9"):
        return f"{c}.SH"
    return f"{c}.SZ"


def raw_to_baostock(code):
    c = str(code).strip()
    if c.startswith("sh.") or c.startswith("sz."):
        return c.lower()
    if "." in c:
        parts = c.split(".")
        return f"{parts[1].lower()}.{parts[0]}"
    if c.startswith("6") or c.startswith("9"):
        return f"sh.{c}"
    return f"sz.{c}"


def baostock_to_raw(bs_code):
    c = str(bs_code).strip()
    if "." in c:
        return c.split(".")[1]
    return c


def raw_to_akshare(code):
    c = str(code).strip()
    if c.startswith("sh.") or c.startswith("sz."):
        return c[3:]
    if "." in c:
        return c.split(".")[0]
    return c


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------

def retry(func, *args, max_retries=3, base_delay=2.0, **kwargs):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(base_delay ** attempt)
    raise last_err


# ---------------------------------------------------------------------------
# Stock list
# ---------------------------------------------------------------------------

def get_stock_list_baostock():
    import baostock as bs
    bs.login()
    try:
        rs = bs.query_stock_basic()
        codes = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            if row[4] == '1' and row[5] == '1':
                codes.append(baostock_to_raw(row[0]))
        return codes
    finally:
        bs.logout()


def get_stock_list_akshare():
    import akshare as ak
    for api_func in [
        lambda: ak.stock_zh_a_spot_em(),
        lambda: ak.stock_info_a_code_name(),
    ]:
        try:
            df = api_func()
            if df is None or df.empty:
                continue
            for col in ["代码", "code", "A股代码"]:
                if col in df.columns:
                    codes = [str(row.get(col, "")).strip() for _, row in df.iterrows()
                             if len(str(row.get(col, "")).strip()) == 6
                             and str(row.get(col, "")).strip().isdigit()]
                    if codes:
                        return codes
        except Exception:
            continue
    return []


def get_stock_list():
    print("Fetching A-share stock list...")
    try:
        codes = retry(get_stock_list_baostock, max_retries=2, base_delay=3.0)
        if codes:
            print(f"  [baostock] {len(codes)} stocks")
            return sorted(codes)
    except Exception as e:
        print(f"  baostock failed: {e}")
    codes = get_stock_list_akshare()
    if codes:
        print(f"  [akshare]  {len(codes)} stocks")
        return sorted(codes)
    print("ERROR: All stock list sources failed")
    sys.exit(1)


# ---------------------------------------------------------------------------
# DataFrame ↔ records (cross-process safe)
# ---------------------------------------------------------------------------

def df_to_records(df):
    if df is None or df.empty:
        return []
    # Convert trade_time to string to avoid pickle issues with Timestamp
    if "trade_time" in df.columns:
        df = df.copy()
        df["trade_time"] = df["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df.to_dict("records")


# ---------------------------------------------------------------------------
# Data fetchers (module-level for pickle)
# ---------------------------------------------------------------------------

def _fetch_daily_baostock(bs, bs_code, start_date, end_date):
    rs = bs.query_history_k_data_plus(
        bs_code, "date,open,high,low,close,volume,amount",
        start_date=start_date, end_date=end_date, frequency="d", adjustflag="2",
    )
    if rs.error_code != '0':
        raise RuntimeError(f"baostock daily error: {rs.error_msg}")
    rows = []
    while (rs.error_code == '0') & rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["trade_time", "open", "high", "low", "close", "vol", "amount"])
    df["trade_time"] = pd.to_datetime(df["trade_time"], format="%Y-%m-%d", errors="coerce")
    for c in ["open", "high", "low", "close", "vol", "amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["trade_time", "open", "high", "low", "close", "vol", "amount"]].dropna(subset=["trade_time"])


def _fetch_daily_akshare(ak_code, start_date, end_date):
    import akshare as ak
    df = ak.stock_zh_a_hist(symbol=ak_code, period="daily",
                            start_date=start_date, end_date=end_date, adjust="qfq")
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={
        "日期": "trade_time", "开盘": "open", "收盘": "close",
        "最高": "high", "最低": "low", "成交量": "vol", "成交额": "amount",
    })
    df["trade_time"] = pd.to_datetime(df["trade_time"], format="%Y-%m-%d", errors="coerce")
    for c in ["open", "high", "low", "close", "vol", "amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    keep = [c for c in ["trade_time", "open", "high", "low", "close", "vol", "amount"] if c in df.columns]
    return df[keep].dropna(subset=["trade_time"])


def _fetch_minute_1min_akshare(ak_code, start_date, end_date):
    import akshare as ak
    start_str = f"{start_date} 09:00:00"
    end_str = f"{end_date} 15:30:00"
    df = ak.stock_zh_a_hist_min_em(symbol=ak_code, period="1",
                                   start_date=start_str, end_date=end_str, adjust="qfq")
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={
        "时间": "trade_time", "开盘": "open", "收盘": "close",
        "最高": "high", "最低": "low", "成交量": "vol", "成交额": "amount",
    })
    df["trade_time"] = pd.to_datetime(df["trade_time"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    for c in ["open", "high", "low", "close", "vol", "amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    keep = [c for c in ["trade_time", "open", "high", "low", "close", "vol", "amount"] if c in df.columns]
    return df[keep].dropna(subset=["trade_time"])


def _fetch_minute_baostock(bs, bs_code, start_date, end_date, freq):
    rs = bs.query_history_k_data_plus(
        bs_code, "date,time,open,high,low,close,volume,amount",
        start_date=start_date, end_date=end_date, frequency=freq, adjustflag="2",
    )
    if rs.error_code != '0':
        raise RuntimeError(f"baostock {freq}min error: {rs.error_msg}")
    rows = []
    while (rs.error_code == '0') & rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["date", "time", "open", "high", "low", "close", "vol", "amount"])
    df["trade_time"] = pd.to_datetime(df["date"] + " " + df["time"],
                                      format=BS_DT_FORMAT, errors="coerce")
    for c in ["open", "high", "low", "close", "vol", "amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["trade_time", "open", "high", "low", "close", "vol", "amount"]].dropna(subset=["trade_time"])


# ---------------------------------------------------------------------------
# Chunk processor — runs in worker PROCESS (1 baostock session per chunk)
# ---------------------------------------------------------------------------

def process_chunk(codes_chunk, dates, interval, delay, akshare_delay,
                  code_daily_start=None, code_minute_start=None):
    """
    Download data for a chunk of stocks in one process.
    Baostock session is opened ONCE and reused across all stocks in the chunk.

    code_daily_start: dict raw_code -> "YYYY-MM-DD" or None (skip daily)
    code_minute_start: dict raw_code -> "YYYY-MM-DD" or None (skip minute)

    Returns: list of (sys_code, {table: [records]}, error_str_or_None)
    """
    import random

    # Stagger startup to avoid connection storm on baostock server
    stagger_s = random.uniform(0.5, 3.0)
    time.sleep(stagger_s)

    import baostock as bs

    lg = bs.login()
    bs_available = (lg.error_code == '0')
    results = []

    chunk_total = len(codes_chunk)
    first_code = raw_to_system(codes_chunk[0] if codes_chunk else "?")
    print(f"  [worker] chunk starting: {chunk_total} stocks, first={first_code}", flush=True)

    backpressure = 0.0

    for idx, raw_code in enumerate(codes_chunk):
        sys_code = raw_to_system(raw_code)
        bs_code = raw_to_baostock(raw_code)
        ak_code = raw_to_akshare(raw_code)
        tables = {}
        error = None

        if idx > 0 and idx % 10 == 0:
            print(f"  [worker] {idx}/{chunk_total} stocks done...", flush=True)

        # Determine per-code start dates
        daily_start = dates["start_dash"]
        if code_daily_start is not None:
            cs = code_daily_start.get(raw_code)
            if cs is None:
                daily_start = None  # skip daily for this stock
            else:
                daily_start = cs

        minute_start = dates["minute_start_dash"]
        if code_minute_start is not None:
            ms = code_minute_start.get(raw_code)
            if ms is None:
                minute_start = None  # skip minute for this stock
            else:
                minute_start = ms

        try:
            # ---- daily ----
            if interval in ("daily", "all") and daily_start is not None:
                df_daily = pd.DataFrame()
                if bs_available:
                    try:
                        df_daily = retry(_fetch_daily_baostock, bs, bs_code,
                                         daily_start, dates["end_dash"],
                                         max_retries=2, base_delay=2.0)
                    except Exception as e:
                        err_lower = str(e).lower()
                        if any(kw in err_lower for kw in ("broken pipe", "reset by peer", "connection reset")):
                            backpressure += 0.5
                if df_daily.empty:
                    try:
                        # Convert to yyyymmdd for akshare
                        ds_yyyymmdd = pd.to_datetime(daily_start).strftime("%Y%m%d")
                        time.sleep(akshare_delay)
                        df_daily = retry(_fetch_daily_akshare, ak_code,
                                         ds_yyyymmdd, dates["end_yyyymmdd"],
                                         max_retries=2, base_delay=2.0)
                    except Exception:
                        pass
                if not df_daily.empty:
                    tables["dat_day"] = df_to_records(df_daily)

            # ---- minute (5/15/30/60min via EastMoney → baostock fallback) ----
            if interval in ("minute", "all") and minute_start is not None:
                # EastMoney API is the primary source for minute K-line data
                from src.utils.eastmoney_provider import EastmoneyProvider
                em = EastmoneyProvider()
                em_freqs = {"5": "dat_5mins", "15": "dat_15mins",
                           "30": "dat_30mins", "60": "dat_60mins"}
                for freq, table in em_freqs.items():
                    try:
                        df_freq = em.get_kline(raw_code, minute_start,
                                               dates["end_dash"], k_type=int(freq))
                        if not df_freq.empty:
                            tables[table] = df_to_records(df_freq)
                    except Exception:
                        pass

                # Fall back to baostock for any freq that EastMoney missed
                for freq, table in BAOSTOCK_MINUTE_FREQ.items():
                    if table in tables and tables[table]:
                        continue  # already got data from EastMoney
                    if not bs_available:
                        break
                    try:
                        df_freq = retry(_fetch_minute_baostock, bs, bs_code,
                                        minute_start, dates["end_dash"], freq,
                                        max_retries=2, base_delay=2.0)
                        if not df_freq.empty:
                            tables[table] = df_to_records(df_freq)
                    except Exception:
                        pass

                # 1min from akshare
                df_1min = pd.DataFrame()
                try:
                    ms_yyyymmdd = pd.to_datetime(minute_start).strftime("%Y%m%d")
                    time.sleep(akshare_delay)
                    df_1min = retry(_fetch_minute_1min_akshare, ak_code,
                                    ms_yyyymmdd, dates["end_yyyymmdd"],
                                    max_retries=2, base_delay=2.0)
                    if not df_1min.empty:
                        tables["dat_1mins"] = df_to_records(df_1min)
                except Exception:
                    pass

                # Resample 1min → 5/10/15/30/60min (fill gaps from missing API data)
                if not df_1min.empty:
                    df_c = df_1min.copy()
                    df_c["trade_time"] = pd.to_datetime(df_c["trade_time"],
                                                        format="%Y-%m-%d %H:%M:%S", errors="coerce")
                    df_ts = df_c.set_index("trade_time")

                    def _resample_to(tables_dict, freq_key, rule):
                        if freq_key not in tables_dict or not tables_dict[freq_key]:
                            r = df_ts.resample(rule).agg({
                                "open": "first", "high": "max", "low": "min",
                                "close": "last", "vol": "sum", "amount": "sum",
                            }).dropna()
                            if not r.empty:
                                tables_dict[freq_key] = df_to_records(r.reset_index())

                    _resample_to(tables, "dat_5mins", "5min")
                    _resample_to(tables, "dat_10mins", "10min")
                    _resample_to(tables, "dat_15mins", "15min")
                    _resample_to(tables, "dat_30mins", "30min")
                    _resample_to(tables, "dat_60mins", "60min")

        except Exception as e:
            error = str(e)

        results.append((sys_code, tables, error))
        # Apply dynamic backpressure: slowdown when server signals overload
        effective_delay = delay + backpressure
        time.sleep(effective_delay)
        # Gradually decay backpressure
        backpressure = max(0.0, backpressure - 0.02)

    if bs_available:
        try:
            bs.logout()
        except Exception:
            pass

    return results


# ---------------------------------------------------------------------------
# DuckDB writer (main process only)
# ---------------------------------------------------------------------------

class DuckDbWriter:
    def __init__(self, db_path):
        import duckdb
        self._db_path = db_path
        self._conn = duckdb.connect(db_path, read_only=False)
        self.totals = Counter()

    def create_tables(self):
        for t in ALL_TABLES:
            self._conn.execute(TABLE_DDL.format(table=t))

    def write_overview_json(self):
        """Write data_overview.json from actual DB state (not just current session)."""
        import json as _json
        overview_path = os.path.join(os.path.dirname(self._db_path), "data_overview.json")
        tables_meta = []
        total_rows = 0
        total_stocks = 0
        for t in ALL_TABLES:
            try:
                cnt = self._conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()
                row_count = int(cnt[0]) if cnt else 0
                stock_cnt = 0
                date_min = None
                date_max = None
                if row_count > 0:
                    sc = self._conn.execute(f'SELECT COUNT(DISTINCT code) FROM "{t}"').fetchone()
                    stock_cnt = int(sc[0]) if sc else 0
                    dr = self._conn.execute(f'SELECT MIN(trade_time), MAX(trade_time) FROM "{t}"').fetchone()
                    date_min = str(dr[0])[:19] if dr and dr[0] else None
                    date_max = str(dr[1])[:19] if dr and dr[1] else None
                tables_meta.append({
                    "table": t,
                    "row_count": row_count,
                    "stock_count": stock_cnt,
                    "date_min": date_min,
                    "date_max": date_max,
                })
                total_rows += row_count
                if stock_cnt > total_stocks:
                    total_stocks = stock_cnt
            except Exception:
                tables_meta.append({
                    "table": t, "row_count": 0, "stock_count": 0,
                    "date_min": None, "date_max": None,
                })
        payload = {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "db_path": self._db_path,
            "total_rows": total_rows,
            "total_stocks": total_stocks,
            "tables": tables_meta,
        }
        try:
            with open(overview_path, "w", encoding="utf-8") as f:
                _json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def write_records(self, table, code, records):
        if not records:
            return 0
        df = pd.DataFrame.from_records(records)
        if df is None or df.empty:
            return 0
        df["code"] = str(code)
        df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
        rows = []
        for _, r in df.iterrows():
            try:
                rows.append((
                    str(r["code"]),
                    pd.to_datetime(r["trade_time"]).to_pydatetime(),
                    float(r["open"]),
                    float(r["high"]),
                    float(r["low"]),
                    float(r["close"]),
                    int(float(r["vol"])) if pd.notna(r.get("vol")) else 0,
                    float(r["amount"]) if pd.notna(r.get("amount")) else 0.0,
                ))
            except Exception:
                continue
        if not rows:
            return 0
        self._conn.executemany(UPSERT_SQL.format(table=table), rows)
        self.totals[table] += len(rows)
        return len(rows)

    def commit(self):
        self._conn.commit()

    def release(self):
        """Commit and release the file lock so other processes can read."""
        try:
            self._conn.commit()
            self._conn.close()
        except Exception:
            pass

    def ensure_connected(self):
        import duckdb
        self._conn = duckdb.connect(self._db_path, read_only=False)

    def close(self):
        self.commit()
        self._conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    # --- DB setup ---
    raw_path = args.db_path or get_config_db_path()
    db_path = resolve_db_path(raw_path)
    if not db_path:
        print("ERROR: Could not determine DuckDB path.")
        sys.exit(1)

    ensure_dir(db_path)
    print(f"DuckDB path: {db_path}")

    writer = DuckDbWriter(db_path)
    writer.create_tables()
    print("Tables ensured.")

    # --- Stock list ---
    if args.codes:
        codes = sorted(set(c.strip() for c in args.codes.split(",") if c.strip()))
    else:
        codes = get_stock_list()

    if args.start_from:
        if args.start_from in codes:
            codes = codes[codes.index(args.start_from):]
        else:
            matched = [c for c in codes if c >= args.start_from]
            if matched:
                codes = matched
        print(f"Resuming from {codes[0] if codes else '?'}, {len(codes)} stocks remaining")

    # --- Date ranges ---
    today = datetime.now()
    akshare_min_cap = max(1, min(args.akshare_min_days, 730))
    minute_cap_days = min(args.years * 365, akshare_min_cap)
    dates = {
        "end_yyyymmdd":   today.strftime("%Y%m%d"),
        "start_yyyymmdd": (today - timedelta(days=args.years * 365)).strftime("%Y%m%d"),
        "end_dash":       today.strftime("%Y-%m-%d"),
        "start_dash":     (today - timedelta(days=args.years * 365)).strftime("%Y-%m-%d"),
        "minute_start_yyyymmdd": (today - timedelta(days=minute_cap_days)).strftime("%Y%m%d"),
        "minute_start_dash":     (today - timedelta(days=minute_cap_days)).strftime("%Y-%m-%d"),
    }

    # --- Pre-check: detect existing data to avoid re-downloading ---
    code_daily_start = {}   # raw_code -> "YYYY-MM-DD" or None (no data)
    code_minute_start = {}  # raw_code -> "YYYY-MM-DD" or None
    skipped_daily = 0
    skipped_minute = 0

    if not args.no_skip_existing and args.interval in ("daily", "all"):
        print("Checking existing daily data...")
        try:
            existing = writer._conn.execute(
                "SELECT code, MAX(trade_time)::DATE AS max_dt FROM dat_day GROUP BY code"
            ).fetchall()
            existing_map = {}
            for row in existing:
                code = str(row[0]).strip().upper()
                max_dt = row[1]
                existing_map[code] = max_dt
            for raw_code in codes:
                sys_code = raw_to_system(raw_code)
                max_dt = existing_map.get(sys_code)
                if max_dt:
                    next_day = pd.to_datetime(max_dt) + timedelta(days=1)
                    if next_day >= today:
                        skipped_daily += 1
                        code_daily_start[raw_code] = None  # fully up-to-date
                    else:
                        code_daily_start[raw_code] = next_day.strftime("%Y-%m-%d")
                else:
                    code_daily_start[raw_code] = dates["start_dash"]  # no data, use full range
            print(f"  Daily: {skipped_daily} up-to-date, {len(codes) - skipped_daily} need updates")
        except Exception as e:
            print(f"  Pre-check skipped (table may be empty): {e}")
            for raw_code in codes:
                code_daily_start[raw_code] = dates["start_dash"]

    if not args.no_skip_existing and args.interval in ("minute", "all"):
        print("Checking existing minute data...")
        try:
            existing = writer._conn.execute(
                "SELECT code, MAX(trade_time)::DATE AS max_dt FROM dat_1mins GROUP BY code"
            ).fetchall()
            existing_map = {}
            for row in existing:
                code = str(row[0]).strip().upper()
                max_dt = row[1]
                existing_map[code] = max_dt
            for raw_code in codes:
                sys_code = raw_to_system(raw_code)
                max_dt = existing_map.get(sys_code)
                if max_dt:
                    next_day = pd.to_datetime(max_dt) + timedelta(days=1)
                    slice_end = pd.to_datetime(dates["end_dash"])
                    if next_day >= slice_end:
                        skipped_minute += 1
                        code_minute_start[raw_code] = None  # fully up-to-date
                    else:
                        code_minute_start[raw_code] = next_day.strftime("%Y-%m-%d")
                else:
                    code_minute_start[raw_code] = dates["minute_start_dash"]
            print(f"  Minute: {skipped_minute} up-to-date, {len(codes) - skipped_minute} need updates")
        except Exception as e:
            print(f"  Pre-check skipped (table may be empty): {e}")
            for raw_code in codes:
                code_minute_start[raw_code] = dates["minute_start_dash"]

    if not args.no_skip_existing and skipped_daily == len(codes) and skipped_minute == len(codes):
        print("All stocks are up-to-date. Nothing to download.")
        writer.close()
        return

    # --- Split into chunks ---
    # Filter out fully-skipped stocks when both daily and minute are complete
    active_codes = []
    for raw_code in codes:
        need_daily = args.interval in ("daily", "all") and code_daily_start.get(raw_code) is not None
        need_minute = args.interval in ("minute", "all") and code_minute_start.get(raw_code) is not None
        if args.interval in ("daily", "all") and code_daily_start.get(raw_code) is not None:
            need_daily = True
        else:
            need_daily = False
        if args.interval in ("minute", "all") and code_minute_start.get(raw_code) is not None:
            need_minute = True
        else:
            need_minute = False
        if need_daily or need_minute or args.no_skip_existing:
            active_codes.append(raw_code)

    if not active_codes:
        print("All stocks are up-to-date. Nothing to download.")
        writer.close()
        return

    print(f"Active stocks to download: {len(active_codes)} (skipped {len(codes) - len(active_codes)} fully up-to-date)")

    chunk_size = max(1, args.chunk_size)
    code_chunks = [active_codes[i:i + chunk_size] for i in range(0, len(active_codes), chunk_size)]
    workers = max(1, min(args.workers, len(code_chunks)))

    print(f"Date range: {dates['start_yyyymmdd']} ~ {dates['end_yyyymmdd']}")
    print(f"Minute range: {dates['minute_start_yyyymmdd']} ~ {dates['end_yyyymmdd']}")
    print(f"Stock count: {len(active_codes)} | Chunks: {len(code_chunks)} ({chunk_size}/chunk)")
    print(f"Workers: {workers} processes | Interval: {args.interval}")
    if not args.no_skip_existing:
        print(f"Incremental: daily skips={skipped_daily}, minute skips={skipped_minute}")
    print("-" * 60)

    # --- Process chunks ---
    start_time = time.perf_counter()
    done_chunks = 0
    done_stocks = 0
    total = len(active_codes)

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(process_chunk, chunk, dates, args.interval,
                            args.delay, args.akshare_delay,
                            code_daily_start, code_minute_start): i
            for i, chunk in enumerate(code_chunks)
        }

        for future in as_completed(futures):
            chunk_idx = futures[future]
            chunk_size_actual = len(code_chunks[chunk_idx])
            try:
                per_stock_results = future.result()
            except Exception as e:
                done_chunks += 1
                done_stocks += chunk_size_actual
                print(f"  [chunk {chunk_idx}] CRASH: {e}")
                continue

            chunk_day = 0
            chunk_min = 0
            chunk_errors = 0
            for sys_code, tables, error in per_stock_results:
                for table, records in tables.items():
                    n = writer.write_records(table, sys_code, records)
                    if table == "dat_day":
                        chunk_day += n
                    elif table == "dat_1mins":
                        chunk_min += n
                if error:
                    chunk_errors += 1

            done_chunks += 1
            done_stocks += chunk_size_actual
            elapsed = time.perf_counter() - start_time
            rate = done_stocks / elapsed if elapsed > 0 else 0
            eta = (total - done_stocks) / rate if rate > 0 else 0

            parts = [f"day={chunk_day}"]
            if args.interval in ("minute", "all"):
                parts.append(f"1min={chunk_min}")
            parts.append(f"err={chunk_errors}")
            info = " ".join(parts)

            print(f"  [{done_stocks}/{total}] chunk {chunk_idx} "
                  f"({chunk_size_actual} stocks) -> {info} | "
                  f"{rate:.1f} stk/s | ETA {eta/60:.0f}min")

            # Write overview JSON BEFORE releasing lock (so query works)
            writer.write_overview_json()
            # Release DB lock briefly so web server can read between chunks
            writer.commit()
            writer.release()
            time.sleep(0.1)
            writer.ensure_connected()

    writer.commit()
    elapsed = time.perf_counter() - start_time

    # --- Summary ---
    print("-" * 60)
    print(f"Complete in {elapsed/60:.1f}min ({total / elapsed:.1f} stocks/s)")
    for t in ALL_TABLES:
        n = writer.totals.get(t, 0)
        if n:
            print(f"  {t}: {n:,} rows")

    writer.close()


if __name__ == "__main__":
    main()
