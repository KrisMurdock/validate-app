# -*- coding: utf-8 -*-
"""
Multi-source A-share K-line data provider.

Primary: EastMoney (push2his.eastmoney.com)
Fallback: Baidu (finance.pae.baidu.com)

Supports k_type: 1=daily, 5=5min, 15=15min, 30=30min, 60=60min
All data is qfq (前复权) adjusted.
"""

import time

import numpy as np
import pandas as pd

from src.utils.adata_utils import (
    BAIDU_JSON_HEADERS,
    EASTMONEY_HEADERS,
    compile_exchange_by_stock_code,
    raw_to_baidu,
    raw_to_eastmoney,
    request,
)


class EastmoneyProvider:
    """A-share stock data from EastMoney with Baidu fallback."""

    _KLINE_COLUMNS = ['trade_time', 'open', 'close', 'high', 'low', 'volume', 'amount']
    _BAIDU_MARKET_COLUMNS = [
        'trade_time', 'open', 'close', 'volume', 'high', 'low', 'amount',
        'change', 'change_pct', 'turnover_ratio', 'pre_close',
    ]

    def __init__(self):
        self.last_error = ""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_kline(self, stock_code, start_date, end_date, k_type=1):
        """
        Get historical K-line data for a single stock.

        Args:
            stock_code: 6-digit code (e.g. '000001', '600036')
            start_date: 'YYYY-MM-DD' or 'YYYYMMDD'
            end_date: 'YYYY-MM-DD' or 'YYYYMMDD'
            k_type: 1=daily, 5=5min, 15=15min, 30=30min, 60=60min

        Returns:
            pd.DataFrame with columns: trade_time, open, close, high, low, volume, amount
        """
        stock_code = str(stock_code).strip()

        # Normalize date formats
        start_date = str(start_date).replace('-', '')
        end_date = str(end_date).replace('-', '')

        df = self._fetch_eastmoney(stock_code, start_date, end_date, k_type)
        if df.empty:
            self.last_error = f"eastmoney returned empty for {stock_code} k_type={k_type}"
            df = self._fetch_baidu(stock_code, start_date, end_date, k_type)
        else:
            self.last_error = ""

        if df.empty:
            return pd.DataFrame(columns=self._KLINE_COLUMNS)

        # Standardize output
        df['trade_time'] = pd.to_datetime(df['trade_time'], errors='coerce')
        for c in ['open', 'close', 'high', 'low']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        if 'volume' in df.columns:
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)

        result = df[self._KLINE_COLUMNS].dropna(subset=['trade_time']).copy()
        result = result.sort_values('trade_time').reset_index(drop=True)
        return result

    # ------------------------------------------------------------------
    # EastMoney K-line API
    # ------------------------------------------------------------------

    def _eastmoney_klt(self, k_type):
        """Convert k_type to EastMoney klt parameter."""
        kt = int(k_type)
        if kt < 5:
            return f"10{kt}"
        return str(kt)

    def _fetch_eastmoney(self, stock_code, start_date, end_date, k_type):
        """
        EastMoney K-line API.

        GET http://push2his.eastmoney.com/api/qt/stock/kline/get
        (HTTP is more reliable than HTTPS for this endpoint)
        """
        secid = raw_to_eastmoney(stock_code)
        klt = self._eastmoney_klt(k_type)

        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "klt": klt,
            "fqt": "1",  # 前复权
            "secid": secid,
            "beg": start_date,
            "end": end_date,
            "_": str(int(time.time() * 1000)),
        }

        for attempt in range(3):
            try:
                r = request('get',
                            url="http://push2his.eastmoney.com/api/qt/stock/kline/get",
                            params=params,
                            headers=dict(EASTMONEY_HEADERS),
                            times=1,
                            retry_wait_time=2000,
                            timeout=30)
                data_json = r.json()

                if not data_json.get("data") or not data_json["data"].get("klines"):
                    return pd.DataFrame()

                lines = data_json["data"]["klines"]
                if not lines:
                    return pd.DataFrame()

                data = [item.split(",") for item in lines]

                # EastMoney returns: date, open, close, high, low, volume, amount, _, change_pct, change, turnover_ratio
                df = pd.DataFrame(data=data, columns=[
                    "trade_date", "open", "close", "high", "low", "volume", "amount",
                    "", "change_pct", "change", "turnover_ratio",
                ])

                # Clean and transform
                df['volume'] = df['volume'].astype(float).astype('int64') * 100  # 手 → 股
                df['trade_time'] = pd.to_datetime(df['trade_date'])
                df['stock_code'] = stock_code

                return df[['stock_code', 'trade_time', 'open', 'close', 'high', 'low', 'volume', 'amount']]

            except Exception as e:
                self.last_error = f"eastmoney request failed (attempt {attempt+1}): {e}"
                time.sleep(2)

        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Baidu K-line API (fallback)
    # ------------------------------------------------------------------

    def _fetch_baidu(self, stock_code, start_date, end_date, k_type):
        """
        Baidu K-line API (fallback).

        GET https://finance.pae.baidu.com/selfselect/getstockquotation
        with group=quotation_kline_ab
        """
        baidu_code = raw_to_baidu(stock_code)
        api_url = (
            f"https://finance.pae.baidu.com/selfselect/getstockquotation"
            f"?all=1&isIndex=false&isBk=false&isBlock=false&isFutures=false"
            f"&isStock=true&newFormat=1&group=quotation_kline_ab&finClientType=pc"
            f"&code={baidu_code}&start_time={start_date}&ktype={k_type}"
        )

        for attempt in range(3):
            try:
                res = request('get', api_url, headers=dict(BAIDU_JSON_HEADERS),
                             times=1, timeout=15)
                res_json = res.json()
                if res_json.get('ResultCode') != '0':
                    time.sleep(2)
                    continue

                result = res_json.get('Result')
                if not result or 'newMarketData' not in result:
                    return pd.DataFrame()

                market_info = result['newMarketData']
                keys = market_info.get('keys', [])
                market_data = market_info.get('marketData', '')
                if not market_data:
                    return pd.DataFrame()

                rows = [r.split(',') for r in str(market_data).split(';') if r]
                if not rows:
                    return pd.DataFrame()

                df = pd.DataFrame(data=rows, columns=keys)

                rename_map = {
                    'time': 'trade_time',
                    'turnoverratio': 'turnover_ratio',
                    'preClose': 'pre_close',
                    'range': 'change',
                    'ratio': 'change_pct',
                }
                existing_rename = {k: v for k, v in rename_map.items() if k in df.columns}
                df = df.rename(columns=existing_rename)

                # Filter to available columns
                avail_cols = [c for c in self._BAIDU_MARKET_COLUMNS if c in df.columns]
                if not avail_cols:
                    return pd.DataFrame()

                df = df[avail_cols]
                df['stock_code'] = stock_code
                df['trade_time'] = pd.to_datetime(df['trade_time'])

                # Clean: remove '--' and '' placeholders
                df = df.replace('--', None).replace('', None)
                for c in ['open', 'close', 'high', 'low', 'amount', 'volume']:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors='coerce')
                for c in ['change', 'change_pct']:
                    if c in df.columns:
                        df[c] = df[c].astype(str).str.replace('+', '')
                        df[c] = pd.to_numeric(df[c], errors='coerce')

                # Drop rows with zero volume AND zero amount
                if 'volume' in df.columns and 'amount' in df.columns:
                    df = df[(df['amount'].fillna(0) > 0) | (df['volume'].fillna(0) > 0)]

                return df

            except Exception as e:
                self.last_error = f"baidu request failed (attempt {attempt+1}): {e}"
                time.sleep(2)

        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Stock list
    # ------------------------------------------------------------------

    def get_stock_list(self):
        """
        Get all A-share stock codes from EastMoney.

        Returns:
            list of 6-digit code strings
        """
        return self._fetch_stock_list_eastmoney()

    def _fetch_stock_list_eastmoney(self):
        """
        EastMoney market rank API for full stock list.

        GET https://82.push2.eastmoney.com/api/qt/clist/get
        """
        data = []
        try:
            curr_page = 1
            page_size = 50
            while curr_page < 200:
                params = {
                    "pn": curr_page, "pz": page_size,
                    "po": "1", "np": "1",
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": "2", "invt": "2", "fid": "f3",
                    "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
                    "fields": "f12,f14",
                    "_": str(int(time.time() * 1000)),
                }
                r = request('get', url="http://82.push2.eastmoney.com/api/qt/clist/get",
                           params=params, headers=dict(EASTMONEY_HEADERS),
                           times=3, retry_wait_time=2000, timeout=15, wait_time=100)
                data_json = r.json()
                p_data = data_json.get("data", {}).get("diff", [])
                if not p_data:
                    break
                data.extend(p_data)
                if len(p_data) < page_size:
                    break
                curr_page += 1
        except Exception as e:
            self.last_error = f"stock list fetch failed: {e}"

        if not data:
            return []

        df = pd.DataFrame(data=data)
        codes = df['f12'].astype(str).str.strip().tolist()
        return sorted(set(codes))
