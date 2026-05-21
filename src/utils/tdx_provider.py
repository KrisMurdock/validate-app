import json
import logging
import os
import platform
import threading
import time
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from src.utils.config_loader import ConfigLoader
from src.utils.indicators import Indicators

logger = logging.getLogger("TdxProvider")


class TdxProvider:
    """
    纯 Mootdx 数据提供器（不依赖 pytdx）。
    统一输出字段: code, dt, open, high, low, close, vol, amount
    """

    _quote_server_cache_lock = threading.Lock()
    _quote_server_cache_mem: dict[str, tuple[str, int]] = {}
    _quote_server_cache_meta_mem: dict[str, dict[str, Any]] = {}

    def __init__(self, host=None, port=None, nodes=None, tdxdir=None):
        cfg = ConfigLoader.reload()
        self.last_error = ""
        self.mootdx_market = str(cfg.get("data_provider.tdx_market", "std") or "std").strip() or "std"
        cfg_host = str(cfg.get("data_provider.tdx_host", "") or "").strip()
        cfg_port = int(cfg.get("data_provider.tdx_port", 7709) or 7709)
        explicit_host = str(host or cfg_host or "").strip()
        explicit_port = int(port or cfg_port or 7709)
        self.host = explicit_host or "119.147.212.81"
        self.port = explicit_port
        # 显式配置节点时优先尊重用户设置；未显式配置时才优先复用缓存节点。
        self._explicit_server = bool(explicit_host)
        # 读取配置中心的 TDX 超时参数；当值为空或 <=0 时回退到历史默认值 6 秒。
        configured_timeout = int(cfg.get("data_provider.tdx_timeout_sec", 0) or 0)
        self.quote_timeout_sec = max(1, configured_timeout) if configured_timeout > 0 else 6
        explicit_dir = str(tdxdir or "").strip()
        self.tdxdir = explicit_dir if explicit_dir else self._resolve_tdxdir(cfg)
        self._cache_enabled = bool(cfg.get("data_provider.local_cache_enabled", True))
        cache_dir = str(cfg.get("data_provider.local_cache_dir", "data/history/cache") or "data/history/cache")
        base_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(base_dir))
        self._cache_dir = cache_dir if os.path.isabs(cache_dir) else os.path.join(project_root, cache_dir)
        os.makedirs(self._cache_dir, exist_ok=True)
        self._reader = None
        self._quotes = None
        self.runtime_platform = str(platform.system() or "").strip().lower() or os.name
        self.provider_mode = "local_vipdoc" if self._has_valid_tdxdir() else "network_mirror"
        self._quote_server_cache_file = os.path.join(self._cache_dir, "tdx_quote_server.json")
        self._cached_server_meta = self._load_cached_quote_server_meta()
        self._preferred_server = self._resolve_preferred_quote_server()
        self._bestip_probe_attempted = False
        # 缓存节点只在整轮任务首次真正用到网络行情时评估是否需要重测 fastest 节点。
        self._refresh_bestip_on_first_network_use = self._should_refresh_cached_server(self._cached_server_meta)

    def _candidate_quote_servers(self):
        out = []
        seen = set()

        def _push(h, p):
            host = str(h or "").strip()
            if not host:
                return
            try:
                port = int(p or 7709)
            except Exception:
                port = 7709
            key = f"{host}:{port}"
            if key in seen:
                return
            seen.add(key)
            out.append((host, port))

        _push(self.host, self.port)
        cfg = ConfigLoader.reload()
        raw_nodes = str(cfg.get("data_provider.tdx_node_list", "") or "").strip()
        for item in [x.strip() for x in raw_nodes.split(",") if x.strip()]:
            if ":" in item:
                h, p = item.rsplit(":", 1)
                _push(h, p)
            else:
                _push(item, 7709)
        try:
            import mootdx.consts as consts  # type: ignore

            hosts = getattr(consts, "HQ_HOSTS", []) or []
            for row in hosts:
                if not isinstance(row, (list, tuple)) or len(row) < 3:
                    continue
                _push(row[1], row[2])
                if len(out) >= 8:
                    break
        except Exception:
            pass
        return out or [("119.147.212.81", 7709)]

    def _quote_server_cache_key(self):
        return str(self.mootdx_market or "std").strip().lower() or "std"

    def _load_cached_quote_server_meta(self):
        key = self._quote_server_cache_key()
        with self._quote_server_cache_lock:
            cached_meta = self._quote_server_cache_meta_mem.get(key)
        if isinstance(cached_meta, dict) and cached_meta.get("host"):
            return cached_meta
        if not os.path.exists(self._quote_server_cache_file):
            return {}
        try:
            with open(self._quote_server_cache_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            market_data = payload.get(key)
            if not isinstance(market_data, dict):
                return {}
            meta = {
                "host": str(market_data.get("host", "") or "").strip(),
                "port": int(market_data.get("port", 7709) or 7709),
                "updated_at": str(market_data.get("updated_at", "") or "").strip(),
                "last_latency_sec": float(market_data.get("last_latency_sec", 0.0) or 0.0),
                "source_mode": str(market_data.get("source_mode", "") or "").strip(),
            }
            if not meta.get("host"):
                return {}
            with self._quote_server_cache_lock:
                self._quote_server_cache_meta_mem[key] = meta
                self._quote_server_cache_mem[key] = (str(meta["host"]), int(meta["port"]))
            return meta
        except Exception:
            return {}

    def _should_refresh_cached_server(self, meta):
        # 任务级最多重测一次 fastest 节点：缓存过旧或最近延迟明显偏慢时才触发。
        if self._explicit_server or not isinstance(meta, dict) or not meta.get("host"):
            return False
        updated_at = str(meta.get("updated_at", "") or "").strip()
        last_latency = float(meta.get("last_latency_sec", 0.0) or 0.0)
        if last_latency >= 8.0:
            return True
        if not updated_at:
            return True
        try:
            updated_dt = datetime.fromisoformat(updated_at)
        except Exception:
            return True
        age_sec = max(0.0, (datetime.now() - updated_dt).total_seconds())
        return age_sec >= 12 * 60 * 60

    def _load_cached_quote_server(self):
        meta = self._load_cached_quote_server_meta()
        if not meta:
            return None
        return (str(meta.get("host", "") or "").strip(), int(meta.get("port", 7709) or 7709))

    def _save_cached_quote_server(self, server, latency_sec=None, source_mode=""):
        if not isinstance(server, (list, tuple)) or len(server) < 2:
            return
        host = str(server[0] or "").strip()
        if not host:
            return
        try:
            port = int(server[1] or 7709)
        except Exception:
            port = 7709
        key = self._quote_server_cache_key()
        cached = (host, port)
        latency_value = 0.0
        try:
            latency_value = max(0.0, float(latency_sec or 0.0))
        except Exception:
            latency_value = 0.0
        with self._quote_server_cache_lock:
            self._quote_server_cache_mem[key] = cached
            self._quote_server_cache_meta_mem[key] = {
                "host": host,
                "port": port,
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "last_latency_sec": latency_value,
                "source_mode": str(source_mode or "").strip(),
            }
        payload = {}
        try:
            if os.path.exists(self._quote_server_cache_file):
                with open(self._quote_server_cache_file, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                    if isinstance(existing, dict):
                        payload = existing
        except Exception:
            payload = {}
        payload[key] = {
            "host": host,
            "port": port,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "last_latency_sec": latency_value,
            "source_mode": str(source_mode or "").strip(),
        }
        try:
            with open(self._quote_server_cache_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            return

    def _resolve_preferred_quote_server(self):
        if self._explicit_server:
            return (self.host, self.port)
        cached = self._load_cached_quote_server()
        if cached:
            self.host = str(cached[0])
            self.port = int(cached[1])
            return cached
        return None

    def _resolve_tdxdir(self, cfg):
        env_dir = str(os.environ.get("TDX_DIR", "") or "").strip()
        cfg_dir = str(cfg.get("data_provider.tdxdir", "") or cfg.get("data_provider.tdx_dir", "") or "").strip()
        raw = env_dir or cfg_dir
        if not raw:
            return ""
        return os.path.normpath(raw)

    def _has_valid_tdxdir(self):
        p = str(self.tdxdir or "").strip()
        if not p:
            return False
        try:
            return os.path.isdir(os.path.join(p, "vipdoc"))
        except Exception:
            return False

    def _cache_file_path(self, code, interval="1min"):
        safe_code = str(code).upper().replace(".", "_")
        return os.path.join(self._cache_dir, f"tdx_{safe_code}_{interval}.csv")

    def describe_mode(self):
        has_vipdoc = self._has_valid_tdxdir()
        return {
            "platform": self.runtime_platform,
            "provider_mode": self.provider_mode,
            "has_vipdoc": has_vipdoc,
            "tdxdir": str(self.tdxdir or "").strip(),
            "cache_dir": self._cache_dir,
        }

    def _normalize_symbol(self, code):
        c = str(code or "").strip().upper()
        if c.endswith(".SH") or c.endswith(".SZ"):
            return c
        if len(c) == 6 and c.isdigit():
            return f"{c}.SH" if c.startswith(("5", "6", "9")) else f"{c}.SZ"
        return c

    def _raw_symbol(self, code):
        sym = self._normalize_symbol(code)
        if "." in sym:
            return sym.split(".", 1)[0]
        return sym

    def _symbol_file_hints(self, code):
        sym = self._normalize_symbol(code)
        raw = self._raw_symbol(code)
        exch = "sh" if sym.endswith(".SH") else "sz"
        root = str(self.tdxdir or "").strip()
        day_path = os.path.join(root, "vipdoc", exch, "lday", f"{exch}{raw}.day")
        # 新版通达信 1 分钟文件常见为 `.01`，旧版/第三方工具仍可能保留 `.1` / `.lc1`。
        # 这里统一返回“优先存在的本地 1 分钟文件”，让诊断信息与本地真实目录保持一致。
        min1_path = self._resolve_local_minute_file(code)
        return day_path, min1_path

    def _symbol_minute_file_candidates(self, code):
        sym = self._normalize_symbol(code)
        raw = self._raw_symbol(code)
        exch = "sh" if sym.endswith(".SH") else "sz"
        root = str(self.tdxdir or "").strip()
        # 按新版优先、旧版兼容的顺序枚举本地 1 分钟文件候选，便于增量同步优先命中新格式。
        return [
            os.path.join(root, "vipdoc", exch, "minline", f"{exch}{raw}.01"),
            os.path.join(root, "vipdoc", exch, "minline", f"{exch}{raw}.1"),
            os.path.join(root, "vipdoc", exch, "minline", f"{exch}{raw}.lc1"),
        ]

    def _resolve_local_minute_file(self, code):
        candidates = self._symbol_minute_file_candidates(code)
        for file_path in candidates:
            if os.path.exists(file_path):
                return file_path
        # 若本地不存在任何分钟文件，也返回新版首选路径，便于错误提示直观展示目标位置。
        return candidates[0] if candidates else ""

    def _read_local_minute_file(self, file_path):
        local_path = str(file_path or "").strip()
        if not local_path or not os.path.exists(local_path):
            return pd.DataFrame()
        try:
            from mootdx.reader import TdxLCMinBarReader, TdxMinBarReader  # type: ignore

            suffix = os.path.splitext(local_path)[1].lower()
            # `.lc1` 仍走旧解析器；`.01` / `.1` 走新版分钟条目解析器。
            reader = TdxLCMinBarReader() if suffix.startswith(".lc") else TdxMinBarReader()
            return reader.get_df(local_path)
        except Exception as e:
            self.last_error = f"本地TDX分钟文件解析失败 path={local_path}: {e}"
            return pd.DataFrame()

    def _import_mootdx(self):
        try:
            from mootdx.reader import Reader  # type: ignore
            from mootdx.quotes import Quotes  # type: ignore
            return Reader, Quotes
        except Exception as e:
            self.last_error = f"mootdx 未安装或导入失败: {e}"
            return None, None

    def _create_reader(self):
        Reader, _ = self._import_mootdx()
        if Reader is None:
            return None
        if not self._has_valid_tdxdir():
            self.last_error = "tdxdir 未配置或无效（需指向包含 vipdoc 的通达信目录）"
            return None
        kwargs = {"market": self.mootdx_market}
        kwargs["tdxdir"] = str(self.tdxdir)
        return Reader.factory(**kwargs)

    def _create_quotes(self, server=None, bestip=False):
        _, Quotes = self._import_mootdx()
        if Quotes is None:
            return None
        # Quotes 连通性探测沿用统一超时配置，避免网络稍慢时过早判定节点不可用。
        kwargs = {"market": self.mootdx_market, "timeout": self.quote_timeout_sec}
        if bool(bestip):
            kwargs["bestip"] = True
        if server and isinstance(server, (list, tuple)) and len(server) >= 2:
            kwargs["server"] = (str(server[0]), int(server[1]))
        return Quotes.factory(**kwargs)

    def _ensure_reader(self):
        if self._reader is not None:
            return self._reader
        try:
            self._reader = self._create_reader()
        except Exception as e:
            self.last_error = f"mootdx Reader 初始化失败: {e}"
            self._reader = None
        return self._reader

    def _ensure_quotes(self):
        if self._quotes is not None:
            return self._quotes
        last_err = ""
        # 缓存节点过旧或最近探测明显偏慢时，整轮任务首次网络使用前只重测一次 fastest 节点。
        if (
            (not self._explicit_server)
            and self._preferred_server is not None
            and self._refresh_bestip_on_first_network_use
            and not self._bestip_probe_attempted
        ):
            self._bestip_probe_attempted = True
            bestip_started = time.perf_counter()
            try:
                logger.info(
                    "TDX 行情连接预热重测开始：模式=任务级自动优选 原因=缓存节点过旧或偏慢 当前缓存=%s:%s",
                    self._preferred_server[0],
                    self._preferred_server[1],
                )
                self._quotes = self._create_quotes(bestip=True)
                elapsed = time.perf_counter() - bestip_started
                self.last_error = ""
                self._preferred_server = (self.host, self.port)
                self._cached_server_meta = {
                    "host": self.host,
                    "port": self.port,
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                    "last_latency_sec": elapsed,
                    "source_mode": "task_bestip_refresh",
                }
                self._save_cached_quote_server((self.host, self.port), latency_sec=elapsed, source_mode="task_bestip_refresh")
                self._refresh_bestip_on_first_network_use = False
                logger.info(
                    "TDX 行情连接预热重测成功：模式=任务级自动优选 节点=%s:%s 耗时=%.2fs",
                    self.host,
                    self.port,
                    elapsed,
                )
                return self._quotes
            except Exception as e:
                last_err = str(e)
                self._quotes = None
                logger.warning("TDX 行情连接预热重测失败：模式=任务级自动优选 原因=%s", last_err)
        # 优先复用显式配置或上次成功节点，避免每次新任务都重新执行 bestip 扫描。
        if self._preferred_server is not None:
            try:
                cached_started = time.perf_counter()
                logger.info(
                    "TDX 行情连接初始化开始：模式=%s 市场=%s 超时=%ss 节点=%s:%s",
                    "显式配置" if self._explicit_server else "缓存节点",
                    self.mootdx_market,
                    self.quote_timeout_sec,
                    self._preferred_server[0],
                    self._preferred_server[1],
                )
                self._quotes = self._create_quotes(server=self._preferred_server)
                self.host = str(self._preferred_server[0])
                self.port = int(self._preferred_server[1])
                self.last_error = ""
                self._save_cached_quote_server(
                    self._preferred_server,
                    latency_sec=time.perf_counter() - cached_started,
                    source_mode="explicit" if self._explicit_server else "cached",
                )
                logger.info(
                    "TDX 行情连接初始化成功：模式=%s 节点=%s:%s",
                    "显式配置" if self._explicit_server else "缓存节点",
                    self.host,
                    self.port,
                )
                return self._quotes
            except Exception as e:
                last_err = str(e)
                logger.warning(
                    "TDX 行情连接初始化失败：模式=%s 节点=%s:%s 原因=%s",
                    "显式配置" if self._explicit_server else "缓存节点",
                    self._preferred_server[0],
                    self._preferred_server[1],
                    last_err,
                )
                self._quotes = None
        # 首选节点不可用时，再回退到 bestip 探测，兼顾启动速度和可用性。
        try:
            self._bestip_probe_attempted = True
            bestip_started = time.perf_counter()
            logger.info(
                "TDX 行情连接初始化开始：模式=自动优选节点 市场=%s 超时=%ss",
                self.mootdx_market,
                self.quote_timeout_sec,
            )
            self._quotes = self._create_quotes(bestip=True)
            self.last_error = ""
            self._preferred_server = (self.host, self.port)
            self._save_cached_quote_server(
                (self.host, self.port),
                latency_sec=time.perf_counter() - bestip_started,
                source_mode="bestip",
            )
            logger.info(
                "TDX 行情连接初始化成功：模式=自动优选节点 节点提示=%s:%s",
                self.host,
                self.port,
            )
            return self._quotes
        except Exception as e:
            last_err = str(e)
            logger.warning("TDX 行情连接初始化失败：模式=自动优选节点 原因=%s", last_err)
            self._quotes = None
        candidates = self._candidate_quote_servers()
        logger.info(
            "TDX 行情连接回退到候选节点扫描：候选数=%s 市场=%s",
            len(candidates),
            self.mootdx_market,
        )
        for server in candidates:
            try:
                candidate_started = time.perf_counter()
                self._quotes = self._create_quotes(server=server)
                self.host = str(server[0])
                self.port = int(server[1])
                self.last_error = ""
                self._preferred_server = (self.host, self.port)
                self._save_cached_quote_server(
                    server,
                    latency_sec=time.perf_counter() - candidate_started,
                    source_mode="candidate_scan",
                )
                logger.info(
                    "TDX 行情连接初始化成功：模式=候选节点扫描 节点=%s:%s",
                    self.host,
                    self.port,
                )
                return self._quotes
            except Exception as e:
                last_err = str(e)
                self._quotes = None
                continue
        logger.error("TDX 行情连接初始化全部失败：市场=%s 原因=%s", self.mootdx_market, last_err or "")
        self.last_error = f"mootdx Quotes 初始化失败: {last_err}" if last_err else "mootdx Quotes 初始化失败"
        return self._quotes

    def _normalize_ohlcv_df(self, df, code):
        if df is None or (hasattr(df, "empty") and bool(df.empty)):
            return pd.DataFrame()
        work = pd.DataFrame(df).copy()
        if work.empty:
            return pd.DataFrame()
        col_map = {
            "datetime": ["datetime", "dt", "trade_time", "date", "time", "日期", "时间"],
            "open": ["open", "OPEN", "开盘"],
            "high": ["high", "HIGH", "最高"],
            "low": ["low", "LOW", "最低"],
            "close": ["close", "CLOSE", "收盘", "price", "现价"],
            "vol": ["vol", "volume", "VOL", "成交量", "量"],
            "amount": ["amount", "turnover", "AMOUNT", "成交额", "额"],
            "code": ["code", "symbol", "ts_code", "股票代码"],
        }

        def _pick(cols):
            for c in cols:
                if c in work.columns:
                    return c
            return ""

        dt_col = _pick(col_map["datetime"])
        open_col = _pick(col_map["open"])
        high_col = _pick(col_map["high"])
        low_col = _pick(col_map["low"])
        close_col = _pick(col_map["close"])
        vol_col = _pick(col_map["vol"])
        amount_col = _pick(col_map["amount"])
        code_col = _pick(col_map["code"])

        if not dt_col and "date" in work.columns and "time" in work.columns:
            work["dt"] = pd.to_datetime(
                work["date"].astype(str).str.strip() + " " + work["time"].astype(str).str.strip(),
                errors="coerce",
            )
        elif dt_col:
            work["dt"] = pd.to_datetime(work[dt_col], errors="coerce")
        elif not work.index.empty:
            # mootdx Reader.daily 常把日期放在索引里
            idx_dt = pd.to_datetime(work.index, errors="coerce")
            if idx_dt.isna().all():
                return pd.DataFrame()
            work["dt"] = idx_dt
        else:
            return pd.DataFrame()

        work["open"] = pd.to_numeric(work[open_col], errors="coerce") if open_col else pd.NA
        work["high"] = pd.to_numeric(work[high_col], errors="coerce") if high_col else pd.NA
        work["low"] = pd.to_numeric(work[low_col], errors="coerce") if low_col else pd.NA
        work["close"] = pd.to_numeric(work[close_col], errors="coerce") if close_col else pd.NA
        work["vol"] = pd.to_numeric(work[vol_col], errors="coerce") if vol_col else 0.0
        work["amount"] = pd.to_numeric(work[amount_col], errors="coerce") if amount_col else 0.0
        if code_col:
            work["code"] = work[code_col].astype(str).str.upper()
        else:
            work["code"] = self._normalize_symbol(code)

        out = work[["code", "dt", "open", "high", "low", "close", "vol", "amount"]].copy()
        out = out.dropna(subset=["dt", "open", "high", "low", "close"])
        out = out.sort_values("dt").drop_duplicates(subset=["dt"]).reset_index(drop=True)
        return out

    def _load_cached_daily_data(self, code, start_time, end_time):
        if not self._cache_enabled:
            return pd.DataFrame(), False
        path = self._cache_file_path(code, "D")
        if not os.path.exists(path):
            return pd.DataFrame(), False
        try:
            df = pd.read_csv(path)
            df = self._normalize_ohlcv_df(df, code=code)
            if df.empty:
                return pd.DataFrame(), False
            full_coverage = df["dt"].min() <= start_time and df["dt"].max() >= end_time
            df_range = df[(df["dt"] >= start_time) & (df["dt"] <= end_time)].copy()
            return df_range, bool(full_coverage and not df_range.empty)
        except Exception:
            return pd.DataFrame(), False

    def _save_daily_cache(self, code, df):
        if not self._cache_enabled or df is None or df.empty:
            return
        path = self._cache_file_path(code, "D")
        try:
            df_save = self._normalize_ohlcv_df(df, code=code)
            if df_save.empty:
                return
            if os.path.exists(path):
                old_df = pd.read_csv(path)
                old_df = self._normalize_ohlcv_df(old_df, code=code)
                if not old_df.empty:
                    df_save = pd.concat([old_df, df_save], ignore_index=True)
                    df_save = self._normalize_ohlcv_df(df_save, code=code)
            df_save.to_csv(path, index=False, encoding="utf-8")
        except Exception:
            return

    def _load_cached_minute_data(self, code, start_time, end_time):
        if not self._cache_enabled:
            return pd.DataFrame(), False
        path = self._cache_file_path(code, "1min")
        if not os.path.exists(path):
            return pd.DataFrame(), False
        try:
            df = pd.read_csv(path)
            df = self._normalize_ohlcv_df(df, code=code)
            if df.empty:
                return pd.DataFrame(), False
            full_coverage = df["dt"].min() <= start_time and df["dt"].max() >= end_time
            df_range = df[(df["dt"] >= start_time) & (df["dt"] <= end_time)].copy()
            return df_range, bool(full_coverage and not df_range.empty)
        except Exception:
            return pd.DataFrame(), False

    def _save_minute_cache(self, code, df):
        if not self._cache_enabled or df is None or df.empty:
            return
        path = self._cache_file_path(code, "1min")
        try:
            df_save = self._normalize_ohlcv_df(df, code=code)
            if df_save.empty:
                return
            if os.path.exists(path):
                old_df = pd.read_csv(path)
                old_df = self._normalize_ohlcv_df(old_df, code=code)
                if not old_df.empty:
                    df_save = pd.concat([old_df, df_save], ignore_index=True)
                    df_save = self._normalize_ohlcv_df(df_save, code=code)
            df_save.to_csv(path, index=False, encoding="utf-8")
        except Exception:
            return

    def _reader_daily(self, raw_code):
        reader = self._ensure_reader()
        if reader is None:
            return pd.DataFrame()
        if hasattr(reader, "daily"):
            return reader.daily(symbol=raw_code)
        return pd.DataFrame()

    def _reader_minute(self, raw_code):
        reader = self._ensure_reader()
        if reader is None:
            # Reader 初始化失败时，仍尝试直接按文件路径解析新版 `.01`，降低对 mootdx 路径匹配的依赖。
            return self._read_local_minute_file(self._resolve_local_minute_file(raw_code))
        for name in ["minute", "min", "mins", "minute_bars", "bars"]:
            if not hasattr(reader, name):
                continue
            fn = getattr(reader, name)
            try:
                df = fn(symbol=raw_code)
            except TypeError:
                try:
                    df = fn(raw_code)
                except Exception:
                    continue
            except Exception:
                continue
            if df is not None and not getattr(df, "empty", True):
                return df
        # mootdx 0.11.x 对新版 `minline/*.01` 不会自动命中，这里补充显式本地文件回退。
        return self._read_local_minute_file(self._resolve_local_minute_file(raw_code))

    def _is_recent_minute_window(self, end_time):
        # 仅当请求窗口贴近当前时刻时，才值得用网络快照补最后几根分钟线。
        end_ts = pd.to_datetime(end_time, errors="coerce")
        if pd.isna(end_ts):
            return False
        now_ts = pd.Timestamp(datetime.now())
        return abs((now_ts - end_ts).total_seconds()) <= 20 * 60

    def _log_minute_fetch_stage(self, code, stage, started_at, df=None, extra=""):
        # 分阶段记录耗时与行数，便于定位分钟线取数到底慢在本地读取还是网络调用。
        rows = 0
        try:
            if df is not None and not getattr(df, "empty", True):
                rows = int(len(df))
        except Exception:
            rows = 0
        extra_text = f" {extra}" if str(extra or "").strip() else ""
        logger.info(
            "TDX 分钟取数阶段：股票=%s 阶段=%s 耗时=%.2fs 行数=%s%s",
            self._normalize_symbol(code),
            stage,
            max(0.0, time.perf_counter() - float(started_at or 0.0)),
            rows,
            extra_text,
        )

    def _quotes_bars(self, raw_code):
        quotes = self._ensure_quotes()
        if quotes is None:
            return pd.DataFrame()
        if not hasattr(quotes, "bars"):
            self.last_error = "mootdx Quotes 不支持 bars 接口"
            return pd.DataFrame()
        try:
            return quotes.bars(symbol=raw_code)
        except TypeError:
            return quotes.bars(raw_code)
        except Exception as e:
            self.last_error = f"mootdx Quotes.bars 失败: {e}"
            return pd.DataFrame()

    def _quotes_snapshot(self, raw_code):
        quotes = self._ensure_quotes()
        if quotes is None:
            return pd.DataFrame()
        if not hasattr(quotes, "quotes"):
            self.last_error = "mootdx Quotes 不支持 quotes 接口"
            return pd.DataFrame()
        variants = []
        code_raw = str(raw_code or "").strip()
        if code_raw:
            variants.append(code_raw)
        if len(code_raw) == 6 and code_raw.isdigit():
            variants.extend([f"sh{code_raw}", f"sz{code_raw}"])
        last_err = ""
        for sym in variants:
            try:
                df = quotes.quotes(symbol=sym)
            except TypeError:
                try:
                    df = quotes.quotes(sym)
                except Exception as e:
                    last_err = str(e)
                    continue
            except Exception as e:
                last_err = str(e)
                continue
            if df is not None and (not getattr(df, "empty", True)):
                self.last_error = ""
                return df
        if last_err:
            self.last_error = f"mootdx Quotes.quotes 失败: {last_err}"
        return pd.DataFrame()

    def _snapshot_time_to_dt(self, servertime):
        t = str(servertime or "").strip()
        if not t:
            return pd.Timestamp(datetime.now().replace(second=0, microsecond=0))
        if "." in t:
            t = t.split(".", 1)[0]
        if len(t) == 5:
            t = f"{t}:00"
        now = datetime.now()
        try:
            dt = pd.to_datetime(f"{now.strftime('%Y-%m-%d')} {t}", errors="coerce")
            if pd.isna(dt):
                return pd.Timestamp(now.replace(second=0, microsecond=0))
            return pd.Timestamp(dt).replace(second=0, microsecond=0)
        except Exception:
            return pd.Timestamp(now.replace(second=0, microsecond=0))

    def _snapshot_to_bar_df(self, snap_df, code):
        if snap_df is None or (hasattr(snap_df, "empty") and bool(snap_df.empty)):
            return pd.DataFrame()
        try:
            row = pd.DataFrame(snap_df).iloc[-1].to_dict()
        except Exception:
            return pd.DataFrame()
        close_v = pd.to_numeric(row.get("price", row.get("last_close", None)), errors="coerce")
        if pd.isna(close_v):
            return pd.DataFrame()
        open_v = pd.to_numeric(row.get("open", close_v), errors="coerce")
        high_v = pd.to_numeric(row.get("high", close_v), errors="coerce")
        low_v = pd.to_numeric(row.get("low", close_v), errors="coerce")
        vol_v = pd.to_numeric(row.get("vol", row.get("volume", 0.0)), errors="coerce")
        amt_v = pd.to_numeric(row.get("amount", 0.0), errors="coerce")
        out = pd.DataFrame(
            [
                {
                    "code": self._normalize_symbol(code),
                    "dt": self._snapshot_time_to_dt(row.get("servertime", "")),
                    "open": float(open_v if not pd.isna(open_v) else close_v),
                    "high": float(high_v if not pd.isna(high_v) else close_v),
                    "low": float(low_v if not pd.isna(low_v) else close_v),
                    "close": float(close_v),
                    "vol": float(vol_v if not pd.isna(vol_v) else 0.0),
                    "amount": float(amt_v if not pd.isna(amt_v) else 0.0),
                }
            ]
        )
        return self._normalize_ohlcv_df(out, code=code)

    def _snapshot_to_pseudo_current_minute_df(self, snap_df, code, anchor_dt):
        base = self._snapshot_to_bar_df(snap_df, code=code)
        if base is None or base.empty:
            return pd.DataFrame()
        dt_anchor = pd.to_datetime(anchor_dt, errors="coerce")
        if pd.isna(dt_anchor):
            dt_anchor = pd.Timestamp(datetime.now())
        dt_anchor = pd.Timestamp(dt_anchor).replace(second=0, microsecond=0)
        out = base.copy()
        out.loc[:, "dt"] = dt_anchor
        return self._normalize_ohlcv_df(out, code=code)

    def check_connectivity(self, code):
        raw_code = self._raw_symbol(code)
        snap = self._snapshot_to_bar_df(self._quotes_snapshot(raw_code), code=code)
        if not snap.empty:
            self.last_error = ""
            return True, "ok_rt"
        bars = self._quotes_bars(raw_code)
        df = self._normalize_ohlcv_df(bars, code=code)
        if not df.empty:
            self.last_error = ""
            return True, "ok"
        daily = self._reader_daily(raw_code)
        df_d = self._normalize_ohlcv_df(daily, code=code)
        if not df_d.empty:
            self.last_error = ""
            return True, "ok_local"
        cached_daily, _ = self._load_cached_daily_data(code, pd.Timestamp(datetime.now()) - pd.Timedelta(days=30), pd.Timestamp(datetime.now()))
        if not cached_daily.empty:
            self.last_error = ""
            return True, "ok_cache"
        # Quotes 可能因网络/节点不可用返回空；若本地 Reader 可初始化且目录有效，
        # 放行预检查，后续数据拉取阶段再按真实数据可得性判定。
        reader = self._ensure_reader()
        if reader is not None and self._has_valid_tdxdir():
            self.last_error = ""
            return True, "ok_reader"
        quotes = self._ensure_quotes()
        if quotes is not None:
            self.last_error = ""
            return True, "ok_network_mirror"
        if self.last_error:
            return False, self.last_error
        return False, "mootdx 连通性检查失败（bars/daily/cache均为空）"

    def fetch_minute_data(self, code, start_time, end_time):
        st = pd.to_datetime(start_time, errors="coerce")
        et = pd.to_datetime(end_time, errors="coerce")
        if pd.isna(st) or pd.isna(et) or st > et:
            self.last_error = "TDX时间参数无效"
            return pd.DataFrame()
        fetch_started = time.perf_counter()
        cached_df, cache_hit = self._load_cached_minute_data(code, st, et)
        if cache_hit:
            self.last_error = ""
            self._log_minute_fetch_stage(code, "分钟缓存命中", fetch_started, cached_df)
            return cached_df
        raw_code = self._raw_symbol(code)
        parts = []
        if not cached_df.empty:
            parts.append(cached_df)
        # 历史分钟增量同步优先读取本地 vipdoc；本地可用时不要先走高延迟网络接口。
        reader_started = time.perf_counter()
        df_reader = self._normalize_ohlcv_df(self._reader_minute(raw_code), code=code)
        self._log_minute_fetch_stage(code, "本地分钟读取", reader_started, df_reader)
        if not df_reader.empty:
            parts.append(df_reader)
        _, min1_path = self._symbol_file_hints(code)
        local_minute_missing = not os.path.exists(min1_path)
        should_try_network = (
            (not self._has_valid_tdxdir())
            or self._is_recent_minute_window(et)
            or df_reader.empty
            or local_minute_missing
        )
        df_snap = pd.DataFrame()
        df_quote = pd.DataFrame()
        if should_try_network:
            # 本地分钟文件缺失或读取为空时，直接回退网络取数；实时窗口再额外补快照。
            snap_started = time.perf_counter()
            df_snap = self._snapshot_to_bar_df(self._quotes_snapshot(raw_code), code=code)
            self._log_minute_fetch_stage(
                code,
                "网络快照读取",
                snap_started,
                df_snap,
                extra=f"本地1min文件存在={not local_minute_missing}",
            )
            quote_started = time.perf_counter()
            df_quote = self._normalize_ohlcv_df(self._quotes_bars(raw_code), code=code)
            self._log_minute_fetch_stage(
                code,
                "网络分钟读取",
                quote_started,
                df_quote,
                extra=f"本地读取为空={df_reader.empty}",
            )
            if not df_snap.empty:
                parts.append(df_snap)
            if not df_quote.empty:
                parts.append(df_quote)
        if not parts:
            if self._has_valid_tdxdir():
                day_path, min1_path = self._symbol_file_hints(code)
                self.last_error = (
                    f"mootdx分钟线为空 code={self._normalize_symbol(code)}; "
                    f"本地文件检查 day_exists={os.path.exists(day_path)} min1_exists={os.path.exists(min1_path)} "
                    f"min1_path={min1_path}; "
                    f"请在通达信客户端下载该标的历史数据后重试"
                )
            else:
                self.last_error = (
                    self.last_error
                    or f"mootdx分钟线为空 code={self._normalize_symbol(code)}；当前处于无vipdoc的网络镜像模式，请先扩大回测窗口触发本地缓存积累或检查节点连通性"
                )
            self._log_minute_fetch_stage(code, "分钟取数失败", fetch_started, extra=f"原因={self.last_error}")
            return pd.DataFrame()
        merged = pd.concat(parts, ignore_index=True)
        merged = self._normalize_ohlcv_df(merged, code=code)
        merged = merged[(merged["dt"] >= st) & (merged["dt"] <= et)].copy()
        if merged.empty:
            if should_try_network:
                # 实时窗口允许用快照补当前分钟，纯历史窗口则直接失败，避免每只股票都长时间等待网络。
                pseudo_started = time.perf_counter()
                pseudo = self._snapshot_to_pseudo_current_minute_df(
                    self._quotes_snapshot(raw_code),
                    code=code,
                    anchor_dt=min(pd.Timestamp(datetime.now()), et),
                )
                if pseudo is not None and (not pseudo.empty):
                    pseudo = pseudo[(pseudo["dt"] >= st) & (pseudo["dt"] <= et)].copy()
                    self._log_minute_fetch_stage(code, "伪分钟补点", pseudo_started, pseudo)
                    if not pseudo.empty:
                        self._save_minute_cache(code, pseudo)
                        self.last_error = ""
                        return pseudo
            if self._has_valid_tdxdir():
                day_path, min1_path = self._symbol_file_hints(code)
                self.last_error = (
                    f"mootdx分钟线为空 code={self._normalize_symbol(code)}; "
                    f"本地文件检查 day_exists={os.path.exists(day_path)} min1_exists={os.path.exists(min1_path)} "
                    f"min1_path={min1_path}; "
                    f"请在通达信客户端下载该标的历史数据后重试"
                )
            else:
                self.last_error = self.last_error or f"mootdx分钟线为空 code={self._normalize_symbol(code)}"
            self._log_minute_fetch_stage(code, "分钟窗口过滤后为空", fetch_started, extra=f"原因={self.last_error}")
            return pd.DataFrame()
        self._save_minute_cache(code, merged)
        self.last_error = ""
        self._log_minute_fetch_stage(code, "分钟取数完成", fetch_started, merged)
        return merged

    def fetch_kline_data(self, code, start_time, end_time, interval="1min"):
        iv = str(interval or "1min").strip()
        iv_low = iv.lower()
        if iv_low in {"d", "1d", "day", "daily"}:
            iv = "D"
        elif iv_low in {"1min", "5min", "10min", "15min", "30min", "60min"}:
            iv = iv_low
        else:
            iv = iv_low

        st = pd.to_datetime(start_time, errors="coerce")
        et = pd.to_datetime(end_time, errors="coerce")
        if pd.isna(st) or pd.isna(et) or st > et:
            self.last_error = "TDX时间参数无效"
            return pd.DataFrame()

        if iv == "1min":
            return self.fetch_minute_data(code, st, et)

        if iv == "D":
            cached_daily, cache_hit = self._load_cached_daily_data(code, st, et)
            if cache_hit:
                self.last_error = ""
                return cached_daily
            raw_code = self._raw_symbol(code)
            daily = self._normalize_ohlcv_df(self._reader_daily(raw_code), code=code)
            if not daily.empty:
                out = daily[(daily["dt"] >= st) & (daily["dt"] <= et)].copy()
                if not out.empty:
                    self._save_daily_cache(code, out)
                    self.last_error = ""
                    return out
            base = self.fetch_minute_data(code, st, et)
            if base.empty:
                return pd.DataFrame()
            out = Indicators.resample(base, "D")
            if not out.empty:
                self._save_daily_cache(code, out)
                self.last_error = ""
            return out

        base = self.fetch_minute_data(code, st, et)
        if base.empty:
            return pd.DataFrame()
        return Indicators.resample(base, iv)

    def get_latest_bar(self, code):
        raw_code = self._raw_symbol(code)
        quote_df = self._snapshot_to_bar_df(self._quotes_snapshot(raw_code), code=code)
        if quote_df.empty:
            quote_df = self._normalize_ohlcv_df(self._quotes_bars(raw_code), code=code)
        if quote_df.empty:
            now = datetime.now()
            quote_df = self.fetch_minute_data(code, now - pd.Timedelta(days=2), now)
        if quote_df.empty:
            return None
        row = quote_df.sort_values("dt").iloc[-1]
        self.last_error = ""
        return {
            "code": str(row["code"]),
            "dt": pd.to_datetime(row["dt"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "vol": float(row["vol"]),
            "amount": float(row["amount"]),
        }
