from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from src.analyzer.rules.base_rule import RuleContext
from src.utils.data_factory import DataFactory


@dataclass
class DataAdapter:
    source: str = "duckdb"
    daily_lookback_days: int = 750
    minute_lookback_days: int = 60
    reference_date: datetime | None = None

    def _date_range(self, lookback_days: int) -> tuple[str, str]:
        end = self.reference_date or datetime.now()
        start = end - timedelta(days=lookback_days)
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    def _get_provider(self):
        factory = DataFactory(source=self.source)
        return factory.get_provider()

    def load_context(self, code: str) -> RuleContext:
        provider = self._get_provider()
        daily = pd.DataFrame()
        try:
            start, end = self._date_range(self.daily_lookback_days)
            daily = provider.fetch_daily_data(code, start, end)
            if daily is None:
                daily = pd.DataFrame()
        except Exception:
            pass
        minute = None
        try:
            start, end = self._date_range(self.minute_lookback_days)
            minute = provider.fetch_minute_data(code, start, end)
        except Exception:
            pass
        return RuleContext(daily=daily, minute=minute)

    def load_contexts_batch(self, codes: list[str]) -> dict[str, RuleContext]:
        return {code: self.load_context(code) for code in codes}

    def get_stock_universe(self) -> list[str]:
        import duckdb
        from src.utils.config_loader import ConfigLoader
        cfg = ConfigLoader.reload()
        db_path = str(cfg.get("data_provider.duckdb_path", "")).strip()
        if not db_path:
            return []
        try:
            conn = duckdb.connect(db_path, read_only=True)
            rows = conn.execute("SELECT DISTINCT code FROM dat_day ORDER BY code").fetchall()
            conn.close()
            return [str(r[0]) for r in rows]
        except Exception:
            return []
