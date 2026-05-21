from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from src.analyzer.adapters.data_adapter import DataAdapter
from src.analyzer.pipeline import AnalysisPipeline
from src.analyzer.rules.composer import ScreeningStrategy
from src.analyzer.models.analysis_report import AnalysisReport


class EngineStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    ERROR = "error"


@dataclass
class AnalysisEngine:
    strategies: list[ScreeningStrategy] = field(default_factory=list)
    top_n: int = 50
    data_adapter: DataAdapter = field(default_factory=DataAdapter)
    status: EngineStatus = EngineStatus.IDLE
    last_run_at: datetime | None = None
    last_error: str | None = None
    last_report_count: int = 0
    total_codes: int = 0
    processed_codes: int = 0

    def run(self, codes: list[str] | None = None) -> list[AnalysisReport]:
        self.status = EngineStatus.RUNNING
        self.last_error = None
        self.total_codes = 0
        self.processed_codes = 0
        try:
            if codes is None:
                codes = self._load_stock_universe()
            if not codes:
                self.status = EngineStatus.IDLE
                self.last_run_at = datetime.now(timezone.utc)
                return []

            self.total_codes = len(codes)
            contexts = self.data_adapter.load_contexts_batch(codes)

            def on_progress(done: int, total: int):
                self.processed_codes = done
                self.total_codes = total

            pipeline = AnalysisPipeline(strategies=self.strategies, top_n=self.top_n)
            reports = pipeline.run(codes, contexts, on_progress=on_progress)
            self.last_report_count = len(reports)
            self.status = EngineStatus.IDLE
            self.last_run_at = datetime.now(timezone.utc)
            return reports
        except Exception as e:
            self.status = EngineStatus.ERROR
            self.last_error = str(e)
            self.last_run_at = datetime.now(timezone.utc)
            return []

    def _load_stock_universe(self) -> list[str]:
        try:
            return self.data_adapter.get_stock_universe()
        except Exception:
            return []

    @classmethod
    def from_config(cls) -> "AnalysisEngine":
        from src.utils.config_loader import ConfigLoader

        cfg = ConfigLoader.reload()
        ac = cfg.get("analyzer", {})
        top_n = int(ac.get("top_n", 50))
        daily_lb = int(ac.get("data", {}).get("daily_lookback_days", 750))
        minute_lb = int(ac.get("data", {}).get("minute_lookback_days", 60))

        strategies = cls._load_strategies_from_config(ac.get("strategies", []))

        return cls(
            strategies=strategies,
            top_n=top_n,
            data_adapter=DataAdapter(
                daily_lookback_days=daily_lb,
                minute_lookback_days=minute_lb,
            ),
        )

    @staticmethod
    def _load_strategies_from_config(strategy_ids: list[str]) -> list:
        """Load ScreeningStrategy instances from the strategy registry.

        Importing a strategy module auto-registers it via the registry.
        """
        # Import demo strategies so they self-register
        try:
            import src.analyzer.strategies.demo_ma_trend  # noqa: F401
            import src.analyzer.strategies.screening_v1  # noqa: F401
        except Exception:
            pass

        from src.analyzer.strategies import get, list_all

        if strategy_ids:
            loaded = []
            for sid in strategy_ids:
                s = get(sid)
                if s:
                    loaded.append(s)
            return loaded

        return list_all()
