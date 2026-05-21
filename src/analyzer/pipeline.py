from __future__ import annotations

from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Callable

import pandas as pd

from src.analyzer.rules.base_rule import RuleContext
from src.analyzer.rules.composer import ScreeningStrategy
from src.analyzer.position_analyzer import PositionAnalyzer
from src.analyzer.models.analysis_report import AnalysisReport


@dataclass
class AnalysisPipeline:
    strategies: list[ScreeningStrategy] = field(default_factory=list)
    top_n: int = 50
    max_workers: int = 8
    position_analyzer: PositionAnalyzer = field(default_factory=PositionAnalyzer)

    def run(
        self,
        codes: list[str],
        contexts: dict[str, RuleContext],
        on_progress: Callable[[int, int], None] | None = None,
    ) -> list[AnalysisReport]:
        if not codes or not self.strategies:
            return []

        all_signals = []
        processed = 0
        total = len([c for c in codes if c in contexts])

        for strategy in self.strategies:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(strategy.evaluate, code, contexts.get(code)): code
                    for code in codes
                    if code in contexts
                }
                for future in as_completed(futures):
                    try:
                        signal = future.result()
                        processed += 1
                        if signal is not None:
                            all_signals.append(signal)
                        if on_progress:
                            on_progress(processed, total)
                    except Exception:
                        processed += 1
                        if on_progress:
                            on_progress(processed, total)
                        continue

        all_signals.sort(key=lambda s: s.score, reverse=True)
        top_signals = all_signals[:self.top_n]

        reports = []
        for signal in top_signals:
            ctx = contexts.get(signal.code)
            daily = ctx.daily if ctx else pd.DataFrame()
            plan, risk = self.position_analyzer.analyze(signal, daily=daily)
            reports.append(AnalysisReport(
                stock=signal,
                position=plan,
                risk=risk,
                strategy_id=signal.strategy_id,
            ))
        return reports
