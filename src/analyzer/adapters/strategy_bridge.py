from dataclasses import dataclass, field
from src.analyzer.models.analysis_report import AnalysisReport


@dataclass
class StockPool:
    name: str
    codes: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


class StrategyBridge:
    def to_stock_pool(self, reports: list[AnalysisReport], name: str = "analyzer_output") -> StockPool:
        codes = list({r.stock.code for r in reports})
        return StockPool(name=name, codes=codes)

    def to_position_params(self, report: AnalysisReport) -> dict:
        return {
            "code": report.stock.code,
            "score": report.stock.score,
            "stop_loss": report.position.stop_loss,
            "take_profit": report.position.take_profit,
            "position_pct": report.position.suggested_position_pct,
            "support_levels": [{"price": s.price, "strength": s.strength} for s in report.position.support_levels],
            "resistance_levels": [{"price": r.price, "strength": r.strength} for r in report.position.resistance_levels],
            "risk_level": report.risk.risk_level.value,
            "confidence": report.risk.confidence_score,
            "strategy_id": report.strategy_id,
        }

    def to_position_params_batch(self, reports: list[AnalysisReport]) -> list[dict]:
        return [self.to_position_params(r) for r in reports]
