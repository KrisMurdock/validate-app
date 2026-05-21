from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from src.analyzer.models.stock_signal import StockSignal
from src.analyzer.models.position_plan import PositionPlan


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    EXTREME = "extreme"


@dataclass
class RiskAssessment:
    volatility_estimate: float = 0.0
    max_drawdown_estimate: float = 0.0
    confidence_score: float = 0.0
    risk_level: RiskLevel = RiskLevel.MEDIUM
    risk_factors: list[str] = field(default_factory=list)


@dataclass
class AnalysisReport:
    stock: StockSignal
    position: PositionPlan
    risk: RiskAssessment
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    strategy_id: str = ""

