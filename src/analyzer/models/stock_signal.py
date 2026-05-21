from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class StockSignal:
    code: str
    strategy_id: str
    score: float
    rule_scores: dict[str, float] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    passed_rules: list[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
