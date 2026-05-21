from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class RuleContext:
    daily: pd.DataFrame
    minute: pd.DataFrame | None = None
    fundamental: dict | None = None
    capital_flow: dict | None = None
    market_index: pd.DataFrame | None = None


@dataclass
class RuleResult:
    rule_name: str
    passed: bool
    score: float = 0.0
    tags: list[str] = field(default_factory=list)
    details: dict = field(default_factory=dict)


class BaseRule(ABC):
    name: str = ""
    category: str = "technical"
    weight: float = 1.0
    required_data: list[str] = []

    @abstractmethod
    def evaluate(self, code: str, context: RuleContext) -> RuleResult:
        ...
