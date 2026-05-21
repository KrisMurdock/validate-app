from dataclasses import dataclass, field
from enum import Enum

from src.analyzer.rules.base_rule import BaseRule, RuleContext
from src.analyzer.models.stock_signal import StockSignal


class ScoringMode(str, Enum):
    ALL_PASS = "all_pass"
    WEIGHTED_SUM = "weighted_sum"
    ANY_PASS = "any_pass"


@dataclass
class ScreeningStrategy:
    strategy_id: str
    name: str
    rules: list[BaseRule] = field(default_factory=list)
    scoring_mode: ScoringMode = ScoringMode.ALL_PASS
    pass_threshold: float = 60.0

    def evaluate(self, code: str, context: RuleContext) -> StockSignal | None:
        rule_scores: dict[str, float] = {}
        tags: list[str] = []
        passed_rules: list[str] = []

        for rule in self.rules:
            result = rule.evaluate(code, context)
            rule_scores[result.rule_name] = result.score

            if result.passed:
                passed_rules.append(result.rule_name)
                tags.extend(result.tags)
            elif self.scoring_mode == ScoringMode.ALL_PASS:
                return None  # early cutoff

        if not passed_rules:
            return None

        if self.scoring_mode == ScoringMode.WEIGHTED_SUM:
            total_weight = sum(r.weight for r in self.rules)
            score = (
                sum(
                    rule_scores[r.name] * r.weight
                    for r in self.rules
                    if r.name in rule_scores
                )
                / total_weight
                if total_weight > 0
                else 0.0
            )
        elif self.scoring_mode == ScoringMode.ALL_PASS:
            score = (
                sum(rule_scores.values()) / len(rule_scores)
                if rule_scores
                else 0.0
            )
        else:  # ANY_PASS
            passed_scores = [
                rule_scores[r] for r in passed_rules if r in rule_scores
            ]
            score = sum(passed_scores) / len(passed_scores) if passed_scores else 0.0

        if score < self.pass_threshold:
            return None

        return StockSignal(
            code=code,
            strategy_id=self.strategy_id,
            score=score,
            rule_scores=rule_scores,
            tags=list(set(tags)),
            passed_rules=passed_rules,
        )
