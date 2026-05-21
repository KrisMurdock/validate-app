from src.analyzer.rules.base_rule import BaseRule, RuleContext, RuleResult


class TrendRule(BaseRule):
    """Placeholder: trend detection rule (MA alignment, ADX, etc.).

    To be implemented with specific trend criteria.
    """
    name = "trend"
    category = "technical"

    def evaluate(self, code: str, context: RuleContext) -> RuleResult:
        return RuleResult(rule_name=self.name, passed=False, score=0.0)
