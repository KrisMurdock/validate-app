from src.analyzer.rules.base_rule import BaseRule, RuleContext, RuleResult


class ValuationRule(BaseRule):
    """Placeholder: fundamental valuation rule (PE, PB, ROE, etc.).

    To be implemented with specific valuation criteria.
    """
    name = "valuation"
    category = "fundamental"

    def evaluate(self, code: str, context: RuleContext) -> RuleResult:
        return RuleResult(rule_name=self.name, passed=False, score=0.0)
