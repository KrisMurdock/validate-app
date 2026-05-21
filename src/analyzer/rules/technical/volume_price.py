from src.analyzer.rules.base_rule import BaseRule, RuleContext, RuleResult


class VolumePriceRule(BaseRule):
    """Placeholder: volume-price relationship rule.

    To be implemented with specific volume/price criteria.
    """
    name = "volume_price"
    category = "technical"

    def evaluate(self, code: str, context: RuleContext) -> RuleResult:
        return RuleResult(rule_name=self.name, passed=False, score=0.0)
