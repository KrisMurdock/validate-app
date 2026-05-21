from src.analyzer.rules.base_rule import BaseRule, RuleContext, RuleResult


class SupportResistanceRule(BaseRule):
    """Placeholder: support/resistance level calculation rule.

    To be implemented with specific S/R detection logic.
    """
    name = "support_resistance"
    category = "technical"

    def evaluate(self, code: str, context: RuleContext) -> RuleResult:
        return RuleResult(rule_name=self.name, passed=False, score=0.0)
