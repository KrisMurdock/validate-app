from src.analyzer.rules.base_rule import BaseRule, RuleContext, RuleResult


class CapitalFlowRule(BaseRule):
    """Placeholder: capital flow rule (north-bound, block trades, etc.).

    To be implemented with specific flow criteria.
    """
    name = "capital_flow"
    category = "capital"

    def evaluate(self, code: str, context: RuleContext) -> RuleResult:
        return RuleResult(rule_name=self.name, passed=False, score=0.0)
