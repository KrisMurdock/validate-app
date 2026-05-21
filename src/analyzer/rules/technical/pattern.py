from src.analyzer.rules.base_rule import BaseRule, RuleContext, RuleResult


class PatternRule(BaseRule):
    """Placeholder: chart pattern recognition rule.

    To be implemented with specific pattern criteria.
    """
    name = "pattern"
    category = "technical"

    def evaluate(self, code: str, context: RuleContext) -> RuleResult:
        return RuleResult(rule_name=self.name, passed=False, score=0.0)
