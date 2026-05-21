"""多头排列 + 放量突破选股策略"""
import numpy as np

from src.analyzer.rules.base_rule import BaseRule, RuleContext, RuleResult
from src.analyzer.rules.composer import ScreeningStrategy, ScoringMode
from src.analyzer.strategies import register


class MATrendRule(BaseRule):
    """均线多头排列：MA5 > MA20 > MA60"""

    name = "ma_trend"
    category = "technical"
    weight = 1.0
    required_data = ["daily_kline"]

    def evaluate(self, code: str, context: RuleContext) -> RuleResult:
        if context.daily.empty or len(context.daily) < 60:
            return RuleResult(rule_name=self.name, passed=False, score=0.0,
                              tags=[], details={"reason": "数据不足"})

        closes = context.daily["close"].values
        ma5 = float(np.mean(closes[-5:]))
        ma20 = float(np.mean(closes[-20:]))
        ma60 = float(np.mean(closes[-60:]))
        current = closes[-1]

        aligned = current > ma5 > ma20 > ma60

        return RuleResult(
            rule_name=self.name,
            passed=aligned,
            score=85.0 if aligned else 10.0,
            tags=["多头排列"] if aligned else [],
            details={"ma5": round(ma5, 2), "ma20": round(ma20, 2),
                     "ma60": round(ma60, 2), "current": round(current, 2)},
        )


class VolumeExpandRule(BaseRule):
    """放量：今日成交量为5日均量的1.2倍以上"""

    name = "volume_expand"
    category = "technical"
    weight = 0.6
    required_data = ["daily_kline"]

    def evaluate(self, code: str, context: RuleContext) -> RuleResult:
        if context.daily.empty or len(context.daily) < 6:
            return RuleResult(rule_name=self.name, passed=False, score=0.0,
                              tags=[], details={"reason": "数据不足"})

        vols = context.daily["vol"].values
        avg5 = float(np.mean(vols[-6:-1]))
        today = float(vols[-1])
        ratio = today / avg5 if avg5 > 0 else 1.0
        passed = ratio > 1.2

        return RuleResult(
            rule_name=self.name,
            passed=passed,
            score=min(100.0, ratio * 40),
            tags=["放量突破"] if passed else [],
            details={"vol_ratio": round(ratio, 2)},
        )


# 模块加载时自动注册
demo_strategy = ScreeningStrategy(
    strategy_id="demo_ma_trend",
    name="均线多头+放量",
    rules=[MATrendRule(), VolumeExpandRule()],
    scoring_mode=ScoringMode.WEIGHTED_SUM,
    pass_threshold=50,
)
register(demo_strategy)
