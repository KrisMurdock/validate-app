import numpy as np
from src.analyzer.rules.base_rule import BaseRule, RuleContext, RuleResult
from src.analyzer.rules.composer import ScreeningStrategy, ScoringMode
from src.analyzer.strategies import register


class MAAlignmentRule(BaseRule):
    name = "ma_alignment"
    category = "technical"
    weight = 1.0
    required_data = ["daily_kline"]

    def evaluate(self, code, context):
        if context.daily.empty or len(context.daily) < 60:
            return RuleResult(rule_name=self.name, passed=False, score=0.0,
                              tags=[], details={"reason": "数据不足"})
        closes = context.daily["close"].values
        ma5 = float(np.mean(closes[-5:]))
        ma20 = float(np.mean(closes[-20:]))
        ma60 = float(np.mean(closes[-60:]))
        aligned = bool(ma5 > ma20 > ma60)
        strength = min(100, max(0, (ma5 - ma60) / ma60 * 100 + 50))
        return RuleResult(rule_name=self.name, passed=aligned,
                          score=round(strength, 1) if aligned else 5.0,
                          tags=["多头排列"] if aligned else [],
                          details={"ma5": round(ma5, 2), "ma20": round(ma20, 2), "ma60": round(ma60, 2)})


class VolumeBreakoutRule(BaseRule):
    name = "volume_breakout"
    category = "technical"
    weight = 0.6
    required_data = ["daily_kline"]

    def evaluate(self, code, context):
        if context.daily.empty or len(context.daily) < 6:
            return RuleResult(rule_name=self.name, passed=False, score=0.0,
                              tags=[], details={"reason": "数据不足"})
        vols = context.daily["vol"].values
        avg5 = float(np.mean(vols[-6:-1]))
        today = float(vols[-1])
        ratio = today / avg5 if avg5 > 0 else 1.0
        passed = bool(ratio > 1.2)
        return RuleResult(rule_name=self.name, passed=passed,
                          score=min(100.0, ratio * 40),
                          tags=["放量"] if passed else [],
                          details={"vol_ratio": round(ratio, 2)})


class RelativeStrengthRule(BaseRule):
    name = "relative_strength"
    category = "technical"
    weight = 0.4
    required_data = ["daily_kline", "market_index"]

    def evaluate(self, code, context):
        if context.daily.empty or len(context.daily) < 20:
            return RuleResult(rule_name=self.name, passed=False, score=0.0,
                              tags=[], details={"reason": "数据不足"})
        sc = context.daily["close"].values
        stock_ret = (sc[-1] / sc[-20] - 1) * 100
        index_ret = 0.0
        if context.market_index is not None and not context.market_index.empty and len(context.market_index) >= 20:
            ic = context.market_index["close"].values
            index_ret = (ic[-1] / ic[-20] - 1) * 100
        outperforming = bool(stock_ret > index_ret)
        score = min(100, max(0, 50 + (stock_ret - index_ret) * 5))
        return RuleResult(rule_name=self.name, passed=outperforming,
                          score=round(score, 1),
                          tags=["相对强势"] if outperforming else [],
                          details={"stock_return": round(stock_ret, 2), "index_return": round(index_ret, 2)})


def create_screening_v1_strategy():
    return ScreeningStrategy(
        strategy_id="screening_v1",
        name="V1技术面选股",
        rules=[MAAlignmentRule(), VolumeBreakoutRule(), RelativeStrengthRule()],
        scoring_mode=ScoringMode.WEIGHTED_SUM,
        pass_threshold=50,
    )


register(create_screening_v1_strategy())
