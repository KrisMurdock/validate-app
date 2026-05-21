from dataclasses import dataclass, field
import numpy as np
import pandas as pd

from src.analyzer.models.stock_signal import StockSignal
from src.analyzer.models.position_plan import PositionPlan, PriceLevel, EntryZone
from src.analyzer.models.analysis_report import RiskAssessment, RiskLevel


def fallback_position_plan(code: str) -> PositionPlan:
    return PositionPlan(code=code)


@dataclass
class PositionAnalyzer:
    ma_windows: list[int] = field(default_factory=lambda: [20, 60, 120])
    atr_window: int = 14
    atr_stop_multiple: float = 2.0
    base_position_pct: float = 0.25
    max_position_pct: float = 0.5
    tp_risk_reward_ratios: list[float] = field(default_factory=lambda: [1.5, 3.0])

    def analyze(self, signal: StockSignal, daily: pd.DataFrame) -> tuple[PositionPlan, RiskAssessment]:
        code = signal.code
        if daily.empty or "close" not in daily.columns:
            return fallback_position_plan(code), RiskAssessment()

        closes = daily["close"].values
        highs = daily["high"].values
        lows = daily["low"].values
        current = float(closes[-1])

        supports: list[PriceLevel] = []
        resistances: list[PriceLevel] = []
        for w in self.ma_windows:
            if len(closes) >= w:
                ma_val = float(np.mean(closes[-w:]))
                level = PriceLevel(price=round(ma_val, 2),
                                   strength="strong" if w >= 60 else "moderate",
                                   method=f"ma_{w}")
                if ma_val < current:
                    supports.append(level)
                else:
                    resistances.append(level)

        if not supports:
            supports.append(PriceLevel(price=round(current * 0.95, 2),
                                       strength="weak", method="pct_fallback"))
        if not resistances:
            resistances.append(PriceLevel(price=round(current * 1.05, 2),
                                          strength="weak", method="pct_fallback"))

        atr = self._calc_atr(highs, lows, closes, self.atr_window)
        stop_loss = round(current - atr * self.atr_stop_multiple, 2)
        risk = current - stop_loss
        take_profit = [round(current + risk * rr, 2) for rr in self.tp_risk_reward_ratios]

        score_factor = max(0.0, min(1.0, signal.score / 100.0))
        suggested_pct = round(min(self.base_position_pct * (0.5 + score_factor), self.max_position_pct), 4)

        volatility = self._calc_volatility(closes)
        confidence = round(signal.score, 1)
        risk_level = RiskLevel.MEDIUM
        if volatility > 40:
            risk_level = RiskLevel.HIGH
        elif volatility < 15:
            risk_level = RiskLevel.LOW
        if suggested_pct > self.base_position_pct * 1.5:
            risk_level = RiskLevel.HIGH

        risk_assessment = RiskAssessment(
            volatility_estimate=round(volatility, 1),
            max_drawdown_estimate=round(volatility * 2.5, 1),
            confidence_score=confidence,
            risk_level=risk_level,
            risk_factors=[],
        )

        plan = PositionPlan(
            code=code,
            support_levels=sorted(supports, key=lambda s: s.price, reverse=True),
            resistance_levels=sorted(resistances, key=lambda r: r.price),
            suggested_position_pct=suggested_pct,
            entry_zones=[EntryZone(price_low=round(s.price * 0.99, 2),
                                   price_high=round(s.price * 1.01, 2),
                                   description=f"回踩{s.method}支撑") for s in supports[:1]],
            stop_loss=stop_loss,
            take_profit=take_profit,
            add_position_prices=[round(s.price, 2) for s in supports[:2]],
            reduce_position_prices=[round(r.price, 2) for r in resistances[:2]],
        )
        return plan, risk_assessment

    @staticmethod
    def _calc_atr(highs, lows, closes, window):
        if len(highs) < 2:
            return float(np.mean(highs - lows))
        prev_close = closes[:-1]
        tr = np.maximum(highs[1:] - lows[1:], np.abs(highs[1:] - prev_close))
        tr = np.maximum(tr, np.abs(lows[1:] - prev_close))
        if len(tr) >= window:
            return float(np.mean(tr[-window:]))
        return float(np.mean(tr)) if len(tr) > 0 else 0.0

    @staticmethod
    def _calc_volatility(closes, window=20):
        if len(closes) < 2:
            return 0.0
        lookback = min(window, len(closes) - 1)
        returns = np.diff(closes[-lookback - 1:]) / closes[-lookback - 1:-1]
        return float(np.std(returns) * np.sqrt(252) * 100)
