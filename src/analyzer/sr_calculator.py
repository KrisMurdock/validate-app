from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass
class DimensionSR:
    name: str
    weight: float
    support: float
    resistance: float


@dataclass
class SRResult:
    support: float
    resistance: float
    dimension_results: list[DimensionSR] = field(default_factory=list)


# ---- Per-dimension calculations ----

def _ma_support_resistance(closes, weight=0.25):
    current = float(closes[-1])
    ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else current
    ma120 = float(np.mean(closes[-120:])) if len(closes) >= 120 else current
    ma5 = float(np.mean(closes[-5:])) if len(closes) >= 5 else current
    ma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else current
    below = [m for m in [ma60, ma120] if m < current]
    support = float(np.mean(below)) if below else current * 0.95
    above = [m for m in [ma5, ma20] if m > current]
    resistance = float(np.mean(above)) if above else current * 1.05
    return DimensionSR(name="ma", weight=weight, support=round(support, 2),
                       resistance=round(resistance, 2))


def _bollinger_support_resistance(closes, window=20, num_std=2.0, weight=0.25):
    current = float(closes[-1])
    if len(closes) < window:
        return DimensionSR(name="bollinger", weight=weight,
                           support=round(current * 0.95, 2),
                           resistance=round(current * 1.05, 2))
    ma = float(np.mean(closes[-window:]))
    std = float(np.std(closes[-window:]))
    support = ma - num_std * std
    resistance = ma + num_std * std
    return DimensionSR(name="bollinger", weight=weight,
                       support=round(support, 2), resistance=round(resistance, 2))


def _high_low_support_resistance(highs, lows, current, window=60, weight=0.25):
    lookback = min(window, len(highs))
    support = float(np.min(lows[-lookback:]))
    resistance = float(np.max(highs[-lookback:]))
    return DimensionSR(name="high_low", weight=weight,
                       support=round(support, 2), resistance=round(resistance, 2))


def _fibonacci_support_resistance(highs, lows, current, window=120, weight=0.25):
    lookback = min(window, len(highs))
    range_high = float(np.max(highs[-lookback:]))
    range_low = float(np.min(lows[-lookback:]))
    rng = range_high - range_low
    support = range_low + rng * 0.382
    resistance = range_low + rng * 0.618
    return DimensionSR(name="fibonacci", weight=weight,
                       support=round(support, 2), resistance=round(resistance, 2))


# ---- Main calculation ----

DEFAULT_WEIGHTS = {"ma": 0.25, "bollinger": 0.25, "high_low": 0.25, "fibonacci": 0.25}


def compute_sr(daily, weights=None):
    if daily.empty or "close" not in daily.columns:
        return SRResult(support=0.0, resistance=0.0)
    w = weights or DEFAULT_WEIGHTS
    closes = daily["close"].values
    highs = daily["high"].values
    lows = daily["low"].values
    current = float(closes[-1])
    dims = [
        _ma_support_resistance(closes, weight=w.get("ma", 0.25)),
        _bollinger_support_resistance(closes, weight=w.get("bollinger", 0.25)),
        _high_low_support_resistance(highs, lows, current, weight=w.get("high_low", 0.25)),
        _fibonacci_support_resistance(highs, lows, current, weight=w.get("fibonacci", 0.25)),
    ]
    total_w = sum(d.weight for d in dims)
    if total_w == 0:
        return SRResult(support=0.0, resistance=0.0, dimension_results=dims)
    support = sum(d.support * d.weight for d in dims) / total_w
    resistance = sum(d.resistance * d.weight for d in dims) / total_w
    return SRResult(support=round(support, 2), resistance=round(resistance, 2),
                    dimension_results=dims)


@dataclass
class SRCalculator:
    weights: dict[str, float] = field(default_factory=lambda: DEFAULT_WEIGHTS.copy())

    def compute(self, daily):
        return compute_sr(daily, weights=self.weights)

    @classmethod
    def from_config(cls):
        from src.utils.config_loader import ConfigLoader
        w = ConfigLoader.reload().get("analyzer", {}).get("sr_weights", {})
        if not w:
            w = DEFAULT_WEIGHTS
        return cls(weights=w)
