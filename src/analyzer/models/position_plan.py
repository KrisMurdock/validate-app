from dataclasses import dataclass, field


@dataclass
class PriceLevel:
    price: float
    strength: str        # "strong" | "moderate" | "weak"
    method: str          # "ma_120" | "previous_high" | "fibonacci" | ...


@dataclass
class EntryZone:
    price_low: float
    price_high: float
    description: str     # "回踩MA60不破"


@dataclass
class PositionPlan:
    code: str
    support_levels: list[PriceLevel] = field(default_factory=list)
    resistance_levels: list[PriceLevel] = field(default_factory=list)
    suggested_position_pct: float = 0.0   # 0.0 - 1.0
    entry_zones: list[EntryZone] = field(default_factory=list)
    stop_loss: float = 0.0
    take_profit: list[float] = field(default_factory=list)
    add_position_prices: list[float] = field(default_factory=list)
    reduce_position_prices: list[float] = field(default_factory=list)
