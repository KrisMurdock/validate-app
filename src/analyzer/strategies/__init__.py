from src.analyzer.rules.composer import ScreeningStrategy

_registry: dict[str, "ScreeningStrategy"] = {}


def register(strategy: ScreeningStrategy) -> ScreeningStrategy:
    _registry[strategy.strategy_id] = strategy
    return strategy


def get(strategy_id: str) -> ScreeningStrategy | None:
    return _registry.get(strategy_id)


def list_all() -> list[ScreeningStrategy]:
    return list(_registry.values())
