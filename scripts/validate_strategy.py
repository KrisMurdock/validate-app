"""Forward validation engine for stock screening strategies.

Usage:
  python scripts/validate_strategy.py                                          # random mode (default)
  python scripts/validate_strategy.py --mode sequential --frequency 7           # sequential mode
  python scripts/validate_strategy.py --trials 10 --window-days 30              # 10 random 30-day trials
  python scripts/validate_strategy.py --codes 000001.SZ,600036.SH --trials 5
"""

import argparse
import json
import os
import random
import sys
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.analyzer.engine import AnalysisEngine
from src.analyzer.adapters.data_adapter import DataAdapter
from src.analyzer.sr_calculator import compute_sr, DEFAULT_WEIGHTS


# ---- Data Models ----

@dataclass
class TradeResult:
    code: str
    date: str
    entry: float
    exit_price: float
    support: float
    resistance: float
    outcome: str       # WIN | LOSS
    trigger: str       # resistance | support_break | support_rebound | expire_win | expire_lose | no_data
    profit_pct: float
    days_held: int
    dimension_sr: list[dict] | None = None   # per-dimension SR values
    forward_prices: list[dict] | None = None  # forward OHLC path
    actual_high: float = 0.0    # highest high in forward window
    actual_low: float = 0.0     # lowest low in forward window


# ---- Trade Simulator ----

def simulate_trade(code, entry, support, resistance, forward_data,
                   support_break_pct=3.0, max_days=30):
    """Simulate a single trade against forward price data.

    Returns a TradeResult with outcome and trigger based on how the
    simulation resolved within the forward window.
    """
    if forward_data.empty or "close" not in forward_data.columns:
        return TradeResult(code=code, date="", entry=entry, exit_price=entry,
                           support=support, resistance=resistance,
                           outcome="LOSS", trigger="no_data", profit_pct=0.0, days_held=0)

    hit_support = False

    for i, (_, row) in enumerate(forward_data.iterrows()):
        day = min(i, max_days - 1)
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])

        # Rule 1: reaches resistance
        if high >= resistance:
            return TradeResult(code=code, date="", entry=entry,
                               exit_price=resistance, support=support,
                               resistance=resistance, outcome="WIN",
                               trigger="resistance",
                               profit_pct=round((resistance - entry) / entry * 100, 2),
                               days_held=day + 1)

        # Check support touch
        if low <= support:
            hit_support = True
            break_pct = (support - low) / support * 100

            # Rule 2: breaks >3% below support
            if break_pct > support_break_pct:
                return TradeResult(code=code, date="", entry=entry,
                                   exit_price=low, support=support,
                                   resistance=resistance, outcome="LOSS",
                                   trigger="support_break",
                                   profit_pct=round((low - entry) / entry * 100, 2),
                                   days_held=day + 1)

            # Rule 3: support touch, rebound above entry
            if close > entry:
                return TradeResult(code=code, date="", entry=entry,
                                   exit_price=close, support=support,
                                   resistance=resistance, outcome="WIN",
                                   trigger="support_rebound",
                                   profit_pct=round((close - entry) / entry * 100, 2),
                                   days_held=day + 1)

    # Rule 4-6: Support was touched but didn't break or rebound yet; check remaining days
    if hit_support:
        for i, (_, row) in enumerate(forward_data.iterrows()):
            day = min(i, max_days - 1)
            close = float(row["close"])
            high = float(row["high"])
            low = float(row["low"])

            if high >= resistance:
                return TradeResult(code=code, date="", entry=entry,
                                   exit_price=resistance, support=support,
                                   resistance=resistance, outcome="WIN",
                                   trigger="resistance",
                                   profit_pct=round((resistance - entry) / entry * 100, 2),
                                   days_held=day + 1)

            if close > entry:
                return TradeResult(code=code, date="", entry=entry,
                                   exit_price=close, support=support,
                                   resistance=resistance, outcome="WIN",
                                   trigger="support_rebound",
                                   profit_pct=round((close - entry) / entry * 100, 2),
                                   days_held=day + 1)

    # Reached max_days limit
    final_close = float(forward_data["close"].values[-1])
    if final_close > entry:
        return TradeResult(code=code, date="", entry=entry,
                           exit_price=final_close, support=support,
                           resistance=resistance, outcome="WIN",
                           trigger="expire_win",
                           profit_pct=round((final_close - entry) / entry * 100, 2),
                           days_held=max_days)
    else:
        return TradeResult(code=code, date="", entry=entry,
                           exit_price=final_close, support=support,
                           resistance=resistance, outcome="LOSS",
                           trigger="expire_lose",
                           profit_pct=round((final_close - entry) / entry * 100, 2),
                           days_held=max_days)


# ---- Validation Engine ----

def run_validation(start_date="2021-01-01", end_date="2026-05-21",
                   frequency_days=7, top_n=30, max_days=30,
                   support_break_pct=3.0, codes=None):
    """Run forward validation across a date range.

    At each step (every `frequency_days`), runs the screening engine,
    then simulates a forward trade for each candidate to determine
    whether the signal would have resulted in a win or loss.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    engine = AnalysisEngine.from_config()
    engine.top_n = top_n

    all_results = []
    dates_scanned = []
    current = start

    while current <= end:
        print(f"\n[{current.strftime('%Y-%m-%d')}] scanning...", flush=True)

        ref_adapter = DataAdapter(source="duckdb", daily_lookback_days=750,
                                  reference_date=current)
        engine.data_adapter = ref_adapter

        reports = engine.run(codes=codes)
        print(f"  candidates: {len(reports)}", flush=True)

        for report in reports:
            code = report.stock.code
            ctx = ref_adapter.load_context(code)
            sr = compute_sr(ctx.daily)
            if sr.support <= 0 or sr.resistance <= 0:
                continue

            provider = ref_adapter._get_provider()
            forward_end = current + timedelta(days=max_days + 5)
            try:
                forward = provider.fetch_daily_data(
                    code, current.strftime("%Y-%m-%d"),
                    forward_end.strftime("%Y-%m-%d"))
                if forward is None:
                    forward = pd.DataFrame()
            except Exception:
                forward = pd.DataFrame()

            entry = float(ctx.daily["close"].values[-1])
            result = simulate_trade(
                code, entry=entry, support=sr.support,
                resistance=sr.resistance, forward_data=forward,
                support_break_pct=support_break_pct, max_days=max_days)
            result.date = current.strftime("%Y-%m-%d")
            result.dimension_sr = [asdict(d) for d in sr.dimension_results]
            result.forward_prices = _serialize_forward(forward)
            if not forward.empty and "high" in forward.columns and "low" in forward.columns:
                result.actual_high = round(float(forward["high"].max()), 2)
                result.actual_low = round(float(forward["low"].min()), 2)
            all_results.append(result)

        dates_scanned.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=frequency_days)

    return _build_report(all_results, dates_scanned, start_date, end_date)


def _serialize_forward(df):
    """Serialize forward DataFrame to a compact list of dicts for JSON."""
    if df is None or df.empty:
        return []
    rows = []
    for _, row in df.head(30).iterrows():
        rows.append({
            "date": str(row.get("trade_time", row.get("dt", "")))[:10],
            "open": round(float(row.get("open", 0)), 2),
            "high": round(float(row.get("high", 0)), 2),
            "low": round(float(row.get("low", 0)), 2),
            "close": round(float(row.get("close", 0)), 2),
        })
    return rows


def _serialize_trade_result(r: TradeResult) -> dict:
    d = asdict(r)
    # dimension_sr and forward_prices are already dicts/lists
    return d


def _build_report(results, dates, period_start, period_end):
    """Aggregate TradeResult list into a summary report dict."""
    if not results:
        return {"error": "No trades generated"}

    wins = [r for r in results if r.outcome == "WIN"]
    losses = [r for r in results if r.outcome == "LOSS"]
    total = len(results)
    win_rate = len(wins) / total * 100 if total > 0 else 0

    win_profits = [r.profit_pct for r in wins]
    loss_profits = [r.profit_pct for r in losses]
    all_profits = [r.profit_pct for r in results]
    trigger_counts = Counter(r.trigger for r in results)

    # Per-trial aggregation (for random mode)
    trial_groups = {}
    for r in results:
        tkey = r.date  # date field holds trial label in random mode
        trial_groups.setdefault(tkey, []).append(r)

    trial_stats = []
    for tkey, t_results in trial_groups.items():
        t_wins = [r for r in t_results if r.outcome == "WIN"]
        t_profits = [r.profit_pct for r in t_results]
        trial_stats.append({
            "trial": tkey,
            "trades": len(t_results),
            "wins": len(t_wins),
            "win_rate": round(len(t_wins) / len(t_results) * 100, 1) if t_results else 0,
            "avg_profit_pct": round(float(np.mean(t_profits)), 2) if t_profits else 0,
            "max_drawdown_pct": round(float(np.min(t_profits)) if t_profits else 0, 2),
        })

    monthly = {}
    for r in results:
        mk = r.date[:7] if r.date else "unknown"
        monthly.setdefault(mk, {"wins": 0, "losses": 0})
        if r.outcome == "WIN":
            monthly[mk]["wins"] += 1
        else:
            monthly[mk]["losses"] += 1

    monthly_win_rates = {
        k: round(v["wins"] / (v["wins"] + v["losses"]) * 100, 1)
        for k, v in monthly.items() if (v["wins"] + v["losses"]) > 0
    }

    return {
        "period": {"start": period_start, "end": period_end},
        "dates_scanned": len(dates),
        "total_trades": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 1),
        "avg_return_pct": round(float(np.mean(all_profits)), 2) if all_profits else 0,
        "avg_max_drawdown_pct": round(float(np.min(all_profits)) if all_profits else 0, 2),
        "profit_factor": round(abs(sum(win_profits) / sum(loss_profits)), 2) if loss_profits and sum(loss_profits) != 0 else None,
        "monthly_win_rates": monthly_win_rates,
        "trial_stats": trial_stats,
        "failure_breakdown": {
            "support_break": trigger_counts.get("support_break", 0),
            "time_expire_lose": trigger_counts.get("expire_lose", 0),
            "no_data": trigger_counts.get("no_data", 0),
        },
        "trades": [_serialize_trade_result(r) for r in results],
    }


# ---- Random Interval Sampling ----

def random_sample_intervals(start_date, end_date, window_days, trials):
    """Generate N non-overlapping random intervals of window_days within [start, end]."""
    total_days = (end_date - start_date).days
    if total_days < window_days * trials:
        trials = max(1, total_days // window_days)

    # Generate candidate start offsets
    max_offset = total_days - window_days
    if max_offset <= 0:
        return [start_date]

    candidates = list(range(0, max_offset + 1, 5))  # sample every 5 days
    if len(candidates) < trials:
        candidates = list(range(0, max_offset + 1))

    random.shuffle(candidates)

    chosen = []
    used = set()
    for offset in candidates:
        start = start_date + timedelta(days=offset)
        end_int = start + timedelta(days=window_days)
        # Check non-overlapping
        overlaps = False
        for cs, ce in chosen:
            if not (end_int <= cs or start >= ce):
                overlaps = True
                break
        if not overlaps:
            chosen.append((start, end_int))
        if len(chosen) >= trials:
            break

    return [s for s, _ in sorted(chosen)]


def run_validation_random(
    start_date="2021-01-01",
    end_date="2026-05-21",
    top_n=30,
    window_days=30,
    trials=10,
    support_break_pct=3.0,
    codes=None,
):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    trial_dates = random_sample_intervals(start, end, window_days, trials)
    print(f"Selected {len(trial_dates)} random trial start dates:\n  " +
          "\n  ".join(d.strftime("%Y-%m-%d") for d in trial_dates))

    engine = AnalysisEngine.from_config()
    engine.top_n = top_n

    all_results = []

    for i, trial_start in enumerate(trial_dates):
        label = f"T{i+1}_{trial_start.strftime('%Y%m%d')}"
        print(f"\n[Trial {i+1}/{len(trial_dates)}] {label}", flush=True)

        ref_adapter = DataAdapter(source="duckdb", daily_lookback_days=750,
                                  reference_date=trial_start)
        engine.data_adapter = ref_adapter

        reports = engine.run(codes=codes)
        print(f"  candidates: {len(reports)}", flush=True)

        for report in reports:
            code = report.stock.code
            ctx = ref_adapter.load_context(code)
            sr = compute_sr(ctx.daily)
            if sr.support <= 0 or sr.resistance <= 0:
                continue

            provider = ref_adapter._get_provider()
            forward_end = trial_start + timedelta(days=window_days + 5)
            try:
                forward = provider.fetch_daily_data(
                    code, trial_start.strftime("%Y-%m-%d"),
                    forward_end.strftime("%Y-%m-%d"))
                if forward is None:
                    forward = pd.DataFrame()
            except Exception:
                forward = pd.DataFrame()

            entry = float(ctx.daily["close"].values[-1])
            result = simulate_trade(
                code, entry=entry, support=sr.support,
                resistance=sr.resistance, forward_data=forward,
                support_break_pct=support_break_pct, max_days=window_days)
            result.date = label
            result.dimension_sr = [asdict(d) for d in sr.dimension_results]
            result.forward_prices = _serialize_forward(forward)
            if not forward.empty and "high" in forward.columns and "low" in forward.columns:
                result.actual_high = round(float(forward["high"].max()), 2)
                result.actual_low = round(float(forward["low"].min()), 2)
            all_results.append(result)

    return _build_report(all_results, [d.strftime("%Y-%m-%d") for d in trial_dates],
                         start_date, end_date)


# ---- S/R-Only Mode ----

def run_validation_sr_only(
    start_date="2021-01-01",
    end_date="2026-05-21",
    codes=None,
    window_days=30,
    trials=10,
    support_break_pct=3.0,
    sr_weights=None,
):
    """Validate S/R parameters directly on fixed stock codes (no screening)."""
    if not codes:
        return {"error": "codes list is required for sr_only mode"}

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    trial_dates = random_sample_intervals(start, end, window_days, trials)

    print(f"SR-Only: {len(codes)} stocks, {len(trial_dates)} trials")
    print(f"Weights: {sr_weights or 'default'}")

    all_results = []

    for i, trial_start in enumerate(trial_dates):
        label = f"T{i+1}_{trial_start.strftime('%Y%m%d')}"
        print(f"\n[Trial {i+1}/{len(trial_dates)}] {label}", flush=True)

        ref_adapter = DataAdapter(source="duckdb", daily_lookback_days=750,
                                  reference_date=trial_start)

        for code in codes:
            ctx = ref_adapter.load_context(code)
            if ctx.daily.empty:
                continue

            sr = compute_sr(ctx.daily, weights=sr_weights)
            if sr.support <= 0 or sr.resistance <= 0:
                continue

            provider = ref_adapter._get_provider()
            forward_end = trial_start + timedelta(days=window_days + 5)
            try:
                forward = provider.fetch_daily_data(
                    code, trial_start.strftime("%Y-%m-%d"),
                    forward_end.strftime("%Y-%m-%d"))
                if forward is None:
                    forward = pd.DataFrame()
            except Exception:
                forward = pd.DataFrame()

            entry = float(ctx.daily["close"].values[-1])
            result = simulate_trade(
                code, entry=entry, support=sr.support,
                resistance=sr.resistance, forward_data=forward,
                support_break_pct=support_break_pct, max_days=window_days)
            result.date = label
            result.dimension_sr = [asdict(d) for d in sr.dimension_results]
            result.forward_prices = _serialize_forward(forward)
            if not forward.empty and "high" in forward.columns and "low" in forward.columns:
                result.actual_high = round(float(forward["high"].max()), 2)
                result.actual_low = round(float(forward["low"].min()), 2)
            all_results.append(result)

    report = _build_report(all_results, [d.strftime("%Y-%m-%d") for d in trial_dates],
                           start_date, end_date)
    report["mode"] = "sr_only"
    report["sr_weights"] = sr_weights or dict(DEFAULT_WEIGHTS)
    return report


# ---- Grid Search ----

GRID_PRESETS = {
    "equal": {"ma": 0.25, "bollinger": 0.25, "high_low": 0.25, "fibonacci": 0.25},
    "ma_heavy": {"ma": 0.55, "bollinger": 0.15, "high_low": 0.15, "fibonacci": 0.15},
    "bollinger_heavy": {"ma": 0.15, "bollinger": 0.55, "high_low": 0.15, "fibonacci": 0.15},
    "high_low_heavy": {"ma": 0.15, "bollinger": 0.15, "high_low": 0.55, "fibonacci": 0.15},
    "fibonacci_heavy": {"ma": 0.15, "bollinger": 0.15, "high_low": 0.15, "fibonacci": 0.55},
}


def generate_fine_grid(step=0.2):
    """Generate all weight combos that sum to 1.0 with given step size.
    Step 0.2 → 56 combos. Step 0.25 → 35 combos.
    """
    combos = []
    for a in range(0, 101, int(step * 100)):
        for b in range(0, 101 - a, int(step * 100)):
            for c in range(0, 101 - a - b, int(step * 100)):
                d = 100 - a - b - c
                if d >= 0:
                    combos.append({
                        "ma": a / 100,
                        "bollinger": b / 100,
                        "high_low": c / 100,
                        "fibonacci": d / 100,
                    })
    return combos


def run_validation_grid(
    start_date="2021-01-01",
    end_date="2026-05-21",
    codes=None,
    window_days=30,
    trials=10,
    support_break_pct=3.0,
    weight_grid=None,
    fine_grid=False,
    fine_step=0.25,
    top_n=30,
):
    """Run validation for multiple S/R weight combinations and compare results."""
    if not codes:
        return {"error": "codes list is required for grid search"}

    if fine_grid:
        combos = generate_fine_grid(fine_step)
        combo_names = [f"w{i+1}" for i in range(len(combos))]
        print(f"Fine Grid: {len(combos)} combos (step={fine_step})")
    else:
        combos = weight_grid or list(GRID_PRESETS.values())
        combo_names = [f"combo_{i}" for i in range(len(combos))]
        for name, preset in GRID_PRESETS.items():
            for i, c in enumerate(combos):
                if c == preset:
                    combo_names[i] = name

    print(f"Grid Search: {len(combos)} weight combinations × {len(codes)} stocks × {trials} trials")
    print(f"Weight combos: {combo_names[:5]}{'...' if len(combo_names) > 5 else ''}")

    grid_results = []
    for i, weights in enumerate(combos):
        name = combo_names[i]
        print(f"\n--- {name}: {weights} ---")
        report = run_validation_sr_only(
            start_date=start_date, end_date=end_date,
            codes=codes, window_days=window_days, trials=trials,
            support_break_pct=support_break_pct, sr_weights=weights,
        )

        # Also run with screening if top_n > 0 and codes is None or large
        grid_results.append({
            "name": name,
            "weights": weights,
            "win_rate": report.get("win_rate", 0),
            "total_trades": report.get("total_trades", 0),
            "avg_return_pct": report.get("avg_return_pct", 0),
            "avg_max_drawdown_pct": report.get("avg_max_drawdown_pct", 0),
            "profit_factor": report.get("profit_factor"),
            "trial_stats": report.get("trial_stats", []),
            "trades": report.get("trades", []),
            "failure_breakdown": report.get("failure_breakdown", {}),
        })

    # Find best
    best = max(grid_results, key=lambda r: r["win_rate"]) if grid_results else None

    return {
        "mode": "grid_search",
        "period": {"start": start_date, "end": end_date},
        "trials": trials,
        "grid": grid_results,
        "best": best,
        "comparison": {
            "win_rate": {r["name"]: r["win_rate"] for r in grid_results},
            "avg_return_pct": {r["name"]: r["avg_return_pct"] for r in grid_results},
            "avg_max_drawdown_pct": {r["name"]: r["avg_max_drawdown_pct"] for r in grid_results},
        },
    }


# ---- CLI ----

def main():
    p = argparse.ArgumentParser(description="Forward validation for stock screening strategies")
    p.add_argument("--start", default="2021-01-01")
    p.add_argument("--end", default="2026-05-21")
    p.add_argument("--mode", default="random", choices=["random", "sequential", "sr_only", "grid"])
    p.add_argument("--trials", type=int, default=10, help="Number of random trials")
    p.add_argument("--window-days", type=int, default=30, help="Duration of each trial in days")
    p.add_argument("--frequency", type=int, default=7, help="Days between scans (sequential mode)")
    p.add_argument("--top-n", type=int, default=30)
    p.add_argument("--support-break-pct", type=float, default=3.0)
    p.add_argument("--codes", default=None)
    p.add_argument("--output", default=None)
    p.add_argument("--grid-preset", default=None, help="Grid preset: equal,ma_heavy,... (comma-separated)")
    p.add_argument("--fine", action="store_true", help="Fine grid search (step=0.25, ~35 combos)")
    p.add_argument("--fine-step", type=float, default=0.25, help="Step size for fine grid (default 0.25)")
    args = p.parse_args()

    codes = [c.strip() for c in args.codes.split(",") if c.strip()] if args.codes else None

    if args.mode == "sr_only":
        if not codes:
            print("ERROR: --codes is required for sr_only mode")
            sys.exit(1)
        report = run_validation_sr_only(
            start_date=args.start, end_date=args.end,
            codes=codes, window_days=args.window_days,
            trials=args.trials, support_break_pct=args.support_break_pct,
        )
    elif args.mode == "grid":
        if not codes:
            print("ERROR: --codes is required for grid mode")
            sys.exit(1)
        weight_grid = None
        if args.grid_preset:
            weight_grid = [GRID_PRESETS[p.strip()] for p in args.grid_preset.split(",") if p.strip() in GRID_PRESETS]
        report = run_validation_grid(
            start_date=args.start, end_date=args.end,
            codes=codes, window_days=args.window_days,
            trials=args.trials, support_break_pct=args.support_break_pct,
            weight_grid=weight_grid, fine_grid=args.fine,
            fine_step=args.fine_step, top_n=args.top_n,
        )
    elif args.mode == "random":
        report = run_validation_random(
            start_date=args.start, end_date=args.end,
            top_n=args.top_n, window_days=args.window_days,
            trials=args.trials, support_break_pct=args.support_break_pct,
            codes=codes,
        )
    else:
        report = run_validation(
            start_date=args.start, end_date=args.end,
            frequency_days=args.frequency, top_n=args.top_n,
            max_days=args.window_days, support_break_pct=args.support_break_pct,
            codes=codes,
        )

    output_path = args.output or "data/validation_report.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*50}")
    print(f"Mode: {args.mode}")
    if args.mode == "grid":
        print(f"Combos tested: {len(report.get('grid',[]))}")
        best = report.get("best", {})
        print(f"Best: {best.get('name', '?')} — WR={best.get('win_rate','?')}% "
              f"Return={best.get('avg_return_pct','?')}% DD={best.get('avg_max_drawdown_pct','?')}%")
        for r in report.get("grid", []):
            print(f"  {r['name']}: WR={r['win_rate']}% | Return={r['avg_return_pct']}% | "
                  f"DD={r['avg_max_drawdown_pct']}% | Trades={r['total_trades']}")
    else:
        print(f"Win Rate: {report.get('win_rate', 'N/A')}%")
        print(f"Trades: {report.get('total_trades', 0)}")
        print(f"Avg Return: {report.get('avg_return_pct', 'N/A')}%")
        print(f"Max Drawdown: {report.get('avg_max_drawdown_pct', 'N/A')}%")
    print(f"Report saved to: {output_path}")


if __name__ == "__main__":
    main()
