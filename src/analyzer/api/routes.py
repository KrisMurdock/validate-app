import json
import os
import sys
import threading
from datetime import datetime

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from src.analyzer.engine import AnalysisEngine


class RunRequest(BaseModel):
    codes: list[str] | None = None


class RunResponse(BaseModel):
    status: str
    report_count: int
    error: str | None = None


class StatusResponse(BaseModel):
    status: str
    last_run_at: str | None = None
    last_error: str | None = None
    last_report_count: int = 0
    total_codes: int = 0
    processed_codes: int = 0


class ValidateRequest(BaseModel):
    start_date: str = "2021-01-01"
    end_date: str = "2026-05-21"
    mode: str = "random"          # "random" | "sequential" | "sr_only" | "grid"
    trials: int = 10
    window_days: int = 30
    frequency_days: int = 7
    top_n: int = 30
    support_break_pct: float = 3.0
    codes: list[str] | None = None
    sr_only: bool = False         # skip screening, validate S/R directly
    grid_search: bool = False     # run grid search over weight combos
    sr_weights: dict[str, float] | None = None  # custom weights for sr_only
    weight_grid: list[dict[str, float]] | None = None  # custom grid
    fine_grid: bool = False     # use fine grid search (~35 combos)
    fine_step: float = 0.25     # step size for fine grid
    output: str | None = None


# ---- Validation state (module-level, shared across requests) ----

_validation_state = {
    "running": False,
    "thread": None,
    "report": None,
    "error": None,
    "started_at": None,
    "finished_at": None,
    "progress": {"dates_scanned": 0, "total_trades": 0, "current_date": None},
}


def _get_project_root():
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    if getattr(sys, "_MEIPASS", None):
        root = sys._MEIPASS
    return root


def _run_validation_background(params: dict):
    global _validation_state
    _validation_state["running"] = True
    _validation_state["report"] = None
    _validation_state["error"] = None
    _validation_state["progress"] = {"dates_scanned": 0, "total_trades": 0, "current_date": None}

    try:
        mode = params.get("mode", "random")
        if params.get("grid_search"):
            from scripts.validate_strategy import run_validation_grid, GRID_PRESETS
            wg = params.get("weight_grid")
            if not wg and not params.get("fine_grid"):
                wg = list(GRID_PRESETS.values())
            report = run_validation_grid(
                start_date=params["start_date"], end_date=params["end_date"],
                codes=params.get("codes"), window_days=params["window_days"],
                trials=params["trials"], support_break_pct=params["support_break_pct"],
                weight_grid=wg, fine_grid=params.get("fine_grid", False),
                fine_step=params.get("fine_step", 0.25),
                top_n=params.get("top_n", 30),
            )
        elif params.get("sr_only") or mode == "sr_only":
            from scripts.validate_strategy import run_validation_sr_only
            report = run_validation_sr_only(
                start_date=params["start_date"], end_date=params["end_date"],
                codes=params.get("codes"), window_days=params["window_days"],
                trials=params["trials"], support_break_pct=params["support_break_pct"],
                sr_weights=params.get("sr_weights"),
            )
        elif mode == "sequential":
            from scripts.validate_strategy import run_validation
            report = run_validation(
                start_date=params["start_date"], end_date=params["end_date"],
                frequency_days=params["frequency_days"], top_n=params["top_n"],
                max_days=params["window_days"], support_break_pct=params["support_break_pct"],
                codes=params.get("codes"),
            )
        else:
            from scripts.validate_strategy import run_validation_random
            report = run_validation_random(
                start_date=params["start_date"], end_date=params["end_date"],
                top_n=params["top_n"], window_days=params["window_days"],
                trials=params["trials"], support_break_pct=params["support_break_pct"],
                codes=params.get("codes"),
            )

        # Save report
        output_path = params.get("output") or "data/validation_report.json"
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        report["_meta"] = {
            "start_date": params["start_date"],
            "end_date": params["end_date"],
            "strategy": params.get("strategy_id", "all"),
            "generated_at": datetime.now().isoformat(),
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        _validation_state["report"] = report
        _validation_state["progress"]["total_trades"] = report.get("total_trades", 0)
        _validation_state["progress"]["dates_scanned"] = report.get("dates_scanned", 0)

    except Exception as e:
        _validation_state["error"] = str(e)
    finally:
        _validation_state["running"] = False
        _validation_state["finished_at"] = datetime.now().isoformat()


def create_router(engine: AnalysisEngine) -> APIRouter:
    router = APIRouter(prefix="/analyzer", tags=["analyzer"])

    # ---- Page routes ----

    def _serve_html(filename: str) -> str:
        html_path = os.path.join(_get_project_root(), filename)
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()

    @router.get("", response_class=HTMLResponse)
    async def analyzer_page():
        return _serve_html("analyzer.html")

    @router.get("/validate", response_class=HTMLResponse)
    async def validate_page():
        return _serve_html("validate.html")

    # ---- Screening endpoints ----

    @router.get("/strategies")
    async def list_strategies():
        return [
            {
                "strategy_id": s.strategy_id,
                "name": s.name,
                "scoring_mode": s.scoring_mode.value,
                "pass_threshold": s.pass_threshold,
                "rule_count": len(s.rules),
            }
            for s in engine.strategies
        ]

    @router.get("/status")
    async def get_status():
        return StatusResponse(
            status=engine.status.value,
            last_run_at=engine.last_run_at.isoformat() if engine.last_run_at else None,
            last_error=engine.last_error,
            last_report_count=engine.last_report_count,
            total_codes=engine.total_codes,
            processed_codes=engine.processed_codes,
        )

    @router.post("/run")
    def trigger_run(req: RunRequest | None = None):
        codes = req.codes if req else None
        reports = engine.run(codes=codes)
        return RunResponse(
            status=engine.status.value,
            report_count=len(reports),
            error=engine.last_error,
        )

    @router.get("/reports")
    async def list_reports(strategy_id: str | None = None, code: str | None = None, limit: int = 50):
        return {"reports": [], "total": 0}

    @router.get("/reports/{report_id}")
    async def get_report(report_id: str):
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")

    # ---- Validation endpoints ----

    @router.post("/validate/run")
    def start_validation(req: ValidateRequest | None = None):
        global _validation_state

        if _validation_state["running"]:
            return {"status": "already_running", "message": "A validation run is already in progress"}

        params = {
            "start_date": req.start_date if req else "2021-01-01",
            "end_date": req.end_date if req else "2026-05-21",
            "mode": req.mode if req else "random",
            "trials": req.trials if req else 10,
            "window_days": req.window_days if req else 30,
            "frequency_days": req.frequency_days if req else 7,
            "top_n": req.top_n if req else 30,
            "support_break_pct": req.support_break_pct if req else 3.0,
            "codes": req.codes if req else None,
            "output": req.output if req else None,
        }

        _validation_state["started_at"] = datetime.now().isoformat()
        _validation_state["finished_at"] = None

        thread = threading.Thread(target=_run_validation_background, args=(params,), daemon=True)
        thread.start()
        _validation_state["thread"] = thread

        return {"status": "started", "message": "Validation started"}

    @router.get("/validate/status")
    def get_validation_status():
        return {
            "running": _validation_state["running"],
            "started_at": _validation_state["started_at"],
            "finished_at": _validation_state["finished_at"],
            "error": _validation_state["error"],
            "progress": _validation_state["progress"],
            "report": _validation_state["report"],
        }

    @router.get("/validate/reports")
    def list_validation_reports():
        project_root = _get_project_root()
        data_dir = os.path.join(project_root, "data")
        reports = []
        try:
            for f in sorted(os.listdir(data_dir), reverse=True):
                if f.startswith("validation_report") and f.endswith(".json"):
                    fpath = os.path.join(data_dir, f)
                    try:
                        with open(fpath, "r", encoding="utf-8") as fp:
                            r = json.load(fp)
                        reports.append({
                            "filename": f,
                            "period": r.get("period", {}),
                            "win_rate": r.get("win_rate"),
                            "total_trades": r.get("total_trades"),
                            "generated_at": r.get("_meta", {}).get("generated_at", ""),
                        })
                    except Exception:
                        reports.append({"filename": f, "error": "unreadable"})
        except FileNotFoundError:
            pass
        return {"reports": reports}

    @router.get("/validate/reports/{filename}")
    def get_validation_report(filename: str):
        import urllib.parse
        fname = urllib.parse.unquote(filename)
        if ".." in fname or "/" in fname:
            raise HTTPException(status_code=400, detail="Invalid filename")
        fpath = os.path.join(_get_project_root(), "data", fname)
        if not os.path.exists(fpath):
            raise HTTPException(status_code=404, detail="Report not found")
        with open(fpath, "r", encoding="utf-8") as f:
            return json.load(f)

    return router
