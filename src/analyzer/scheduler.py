from dataclasses import dataclass, field
from src.analyzer.engine import AnalysisEngine


@dataclass
class ScheduleConfig:
    cron: str = "0 15 * * 1-5"
    timezone: str = "Asia/Shanghai"
    enabled: bool = False
    default_codes: list[str] | None = None


class AnalysisScheduler:
    def __init__(self, engine: AnalysisEngine, config: ScheduleConfig | None = None):
        self.engine = engine
        self.config = config or ScheduleConfig()
        self._running = False
        self._job = None

    def start(self) -> None:
        self.config.enabled = True
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger
            scheduler = BackgroundScheduler()
            trigger = CronTrigger.from_crontab(self.config.cron, timezone=self.config.timezone)
            scheduler.add_job(self._run_wrapper, trigger=trigger, id="analyzer_scheduled_scan",
                              name="Analyzer Scheduled Scan")
            scheduler.start()
            self._job = scheduler
            self._running = True
        except ImportError:
            self._running = False

    def stop(self) -> None:
        self.config.enabled = False
        if self._job is not None:
            try:
                self._job.shutdown(wait=False)
            except Exception:
                pass
            self._job = None
        self._running = False

    def _run_wrapper(self) -> None:
        try:
            codes = self.config.default_codes
            self.engine.run(codes=codes)
        except Exception:
            pass

    def get_status(self) -> dict:
        next_run = None
        if self._job is not None:
            try:
                job = self._job.get_job("analyzer_scheduled_scan")
                if job and job.next_run_time:
                    next_run = job.next_run_time.isoformat()
            except Exception:
                pass
        return {"enabled": self.config.enabled, "running": self._running,
                "cron": self.config.cron, "timezone": self.config.timezone,
                "next_run": next_run,
                "last_run_at": self.engine.last_run_at.isoformat() if self.engine.last_run_at else None}
