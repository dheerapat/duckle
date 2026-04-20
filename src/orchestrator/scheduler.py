import threading
import time

import schedule

from orchestrator.runner import Pipeline, PipelineRunner


class Scheduler:
    """Lightweight cron-style scheduler for pipelines."""

    def __init__(self):
        self.runner = PipelineRunner()
        self._thread = None
        self._running = False

    def add(self, pipeline: Pipeline, cron: str):
        """
        Schedule a pipeline.
        cron examples: "hourly", "daily", "every 30 minutes"
        """

        def job():
            self.runner.run(pipeline)

        if cron == "hourly":
            schedule.every().hour.do(job)
        elif cron == "daily":
            schedule.every().day.at("06:00").do(job)
        elif cron.startswith("every "):
            parts = cron.split(" ")
            interval = int(parts[1])
            unit = parts[2]
            if unit in ("minute", "minutes"):
                schedule.every(interval).minutes.do(job)
            elif unit in ("hour", "hours"):
                schedule.every(interval).hours.do(job)
        else:
            raise ValueError(f"Unknown cron expression: {cron}")

        print(f"[Scheduler] Registered '{pipeline.name}' → {cron}")

    def start(self):
        """Start the scheduler in a background thread."""
        self._running = True

        def loop():
            while self._running:
                schedule.run_pending()
                time.sleep(1)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
        print("[Scheduler] Started.")

    def stop(self):
        self._running = False
        print("[Scheduler] Stopped.")
