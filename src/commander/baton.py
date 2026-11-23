import time

from commander.backends.base import JobBackend
from commander.queues.base import JobQueue
from commander.tasks.task import (
    CronSchedule,
    IntervalSchedule,
    OnceSchedule,
    ScheduledTask,
    queue_task,
)


class Baton:
    def __init__(
        self,
        queue: JobQueue,
        backend: JobBackend,
        tick_interval: float = 1,
        refresh_interval: float = 30,
    ) -> None:
        self._backend = backend
        self._queue = queue
        self._tick_interval = tick_interval
        self._refresh_interval = refresh_interval
        self._schedules: dict[str, ScheduledTask] = {}
        self._last_refresh = 0.0

    def _advance(self, task: ScheduledTask, now: int) -> ScheduledTask | None:
        if isinstance(task.schedule, OnceSchedule):
            return None
        if isinstance(task.schedule, IntervalSchedule):
            next_run = task.next_run
            step = max(1, int(task.schedule.seconds))
            while next_run <= now:
                next_run += step
            return task.model_copy(update={"next_run": next_run, "last_run": now})

        next_run = CronSchedule.next_run(task.schedule.expression, now)
        return task.model_copy(update={"next_run": next_run, "last_run": now})

    async def refresh(self) -> None:
        schedules = await self._backend.get_schedules()
        self._schedules = {str(task.id): task for task in schedules}
        self._last_refresh = time.monotonic()

    async def run(self):
        await self.refresh()
        while True:
            now = int(time.time())
            pending_tasks = [
                task for task in self._schedules.values() if task.next_run <= now
            ]
            for task in pending_tasks:
                await queue_task(self._queue, self._backend, task.payload)
                advanced = self._advance(task, now)
                if advanced is None:
                    await self._backend.delete_schedule(task.id)
                    self._schedules.pop(str(task.id), None)
                else:
                    await self._backend.update_schedule(advanced)
                    self._schedules[str(task.id)] = advanced

            refresh_due = (
                time.monotonic() - self._last_refresh >= self._refresh_interval
            )
            refreshed = await self._queue.wait_for_refresh(self._tick_interval)
            if refreshed or refresh_due:
                await self.refresh()
                continue
