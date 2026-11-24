import asyncio
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
    ) -> None:
        self._backend = backend
        self._queue = queue
        self._schedules: dict[str, ScheduledTask] = {}

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

    async def run(self):
        await self.refresh()
        while True:
            now = int(time.time())
            pending_tasks = [
                task for task in self._schedules.values() if task.next_run <= now
            ]
            for task in pending_tasks:
                try:
                    await queue_task(
                        self._queue,
                        self._backend,
                        task.payload,
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"Could not enqueue scheduled task {task.id}: {exc!r}")
                    continue

                advanced = self._advance(task, now)
                if advanced is None:
                    await self._backend.delete_schedule(task.id)
                    self._schedules.pop(str(task.id), None)
                else:
                    await self._backend.update_schedule(advanced)
                    self._schedules[str(task.id)] = advanced
            await asyncio.sleep(0.1)

