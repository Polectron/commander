from __future__ import annotations

import asyncio
import functools
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ImportString

if TYPE_CHECKING:
    from commander import Commander
    from commander.backends.base import JobBackend
    from commander.queues.base import JobQueue


class TaskPayload(BaseModel):
    fn: ImportString[Callable[..., Any]]
    args: tuple
    kwargs: dict


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


class OnceSchedule(BaseModel):
    kind: Literal["once"] = "once"
    run_at: int


class IntervalSchedule(BaseModel):
    kind: Literal["interval"] = "interval"
    seconds: float


class CronSchedule(BaseModel):
    kind: Literal["cron"] = "cron"
    expression: str

    @staticmethod
    def next_run(expression: str, after: int) -> int:
        from croniter import croniter

        point = datetime.fromtimestamp(after, tz=UTC)
        try:
            return int(croniter(expression, point).get_next(datetime).timestamp())
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid cron expression: {expression}") from exc


class QueuedTask(BaseModel):
    id: uuid.UUID
    payload: TaskPayload


class ScheduledTask(BaseModel):
    id: uuid.UUID
    payload: TaskPayload
    schedule: OnceSchedule | IntervalSchedule | CronSchedule
    next_run: int
    last_run: int | None = None


Schedule = OnceSchedule | IntervalSchedule | CronSchedule


class TaskState(BaseModel):
    id: uuid.UUID
    payload: TaskPayload
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: str | None = None

    @property
    def finished(self) -> bool:
        return self.status in (TaskStatus.FINISHED, TaskStatus.FAILED)


class TaskHandler:
    def __init__(self, task_id: uuid.UUID, backend: JobBackend | None = None):
        self.task_id = task_id
        self._backend = backend

    async def finished(self) -> bool:
        if self._backend is None:
            raise ValueError("No backend configured so tasks can't be inspected")
        task = await self._backend.get(self.task_id)
        if task is None:
            raise ValueError(f"Task {self.task_id} not found")
        return bool(task.finished)

    async def wait(self, timeout: float | None = None):
        if self._backend is None:
            raise ValueError("No backend configured so tasks can't be waited")
        start_time = time.monotonic()
        while True:
            task = await self._backend.get(self.task_id)
            if task is None:
                raise ValueError(
                    f"Task {self.task_id} was not found when waiting for it"
                )
            if task.finished:
                if task.status == TaskStatus.FAILED:
                    raise RuntimeError(task.error or f"Task {self.task_id} failed")
                return task.result
            await asyncio.sleep(0.1)
            if timeout and time.monotonic() - start_time > timeout:
                raise TimeoutError("Timeout waiting for task")


async def queue_task(
    queue: JobQueue,
    backend: JobBackend | None,
    payload: TaskPayload,
) -> TaskHandler:
    task_id = uuid.uuid4()
    task = QueuedTask(
        id=task_id,
        payload=payload,
    )
    if backend:
        await backend.save(task_id, payload)
    await queue.put(task)
    return TaskHandler(task_id, backend)


class Task:
    def __init__(self, commander: Commander, fn) -> None:
        self._fn = fn
        self._commander = commander
        functools.update_wrapper(self, fn)

    async def __call__(self, *args, **kwargs):
        return await self._fn(*args, **kwargs)

    async def schedule(
        self,
        *args,
        at: datetime | int | None = None,
        when: int | None = None,
        every: timedelta | None = None,
        cron: str | None = None,
        **kwargs,
    ) -> None:
        if self._commander.backend is None:
            raise ValueError("No commander is set up, can't schedule task")

        options = sum(option is not None for option in (at, when, every, cron))
        if options != 1:
            raise ValueError("Schedule exactly one of at, when, every, or cron")

        schedule_id = uuid.uuid4()
        payload = TaskPayload(fn=self._fn, args=args, kwargs=kwargs)
        now = int(datetime.now(UTC).timestamp())

        if at is not None:
            if isinstance(at, datetime):
                if at.tzinfo is None:
                    at = at.replace(tzinfo=UTC)
                next_run = int(at.timestamp())
            else:
                next_run = at
            schedule: Schedule = OnceSchedule(run_at=next_run)
        elif when is not None:
            next_run = when
            schedule = OnceSchedule(run_at=when)
        elif every is not None:
            if every.total_seconds() <= 0:
                raise ValueError("Schedule interval must be greater than zero")
            next_run = now + int(every.total_seconds())
            schedule = IntervalSchedule(seconds=every.total_seconds())
        else:
            schedule = CronSchedule(expression=cron or "")
            next_run = CronSchedule.next_run(cron or "", now)

        await self._commander.backend.schedule_task(
            schedule_id,
            payload,
            schedule,
            next_run,
        )
        await self._commander.queue.notify_refresh()

    async def queue(self, *args, **kwargs) -> TaskHandler:
        payload = TaskPayload(fn=self._fn, args=args, kwargs=kwargs)
        return await queue_task(self._commander.queue, self._commander.backend, payload)

    async def run(self, *args, **kwargs):
        return await self._fn(*args, **kwargs)
