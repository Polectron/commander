from __future__ import annotations

import asyncio
import functools
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ImportString

if TYPE_CHECKING:
    from commander import Commander
    from commander.backends.base import JobBackend
    from commander.queues.base import JobQueue


class TaskPayload(BaseModel):
    fn: ImportString[Callable[..., Any]]
    args: tuple
    kwargs: dict


class QueuedTask(BaseModel):
    id: uuid.UUID
    payload: TaskPayload


class ScheduledTask(BaseModel):
    id: uuid.UUID
    payload: TaskPayload
    when: int


class TaskResult(BaseModel):
    id: uuid.UUID
    payload: TaskPayload
    finished: bool
    result: Any


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
                return task.result
            await asyncio.sleep(0.1)
            if timeout and time.monotonic() - start_time > timeout:
                raise TimeoutError("Timeout waiting for task")


async def queue_task(
    queue: JobQueue, backend: JobBackend | None, payload: TaskPayload
) -> TaskHandler:
    task_id = uuid.uuid4()
    task = QueuedTask(id=task_id, payload=payload)
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

    async def schedule(self, *args, when: int, **kwargs):
        if self._commander.backend is None:
            raise ValueError("No commander is set up, can't schedule task")
        schedule_id = uuid.uuid4()
        payload = TaskPayload(fn=self._fn, args=args, kwargs=kwargs)
        await self._commander.backend.schedule_task(schedule_id, payload, when)

    async def queue(self, *args, **kwargs) -> TaskHandler:
        payload = TaskPayload(fn=self._fn, args=args, kwargs=kwargs)
        return await queue_task(self._commander.queue, self._commander.backend, payload)

    async def run(self, *args, **kwargs):
        return await self._fn(*args, **kwargs)
