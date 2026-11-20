from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from commander import Commander
    from commander.backends.base import JobBackend


class TaskPayload(BaseModel):
    id: uuid.UUID
    fn: Callable
    args: tuple
    kwargs: dict

class ScheduledTask(TaskPayload):
    when: int

class TaskResult(TaskPayload):
    finished: bool
    result: Any

class TaskHandler:
    def __init__(self, task_id: uuid.UUID, backend: JobBackend | None = None):
        self.task_id = task_id
        self._backend = backend

    async def finished(self) -> bool:
        return False

    async def wait(self, timeout: float | None = None):
        if self._backend is None:
            raise ValueError("No backend configured so tasks can't be waited")
        start_time = time.monotonic()
        while True:
            result = await self._backend.get(self.task_id)
            if result and result.finished:
                return result.result
            await asyncio.sleep(0.1)
            if timeout and time.monotonic() - start_time > timeout:
                raise TimeoutError("Timeout waiting for task")
        

class Task:
    def __init__(self, commander: Commander, fn) -> None:
        self._fn = fn
        self._commander = commander

    async def schedule(self, *args, when: int, **kwargs):
        if self._commander.backend is None:
            raise ValueError("No commander is set up, can't schedule task")
        payload = TaskPayload(id=uuid.uuid4(), fn=self._fn, args=args, kwargs=kwargs)
        await self._commander.backend.schedule_task(payload, when)

    async def queue(self, *args, **kwargs) -> TaskHandler:
        payload = TaskPayload(id=uuid.uuid4(), fn=self._fn, args=args, kwargs=kwargs)
        if self._commander.backend:
            await self._commander.backend.save(payload)
        await self._commander.queue.put(payload)

        return TaskHandler(payload.id, self._commander.backend)

    async def run(self, *args, **kwargs):
        return await self._fn(*args, **kwargs)
