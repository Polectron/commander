from __future__ import annotations

import abc
import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from commander.tasks.task import QueuedTask


class JobQueue(abc.ABC):
    async def put(self, task: QueuedTask):
        raise NotImplementedError

    async def get(self) -> QueuedTask:
        raise NotImplementedError

    async def ack(self, task: QueuedTask) -> None:
        pass

    async def nack(self, task: QueuedTask, requeue: bool = True) -> None:
        if requeue:
            await self.put(task)
