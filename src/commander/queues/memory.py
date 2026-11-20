import asyncio

from commander.queues.base import JobQueue
from commander.tasks.task import TaskPayload


class MemoryQueue(JobQueue):
    def __init__(self) -> None:
        self._queue = asyncio.Queue()

    async def put(self, payload: TaskPayload):
        await self._queue.put(payload)

    async def get(self) -> TaskPayload:
        return await self._queue.get()