import asyncio

from commander.queues.base import JobQueue
from commander.tasks.task import QueuedTask


class MemoryQueue(JobQueue):
    def __init__(self) -> None:
        self._queue: asyncio.Queue[QueuedTask] = asyncio.Queue()
        self._refresh = asyncio.Event()

    async def put(self, task: QueuedTask):
        await self._queue.put(task)

    async def get(self) -> QueuedTask:
        return await self._queue.get()
