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

    async def notify_refresh(self) -> None:
        self._refresh.set()

    async def wait_for_refresh(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._refresh.wait(), timeout=timeout)
        except TimeoutError:
            return False
        self._refresh.clear()
        return True
