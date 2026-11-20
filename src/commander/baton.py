import asyncio

from commander.backends.base import JobBackend
from commander.queues.base import JobQueue


class Baton:
    def __init__(self, queue: JobQueue, backend: JobBackend) -> None:
        self._backend = backend
        self._queue = queue


    async def run(self):
        while True:
            pending_tasks = await self._backend.get_scheduled_tasks()
            for task in pending_tasks:
                await self._queue.put(task)
            await asyncio.sleep(1)