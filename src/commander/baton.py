import asyncio

from commander.backends.base import JobBackend
from commander.queues.base import JobQueue
from commander.tasks.task import queue_task


class Baton:
    def __init__(self, queue: JobQueue, backend: JobBackend) -> None:
        self._backend = backend
        self._queue = queue

    async def run(self):
        while True:
            pending_tasks = await self._backend.get_scheduled_tasks()
            for task in pending_tasks:
                await queue_task(self._queue, self._backend, task.payload)
            await asyncio.sleep(1)
