import asyncio

from commander.backends.base import JobBackend
from commander.queues.base import JobQueue


class TasksRunner:
    def __init__(
        self,
        queue: JobQueue,
        backend: JobBackend | None = None,
        parallel_tasks: int = 4,
    ) -> None:
        if parallel_tasks < 1:
            raise ValueError("parallel_tasks must be at least 1")

        self.parallel_tasks = parallel_tasks
        self._queue = queue
        self._backend = backend

    async def run(self) -> None:
        if self.parallel_tasks < 1:
            raise ValueError("parallel_tasks must be at least 1")

        async with asyncio.TaskGroup() as task_group:
            for _ in range(self.parallel_tasks):
                task_group.create_task(self._consume())

    async def _consume(self) -> None:
        while True:
            task = await self._queue.get()
            print(f"Running {task}")
            try:
                result = await task.payload.fn(
                    *task.payload.args, **task.payload.kwargs
                )
                print(f"Finished {task}")
                if self._backend:
                    await self._backend.set_result(task.id, result)
            except Exception as e:  # noqa: BLE001
                print(f"Task {task.id} failed with {e!r}")
