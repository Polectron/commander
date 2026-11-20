from commander.backends.base import JobBackend
from commander.queues.base import JobQueue
from commander.tasks.task import Task


class Commander:
    def __init__(self, queue: JobQueue, backend: JobBackend | None = None) -> None:
        self.queue = queue
        self.backend = backend

    def task(self, fn) -> Task:
        return Task(self, fn)