import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from commander.tasks.task import QueuedTask


class JobQueue(abc.ABC):
    async def put(self, task: QueuedTask):
        raise NotImplementedError

    async def get(self) -> QueuedTask:
        raise NotImplementedError
