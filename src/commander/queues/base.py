
import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from commander.tasks.task import TaskPayload


class JobQueue(abc.ABC):

    async def put(self, payload: TaskPayload):
        raise NotImplementedError

    async def get(self) -> TaskPayload:
        raise NotImplementedError