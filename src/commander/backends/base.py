from __future__ import annotations

import abc
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from commander.tasks.task import TaskPayload, TaskResult


class JobBackend(abc.ABC):

    async def ensure_setup(self):
        pass

    @abc.abstractmethod
    async def save(self, payload: TaskPayload):
        raise NotImplementedError

    @abc.abstractmethod
    async def set_result(self, task_id: uuid.UUID, result):
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, task_id: uuid.UUID) -> TaskResult | None:
        raise NotImplementedError

    @abc.abstractmethod
    async def schedule_task(self, payload: TaskPayload, when: int):
        raise NotImplementedError

    @abc.abstractmethod
    async def get_scheduled_tasks(self) -> list[TaskPayload]:
        raise NotImplementedError