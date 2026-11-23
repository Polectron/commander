from __future__ import annotations

import abc
import uuid
from typing import TYPE_CHECKING

from commander.tasks.task import ScheduledTask

if TYPE_CHECKING:
    from commander.tasks.task import Schedule, TaskPayload, TaskState


class JobBackend(abc.ABC):
    async def ensure_setup(self):
        pass

    @abc.abstractmethod
    async def save(self, task_id: uuid.UUID, payload: TaskPayload):
        raise NotImplementedError

    @abc.abstractmethod
    async def set_running(self, task_id: uuid.UUID):
        raise NotImplementedError

    @abc.abstractmethod
    async def set_finished(self, task_id: uuid.UUID, result):
        raise NotImplementedError

    @abc.abstractmethod
    async def set_failed(self, task_id: uuid.UUID, error: str):
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, task_id: uuid.UUID) -> TaskState | None:
        raise NotImplementedError

    @abc.abstractmethod
    async def schedule_task(
        self,
        schedule_id: uuid.UUID,
        payload: TaskPayload,
        schedule: Schedule | int,
        next_run: int | None = None,
    ):
        raise NotImplementedError

    @abc.abstractmethod
    async def get_schedules(self) -> list[ScheduledTask]:
        raise NotImplementedError

    @abc.abstractmethod
    async def update_schedule(self, task: ScheduledTask) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_schedule(self, schedule_id: uuid.UUID) -> None:
        raise NotImplementedError
