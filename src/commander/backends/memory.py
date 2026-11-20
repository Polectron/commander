import time
import uuid

from commander.backends.base import JobBackend
from commander.tasks.task import ScheduledTask, TaskPayload, TaskResult


class MemoryBackend(JobBackend):
    def __init__(self) -> None:
        super().__init__()
        self._tasks: dict[uuid.UUID, TaskResult] = {}
        self._scheduled_tasks: list[ScheduledTask] = []

    async def save(self, payload: TaskPayload):
        self._tasks[payload.id] = TaskResult(
            **payload.model_dump(), finished=False, result=None
        )

    async def set_result(self, task_id: uuid.UUID, result):
        self._tasks[task_id].finished = True
        self._tasks[task_id].result = result

    async def get(self, task_id: uuid.UUID) -> TaskResult | None:
        return self._tasks[task_id]

    async def schedule_task(self, payload: TaskPayload, when: int):
        await self.save(payload)
        self._scheduled_tasks.append(ScheduledTask(**payload.model_dump(), when=when))

    async def get_scheduled_tasks(self) -> list[TaskPayload]:
        now = int(time.time())
        tasks: list[TaskPayload] = []
        for i, t in enumerate(self._scheduled_tasks):
            if now >= t.when:
                tasks.append(self._scheduled_tasks.pop(i))
        return tasks