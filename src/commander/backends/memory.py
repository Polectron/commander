import time
import uuid

from commander.backends.base import JobBackend
from commander.tasks.task import ScheduledTask, TaskPayload, TaskResult


class MemoryBackend(JobBackend):
    def __init__(self) -> None:
        super().__init__()
        self._tasks: dict[uuid.UUID, TaskResult] = {}
        self._scheduled_tasks: list[ScheduledTask] = []

    async def save(self, task_id: uuid.UUID, payload: TaskPayload):
        self._tasks[task_id] = TaskResult(
            id=task_id, payload=payload, finished=False, result=None
        )

    async def set_result(self, task_id: uuid.UUID, result):
        self._tasks[task_id].finished = True
        self._tasks[task_id].result = result

    async def get(self, task_id: uuid.UUID) -> TaskResult | None:
        return self._tasks.get(task_id)

    async def schedule_task(
        self, schedule_id: uuid.UUID, payload: TaskPayload, when: int
    ):
        self._scheduled_tasks.append(
            ScheduledTask(id=schedule_id, payload=payload, when=when)
        )

    async def get_scheduled_tasks(self) -> list[ScheduledTask]:
        now = int(time.time())
        tasks = [task for task in self._scheduled_tasks if now >= task.when]
        self._scheduled_tasks = [
            task for task in self._scheduled_tasks if now < task.when
        ]
        return tasks
