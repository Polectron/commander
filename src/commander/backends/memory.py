import time
import uuid

from commander.backends.base import JobBackend
from commander.tasks.task import ScheduledTask, TaskPayload, TaskState, TaskStatus


class MemoryBackend(JobBackend):
    def __init__(self) -> None:
        super().__init__()
        self._tasks: dict[uuid.UUID, TaskState] = {}
        self._scheduled_tasks: list[ScheduledTask] = []

    async def save(self, task_id: uuid.UUID, payload: TaskPayload):
        self._tasks[task_id] = TaskState(
            id=task_id, payload=payload, status=TaskStatus.PENDING
        )

    async def set_running(self, task_id: uuid.UUID):
        task = self._tasks.get(task_id)
        if task is None:
            raise ValueError(f"Task {task_id} was not found")
        task.status = TaskStatus.RUNNING

    async def set_finished(self, task_id: uuid.UUID, result):
        task = self._tasks.get(task_id)
        if task is None:
            raise ValueError(f"Task {task_id} was not found")
        task.status = TaskStatus.FINISHED
        task.result = result
        task.error = None

    async def set_failed(self, task_id: uuid.UUID, error: str):
        task = self._tasks.get(task_id)
        if task is None:
            raise ValueError(f"Task {task_id} was not found")
        task.status = TaskStatus.FAILED
        task.result = None
        task.error = error

    async def get(self, task_id: uuid.UUID) -> TaskState | None:
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
