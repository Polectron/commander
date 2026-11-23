import uuid

from commander.backends.base import JobBackend
from commander.tasks.task import (
    OnceSchedule,
    Schedule,
    ScheduledTask,
    TaskPayload,
    TaskState,
    TaskStatus,
)


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
        self,
        schedule_id: uuid.UUID,
        payload: TaskPayload,
        schedule: Schedule | int,
        next_run: int | None = None,
    ):
        if isinstance(schedule, int):
            next_run = next_run if next_run is not None else schedule
            schedule = OnceSchedule(run_at=next_run)
        self._scheduled_tasks.append(
            ScheduledTask(
                id=schedule_id,
                payload=payload,
                schedule=schedule,
                next_run=next_run,
            )
        )

    async def get_schedules(self) -> list[ScheduledTask]:
        return list(self._scheduled_tasks)

    async def update_schedule(self, task: ScheduledTask) -> None:
        for index, scheduled in enumerate(self._scheduled_tasks):
            if scheduled.id == task.id:
                self._scheduled_tasks[index] = task
                return

    async def delete_schedule(self, schedule_id: uuid.UUID) -> None:
        self._scheduled_tasks = [
            task for task in self._scheduled_tasks if task.id != schedule_id
        ]
