import json
import os
from uuid import UUID

import valkey.asyncio as valkey
from pydantic import TypeAdapter

from commander.backends.base import JobBackend
from commander.tasks.task import (
    OnceSchedule,
    Schedule,
    ScheduledTask,
    TaskPayload,
    TaskState,
    TaskStatus,
)


class ValkeyBackend(JobBackend):
    SCHEDULE_ADAPTER = TypeAdapter(Schedule)

    def __init__(self):
        self._connection = valkey.Valkey(
            host=os.getenv("VALKEY_HOST", "valkey"), port=6379, db=0
        )

    async def save(self, task_id: UUID, payload: TaskPayload):
        await self._connection.hset(
            f"commander:task:{task_id}",
            mapping={
                "payload": payload.model_dump_json(),
                "status": TaskStatus.PENDING,
                "result": "null",
                "error": "null",
            },
        )  # pyright: ignore[reportGeneralTypeIssues]

    async def set_running(self, task_id: UUID):
        key = f"commander:task:{task_id}"
        if not await self._connection.exists(key):
            raise ValueError(f"Task {task_id} was not found")
        await self._connection.hset(key, "status", TaskStatus.RUNNING)  # pyright: ignore[reportGeneralTypeIssues]

    async def set_finished(self, task_id: UUID, result):
        key = f"commander:task:{task_id}"
        if not await self._connection.exists(key):
            raise ValueError(f"Task {task_id} was not found")
        await self._connection.hset(
            key,
            mapping={
                "status": TaskStatus.FINISHED,
                "result": json.dumps(result),
                "error": "null",
            },
        )  # pyright: ignore[reportGeneralTypeIssues]

    async def set_failed(self, task_id: UUID, error: str):
        key = f"commander:task:{task_id}"
        if not await self._connection.exists(key):
            raise ValueError(f"Task {task_id} was not found")
        await self._connection.hset(
            key,
            mapping={
                "status": TaskStatus.FAILED,
                "result": "null",
                "error": json.dumps(error),
            },
        )  # pyright: ignore[reportGeneralTypeIssues]

    async def get(self, task_id: UUID) -> TaskState | None:
        data = await self._connection.hgetall(f"commander:task:{task_id}")  # pyright: ignore[reportGeneralTypeIssues]
        if not data:
            return None

        data = {
            key.decode() if isinstance(key, bytes) else key: value.decode()
            if isinstance(value, bytes)
            else value
            for key, value in data.items()
        }
        return TaskState(
            id=task_id,
            payload=TaskPayload.model_validate_json(data["payload"]),
            status=TaskStatus(data["status"]),
            result=json.loads(data["result"]),
            error=json.loads(data["error"]),
        )

    async def schedule_task(
        self,
        schedule_id: UUID,
        payload: TaskPayload,
        schedule: Schedule | int,
        next_run: int | None = None,
    ):
        if isinstance(schedule, int):
            next_run = next_run if next_run is not None else schedule
            schedule = OnceSchedule(run_at=next_run)
        async with self._connection.pipeline(transaction=True) as pipeline:
            pipeline.zadd("commander:schedule", {str(schedule_id): next_run})
            pipeline.hset(
                f"commander:schedule:{schedule_id}",
                mapping={
                    "payload": payload.model_dump_json(),
                    "schedule": schedule.model_dump_json(),
                    "next_run": str(next_run),
                    "last_run": "null",
                },
            )
            await pipeline.execute()

    async def get_schedules(self) -> list[ScheduledTask]:
        ids = await self._connection.zrange("commander:schedule", 0, -1)
        tasks: list[ScheduledTask] = []
        for schedule_id in ids:
            raw_id = (
                schedule_id.decode() if isinstance(schedule_id, bytes) else schedule_id
            )
            scheduled = await self._connection.hgetall(f"commander:schedule:{raw_id}")
            if not scheduled:
                continue
            scheduled = {
                key.decode() if isinstance(key, bytes) else key: value.decode()
                if isinstance(value, bytes)
                else value
                for key, value in scheduled.items()
            }
            last_run = json.loads(scheduled["last_run"])
            tasks.append(
                ScheduledTask(
                    id=UUID(raw_id),
                    payload=TaskPayload.model_validate_json(scheduled["payload"]),
                    schedule=self.SCHEDULE_ADAPTER.validate_json(scheduled["schedule"]),
                    next_run=int(scheduled["next_run"]),
                    last_run=last_run,
                )
            )
        return tasks

    async def update_schedule(self, task: ScheduledTask) -> None:
        async with self._connection.pipeline(transaction=True) as pipeline:
            pipeline.zadd(
                "commander:schedule",
                {str(task.id): task.next_run},
            )
            pipeline.hset(
                f"commander:schedule:{task.id}",
                mapping={
                    "payload": task.payload.model_dump_json(),
                    "schedule": task.schedule.model_dump_json(),
                    "next_run": str(task.next_run),
                    "last_run": json.dumps(task.last_run),
                },
            )
            await pipeline.execute()

    async def delete_schedule(self, schedule_id: UUID) -> None:
        async with self._connection.pipeline(transaction=True) as pipeline:
            pipeline.zrem("commander:schedule", str(schedule_id))
            pipeline.delete(f"commander:schedule:{schedule_id}")
            await pipeline.execute()
