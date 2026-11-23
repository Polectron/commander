import json
import os
import time
from uuid import UUID

import valkey.asyncio as valkey

from commander.backends.base import JobBackend
from commander.tasks.task import (
    ScheduledTask,
    TaskPayload,
    TaskState,
    TaskStatus,
)


class ValkeyBackend(JobBackend):
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

    async def schedule_task(self, schedule_id: UUID, payload: TaskPayload, when: int):
        async with self._connection.pipeline(transaction=True) as pipeline:
            pipeline.zadd("commander:schedule", {str(schedule_id): when})
            pipeline.hset(
                f"commander:schedule:{schedule_id}",
                mapping={
                    "payload": payload.model_dump_json(),
                    "when": str(when),
                },
            )
            await pipeline.execute()

    async def get_scheduled_tasks(self) -> list[ScheduledTask]:
        ids = await self._connection.zrangebyscore(
            "commander:schedule", "-inf", int(time.time())
        )
        if not ids:
            return []

        tasks: list[ScheduledTask] = []
        async with self._connection.pipeline(transaction=True) as pipeline:
            for schedule_id in ids:
                raw_id = (
                    schedule_id.decode()
                    if isinstance(schedule_id, bytes)
                    else schedule_id
                )
                pipeline.hgetall(f"commander:schedule:{raw_id}")
                pipeline.zrem("commander:schedule", schedule_id)
            responses = await pipeline.execute()

        for index, schedule_id in enumerate(ids):
            raw_id = (
                schedule_id.decode() if isinstance(schedule_id, bytes) else schedule_id
            )
            scheduled = responses[index * 2]
            if not scheduled:
                continue
            scheduled = {
                key.decode() if isinstance(key, bytes) else key: value.decode()
                if isinstance(value, bytes)
                else value
                for key, value in scheduled.items()
            }
            tasks.append(
                ScheduledTask(
                    id=UUID(raw_id),
                    payload=TaskPayload.model_validate_json(scheduled["payload"]),
                    when=int(scheduled["when"]),
                )
            )
            await self._connection.delete(f"commander:schedule:{raw_id}")
        return tasks
