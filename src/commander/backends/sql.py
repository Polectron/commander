from __future__ import annotations

import json
import uuid
from typing import Any, Self

from pydantic import TypeAdapter
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from commander.backends.base import JobBackend
from commander.tasks.task import (
    OnceSchedule,
    Schedule,
    ScheduledTask,
    TaskPayload,
    TaskState,
    TaskStatus,
)

_METADATA = MetaData()

_TASKS = Table(
    "tasks",
    _METADATA,
    Column("id", String(36), primary_key=True),
    Column("payload", Text, nullable=False),
    Column("status", String, nullable=False),
    Column("result", Text),
    Column("error", Text),
)

_SCHEDULES = Table(
    "schedule",
    _METADATA,
    Column("id", String(36), primary_key=True),
    Column("payload", Text, nullable=False),
    Column("schedule", Text, nullable=False),
    Column("next_run", Integer, nullable=False),
    Column("last_run", Integer),
)


class SqlBackend(JobBackend):
    SCHEDULE_ADAPTER = TypeAdapter(Schedule)

    def __init__(self, database_url: str) -> None:
        super().__init__()

        url = make_url(database_url)
        engine = create_async_engine(
            url,
        )
        self._engine = engine

    async def ensure_setup(self) -> None:
        async with self._engine.begin() as connection:
            await connection.run_sync(_METADATA.create_all)

    async def save(self, task_id: uuid.UUID, payload: TaskPayload) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                insert(_TASKS).values(
                    id=str(task_id),
                    payload=payload.model_dump_json(),
                    status=TaskStatus.PENDING,
                    result=None,
                    error=None,
                )
            )

    async def get(self, task_id: uuid.UUID) -> TaskState | None:
        async with self._engine.connect() as connection:
            result = await connection.execute(
                select(_TASKS).where(_TASKS.c.id == str(task_id))
            )
            row = result.mappings().one_or_none()
            if row is None:
                return None

            return TaskState(
                id=uuid.UUID(row["id"]),
                payload=TaskPayload.model_validate_json(row["payload"]),
                status=TaskStatus(row["status"]),
                result=json.loads(row["result"]) if row["result"] is not None else None,
                error=row["error"],
            )

    async def set_running(self, task_id: uuid.UUID) -> None:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(_TASKS)
                .where(_TASKS.c.id == str(task_id))
                .values(status=TaskStatus.RUNNING)
            )
            await self._ensure_updated(
                connection, result.rowcount, _TASKS, task_id, "Task"
            )

    async def set_finished(self, task_id: uuid.UUID, result: Any) -> None:
        async with self._engine.begin() as connection:
            update_result = await connection.execute(
                update(_TASKS)
                .where(_TASKS.c.id == str(task_id))
                .values(
                    status=TaskStatus.FINISHED,
                    result=json.dumps(result),
                    error=None,
                )
            )
            await self._ensure_updated(
                connection, update_result.rowcount, _TASKS, task_id, "Task"
            )

    async def set_failed(self, task_id: uuid.UUID, error: str) -> None:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(_TASKS)
                .where(_TASKS.c.id == str(task_id))
                .values(status=TaskStatus.FAILED, result=None, error=error)
            )
            await self._ensure_updated(
                connection, result.rowcount, _TASKS, task_id, "Task"
            )

    async def schedule_task(
        self,
        schedule_id: uuid.UUID,
        payload: TaskPayload,
        schedule: Schedule | int,
        next_run: int | None = None,
    ) -> None:
        if isinstance(schedule, int):
            next_run = next_run if next_run is not None else schedule
            schedule = OnceSchedule(run_at=next_run)

        async with self._engine.begin() as connection:
            await connection.execute(
                insert(_SCHEDULES).values(
                    id=str(schedule_id),
                    payload=payload.model_dump_json(),
                    schedule=schedule.model_dump_json(),
                    next_run=next_run,
                    last_run=None,
                )
            )

    async def get_schedules(self) -> list[ScheduledTask]:
        async with self._engine.connect() as connection:
            result = await connection.execute(select(_SCHEDULES))
            return [
                ScheduledTask(
                    id=uuid.UUID(row["id"]),
                    payload=TaskPayload.model_validate_json(row["payload"]),
                    schedule=self.SCHEDULE_ADAPTER.validate_json(row["schedule"]),
                    next_run=row["next_run"],
                    last_run=row["last_run"],
                )
                for row in result.mappings()
            ]

    async def update_schedule(self, task: ScheduledTask) -> None:
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(_SCHEDULES)
                .where(_SCHEDULES.c.id == str(task.id))
                .values(
                    payload=task.payload.model_dump_json(),
                    schedule=task.schedule.model_dump_json(),
                    next_run=task.next_run,
                    last_run=task.last_run,
                )
            )
            await self._ensure_updated(
                connection, result.rowcount, _SCHEDULES, task.id, "Schedule"
            )

    @staticmethod
    async def _ensure_updated(
        connection: AsyncConnection,
        rowcount: int | None,
        table: Table,
        row_id: uuid.UUID,
        resource_name: str,
    ) -> None:
        if rowcount == 1:
            return
        existing_id = await connection.scalar(
            select(table.c.id).where(table.c.id == str(row_id))
        )
        if existing_id is None:
            raise ValueError(f"{resource_name} {row_id} was not found")

    async def delete_schedule(self, schedule_id: uuid.UUID) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                delete(_SCHEDULES).where(_SCHEDULES.c.id == str(schedule_id))
            )

    async def close(self) -> None:
        await self._engine.dispose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()
