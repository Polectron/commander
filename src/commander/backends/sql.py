import asyncio
import json
import uuid

import aiosqlite
from pydantic import TypeAdapter
from pypika import Column, Field, Query

from commander.backends.base import JobBackend
from commander.tasks.task import (
    OnceSchedule,
    Schedule,
    ScheduledTask,
    TaskPayload,
    TaskState,
    TaskStatus,
)


class SqlBackend(JobBackend):
    DATABASE_PATH = "commander.db"
    DATABASE_TIMEOUT = 30
    SCHEDULE_ADAPTER = TypeAdapter(Schedule)

    def __init__(self) -> None:
        super().__init__()
        self._lock = asyncio.Lock()

    async def ensure_setup(self):
        async with self._lock:
            tasks_query = (
                Query.create_table("tasks")
                .if_not_exists()
                .columns(
                    Column("id", "uuid", nullable=False),
                    Column("payload", "text", nullable=False),
                    Column("status", "text", nullable=False),
                    Column("result", "text", nullable=True),
                    Column("error", "text", nullable=True),
                )
                .primary_key("id")
                .get_sql()
            )
            schedule_query = (
                Query.create_table("schedule")
                .if_not_exists()
                .columns(
                    Column("id", "uuid", nullable=False),
                    Column("payload", "text", nullable=False),
                    Column("schedule", "text", nullable=False),
                    Column("next_run", "int", nullable=False),
                    Column("last_run", "int", nullable=True),
                )
                .primary_key("id")
                .get_sql()
            )
            async with self._connect() as db:
                await db.execute("PRAGMA journal_mode = WAL")
                await db.execute("PRAGMA busy_timeout = 30000")
                await db.execute(tasks_query)
                await db.execute(schedule_query)
                await db.commit()

    def _connect(self) -> aiosqlite.Connection:
        return aiosqlite.connect(self.DATABASE_PATH, timeout=self.DATABASE_TIMEOUT)

    async def save(self, task_id: uuid.UUID, payload: TaskPayload):
        serialized_payload = payload.model_dump(mode="json", exclude={"id"})
        async with self._lock, self._connect() as db:
            query = (
                Query.into("tasks")
                .insert(
                    task_id,
                    json.dumps(serialized_payload),
                    TaskStatus.PENDING,
                    None,
                    None,
                )
                .get_sql()
            )
            await db.execute(query)
            await db.commit()

    async def get(self, task_id: uuid.UUID) -> TaskState | None:
        async with self._lock, self._connect() as db:
            db.row_factory = aiosqlite.Row
            query = (
                Query.from_("tasks")
                .select("id", "payload", "status", "result", "error")
                .where(Field("id") == str(task_id))
                .get_sql()
            )
            cursor = await db.execute(query)
            row = await cursor.fetchone()
            if row is not None:
                payload = TaskPayload.model_validate_json(row["payload"])
                return TaskState(
                    id=uuid.UUID(row["id"]),
                    payload=payload,
                    status=TaskStatus(row["status"]),
                    result=(
                        json.loads(row["result"]) if row["result"] is not None else None
                    ),
                    error=row["error"],
                )

    async def set_running(self, task_id: uuid.UUID):
        async with self._lock, self._connect() as db:
            cursor = await db.execute(
                "UPDATE tasks SET status = ? WHERE id = ?",
                (TaskStatus.RUNNING, str(task_id)),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"Task {task_id} was not found")
            await db.commit()

    async def set_finished(self, task_id: uuid.UUID, result):
        async with self._lock, self._connect() as db:
            query = (
                Query.update("tasks")
                .set("status", TaskStatus.FINISHED)
                .set("result", json.dumps(result))
                .set("error", None)
                .where(Field("id") == str(task_id))
                .get_sql()
            )
            cursor = await db.execute(query)
            if cursor.rowcount != 1:
                raise ValueError(f"Task {task_id} was not found")
            await db.commit()

    async def set_failed(self, task_id: uuid.UUID, error: str):
        async with self._lock, self._connect() as db:
            query = (
                Query.update("tasks")
                .set("status", TaskStatus.FAILED)
                .set("result", None)
                .set("error", error)
                .where(Field("id") == str(task_id))
                .get_sql()
            )
            cursor = await db.execute(query)
            if cursor.rowcount != 1:
                raise ValueError(f"Task {task_id} was not found")
            await db.commit()

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
        serialized_payload = payload.model_dump(mode="json", exclude={"id"})
        async with self._lock, self._connect() as db:
            schedule_query = (
                Query.into("schedule")
                .columns(
                    "id",
                    "payload",
                    "schedule",
                    "next_run",
                    "last_run",
                )
                .insert(
                    schedule_id,
                    json.dumps(serialized_payload),
                    schedule.model_dump_json(),
                    next_run,
                    None,
                )
                .get_sql()
            )
            await db.execute(schedule_query)
            await db.commit()

    async def get_schedules(self) -> list[ScheduledTask]:
        async with self._lock, self._connect() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                'SELECT "id", "payload", "schedule", "next_run", "last_run" '
                "FROM schedule"
            )
            rows = await cursor.fetchall()
            return [
                ScheduledTask(
                    id=uuid.UUID(row["id"]),
                    payload=TaskPayload.model_validate_json(row["payload"]),
                    schedule=self.SCHEDULE_ADAPTER.validate_json(row["schedule"]),
                    next_run=row["next_run"],
                    last_run=row["last_run"],
                )
                for row in rows
            ]

    async def update_schedule(self, task: ScheduledTask) -> None:
        async with self._lock, self._connect() as db:
            cursor = await db.execute(
                "UPDATE schedule "
                "SET payload = ?, schedule = ?, next_run = ?, last_run = ? "
                "WHERE id = ?",
                (
                    task.payload.model_dump_json(),
                    task.schedule.model_dump_json(),
                    task.next_run,
                    task.last_run,
                    str(task.id),
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"Schedule {task.id} was not found")
            await db.commit()

    async def delete_schedule(self, schedule_id: uuid.UUID) -> None:
        async with self._lock, self._connect() as db:
            await db.execute(
                "DELETE FROM schedule WHERE id = ?",
                (str(schedule_id),),
            )
            await db.commit()
