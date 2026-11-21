import asyncio
import json
import time
import uuid

import aiosqlite
from pypika import Column, Field, Query

from commander.backends.base import JobBackend
from commander.tasks.task import ScheduledTask, TaskPayload, TaskResult


class SqlBackend(JobBackend):
    DATABASE_PATH = "commander.db"
    DATABASE_TIMEOUT = 30

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
                    Column("finished", "boolean", nullable=False),
                    Column("result", "text", nullable=True),
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
                    Column("when", "int", nullable=False),
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
                .insert(task_id, json.dumps(serialized_payload), False, None)
                .get_sql()
            )
            await db.execute(query)
            await db.commit()

    async def get(self, task_id: uuid.UUID) -> TaskResult | None:
        async with self._lock, self._connect() as db:
            db.row_factory = aiosqlite.Row
            query = (
                Query.from_("tasks")
                .select("id", "payload", "finished", "result")
                .where(Field("id") == str(task_id))
                .get_sql()
            )
            cursor = await db.execute(query)
            row = await cursor.fetchone()
            if row is not None:
                payload = TaskPayload.model_validate_json(row["payload"])
                return TaskResult(
                    id=uuid.UUID(row["id"]),
                    payload=payload,
                    finished=bool(row["finished"]),
                    result=(
                        json.loads(row["result"]) if row["result"] is not None else None
                    ),
                )

    async def set_result(self, task_id: uuid.UUID, result):
        async with self._lock, self._connect() as db:
            query = (
                Query.update("tasks")
                .set("finished", True)
                .set("result", json.dumps(result))
                .where(Field("id") == str(task_id))
                .get_sql()
            )
            cursor = await db.execute(query)
            if cursor.rowcount != 1:
                raise ValueError(f"Task {task_id} was not found")
            await db.commit()

    async def schedule_task(
        self, schedule_id: uuid.UUID, payload: TaskPayload, when: int
    ):
        serialized_payload = payload.model_dump(mode="json", exclude={"id"})
        async with self._lock, self._connect() as db:
            schedule_query = (
                Query.into("schedule")
                .insert(schedule_id, json.dumps(serialized_payload), when)
                .get_sql()
            )
            await db.execute(schedule_query)
            await db.commit()

    async def get_scheduled_tasks(self) -> list[ScheduledTask]:
        async with self._lock, self._connect() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                'DELETE FROM schedule WHERE "when" <= ? '
                'RETURNING "id", "payload", "when"',
                (int(time.time()),),
            )
            rows = await cursor.fetchall()
            await db.commit()
            return [
                ScheduledTask(
                    id=row["id"],
                    payload=TaskPayload.model_validate_json(row["payload"]),
                    when=row["when"],
                )
                for row in rows
            ]
