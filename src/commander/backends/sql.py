import uuid

import aiosqlite

from commander.backends.base import JobBackend
from commander.tasks.task import TaskPayload, TaskResult


class SqlBackend(JobBackend):
    def __init__(self) -> None:
        super().__init__()

    async def ensure_setup(self):
        # TODO: ensure tables are created
        pass

    async def save(self, payload: TaskPayload):
        serialized_payload = payload.model_dump()
        async with aiosqlite.connect("commander.db") as db:
            await db.execute(
                "INSERT INTO tasks (id, fn, args, kwargs) VALUES (?, ?, ?, ?)",
                (
                    serialized_payload["id"],
                    serialized_payload["fn"],
                    serialized_payload["args"],
                    serialized_payload["kwargs"],
                ),
            )
            await db.commit()

    async def get(self, task_id: uuid.UUID) -> TaskResult | None:
        async with aiosqlite.connect("commander.db") as db:
            cursor = await db.execute("SELECT * FROM tasks")
            row = await cursor.fetchone()
            if row is not None:
                return TaskResult(
                    id=row["id"],
                    fn=row["fn"],
                    args=row["args"],
                    kwargs=row["kwargs"],
                    finished=row["finished"],
                    result=row["result"],
                )

    async def set_result(self, task_id: uuid.UUID, result):
        raise NotImplementedError

    async def schedule_task(self, payload: TaskPayload, when: int):
        raise NotImplementedError

    async def get_scheduled_tasks(self) -> list[TaskPayload]:
        raise NotImplementedError
