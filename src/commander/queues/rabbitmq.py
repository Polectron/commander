from __future__ import annotations

import asyncio
import os
from typing import Self

import aio_pika
from pydantic import ValidationError

from commander.queues.base import JobQueue
from commander.tasks.task import QueuedTask


class RabbitMQQueue(JobQueue):
    QUEUE_NAME = "commander"

    def __init__(
        self,
        url: str,
        prefetch_count: int = 1,
    ) -> None:
        if prefetch_count < 1:
            raise ValueError("prefetch_count must be at least 1")

        self.url = url
        self.prefetch_count = prefetch_count
        self.channel_options = {"on_return_raises": True}
        self.queue_options = {"durable": True}

        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.RobustChannel | None = None
        self._qos_configured = False
        self._queue: aio_pika.abc.AbstractQueue | None = None
        self._iterator: aio_pika.abc.AbstractQueueIterator | None = None
        self._deliveries: dict[
            int, tuple[QueuedTask, aio_pika.abc.AbstractIncomingMessage]
        ] = {}
        self._initialize_lock = asyncio.Lock()

    async def _initialize(self, consume: bool = False) -> None:
        async with self._initialize_lock:
            if self._connection is None:
                self._connection = await aio_pika.connect_robust(
                    self.url,
                )
            if self._channel is None:
                self._channel = await self._connection.channel(**self.channel_options)
            if not self._qos_configured:
                await self._channel.set_qos(prefetch_count=self.prefetch_count)
                self._qos_configured = True
            if self._queue is None:
                self._queue = await self._channel.declare_queue(
                    self.QUEUE_NAME,
                )
            if consume and self._iterator is None:
                iterator = self._queue.iterator()
                await iterator.__aenter__()
                self._iterator = iterator

    async def put(self, task: QueuedTask) -> None:
        await self._initialize()
        if self._channel is None:
            raise RuntimeError("RabbitMQ channel is not initialized")
        await self._channel.default_exchange.publish(
            aio_pika.Message(
                body=task.model_dump_json().encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=self.QUEUE_NAME,
            mandatory=True,
        )

    async def get(self) -> QueuedTask:
        await self._initialize(consume=True)
        if self._iterator is None:
            raise RuntimeError("RabbitMQ consumer is not initialized")
        message = await self._iterator.__anext__()
        try:
            task = QueuedTask.model_validate_json(message.body)
        except ValidationError:
            await message.nack(requeue=False)
            raise
        self._deliveries[id(task)] = (task, message)
        return task

    async def ack(self, task: QueuedTask) -> None:
        delivery = self._deliveries.get(id(task))
        if delivery is None or delivery[0] is not task:
            raise ValueError(f"Task {task.id} has no outstanding RabbitMQ delivery")
        await delivery[1].ack()
        del self._deliveries[id(task)]

    async def nack(self, task: QueuedTask, requeue: bool = True) -> None:
        delivery = self._deliveries.get(id(task))
        if delivery is None or delivery[0] is not task:
            raise ValueError(f"Task {task.id} has no outstanding RabbitMQ delivery")
        await delivery[1].nack(requeue=requeue)
        del self._deliveries[id(task)]

    async def close(self) -> None:
        iterator, self._iterator = self._iterator, None
        connection, self._connection = self._connection, None
        self._channel = None
        self._qos_configured = False
        self._queue = None
        self._deliveries.clear()

        try:
            if iterator is not None:
                await iterator.__aexit__(None, None, None)
        finally:
            if connection is not None:
                await connection.close()

    async def __aenter__(self) -> Self:
        await self._initialize()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()
