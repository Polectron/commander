import asyncio
import time
from contextlib import suppress

from commander import Commander
from commander.backends.sql import SqlBackend
from commander.baton import Baton
from commander.queues.memory import MemoryQueue
from commander.runner import TasksRunner

job_queue = MemoryQueue()
backend = SqlBackend()

commander = Commander(job_queue, backend)
runner = TasksRunner(job_queue, backend)
baton = Baton(job_queue, backend)


@commander.task
async def sleep(t: int):
    await asyncio.sleep(t)
    return t


async def main():
    await backend.ensure_setup()
    runner_task = asyncio.create_task(runner.run())
    baton_task = asyncio.create_task(baton.run())
    try:
        # the task can be queued awaited
        result = await sleep.queue(2)
        print(result)
        # you can check for the task status
        print(await result.finished())
        # you can wait for the task to finish
        print(await result.wait())
        # and add a timeout
        try:
            await (await sleep.queue(60)).wait(5)
        except TimeoutError as e:
            print(e)

        # tasks can also be run inmediately
        result = await sleep.run(2)
        print(result)

        # tasks can also be scheduled to run on a later time
        await sleep.schedule(5, when=int(time.time()) + 5)

        await asyncio.sleep(60)
    finally:
        runner_task.cancel()
        baton_task.cancel()
        with suppress(asyncio.CancelledError):
            await runner_task
            await baton_task


asyncio.run(main())
