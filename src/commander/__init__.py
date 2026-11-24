from collections.abc import Callable, Coroutine
from typing import Any, ParamSpec, TypeVar, overload

from commander.backends.base import JobBackend
from commander.queues.base import JobQueue
from commander.tasks.task import Task

P = ParamSpec("P")
R = TypeVar("R")


class Commander:
    def __init__(self, queue: JobQueue, backend: JobBackend | None = None) -> None:
        self.queue = queue
        self.backend = backend

    @overload
    def task(
        self,
        fn: Callable[P, Coroutine[Any, Any, R]],
        *,
        retries: int = 0,
    ) -> Task[P, R]: ...

    @overload
    def task(
        self,
        fn: None = None,
        *,
        retries: int = 0,
    ) -> Callable[[Callable[P, Coroutine[Any, Any, R]]], Task[P, R]]: ...

    def task(
        self,
        fn: Callable[..., Coroutine[Any, Any, Any]] | None = None,
        *,
        retries: int = 0,
    ) -> (
        Task[Any, Any]
        | Callable[[Callable[..., Coroutine[Any, Any, Any]]], Task[Any, Any]]
    ):
        def decorate(func: Callable[..., Coroutine[Any, Any, Any]]) -> Task[Any, Any]:
            return Task(self, func, retries)

        return decorate(fn) if fn is not None else decorate
