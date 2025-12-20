"""Async utilities shared across workers."""
from asyncio import get_event_loop
from concurrent.futures import Executor
from functools import partial
from typing import Any, Callable


async def run_in_executor(executor: Executor, func: Callable[..., Any], *args) -> Any:
    """Run a blocking function in the given executor."""
    loop = get_event_loop()
    return await loop.run_in_executor(executor, partial(func, *args))
