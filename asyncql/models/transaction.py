from __future__ import annotations

import functools
from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, Generator, Optional, Type

from asyncql.backends.models.transaction import BackendTransaction

if TYPE_CHECKING:
    from asyncql.models.connection import Connection


class Transaction:
    def __init__(
        self,
        connection_callable: Callable[[], Connection],
        force_rollback: bool,
        **kwargs: Any,
    ) -> None:
        self._connection_callable = connection_callable
        self._force_rollback = force_rollback
        self._kwargs = kwargs

        self._connection: Optional[Connection] = None
        self._transaction: Optional[BackendTransaction] = None

    async def __aenter__(self) -> Transaction:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        if exc_type is not None or self._force_rollback:
            await self.rollback()
        else:
            await self.commit()

    async def __await__(self) -> Generator[None, None, Transaction]:
        return self.start().__await__()

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with self:
                return await func(*args, **kwargs)

        return wrapper

    async def start(self) -> Transaction:
        self._connection = self._connection_callable()
        self._transaction = self._connection._connection.transaction()

        async with self._connection._transaction_lock:
            is_root = not self._connection._transaction_stack

            await self._connection.__aenter__()
            await self._transaction.start(is_root=is_root)
            self._connection._transaction_stack.append(self)

        return self

    async def commit(self) -> None:
        if self._connection is None:
            raise RuntimeError("No connection established")

        if self._transaction is None:
            raise RuntimeError("No transaction established")

        async with self._connection._transaction_lock:
            if self._connection._transaction_stack[-1] is not self:
                raise RuntimeError("Transaction is not the current transaction")

            self._connection._transaction_stack.pop()

            await self._transaction.commit()
            await self._connection.__aexit__()

    async def rollback(self) -> None:
        if self._connection is None:
            raise RuntimeError("No connection established")

        if self._transaction is None:
            raise RuntimeError("No transaction established")

        async with self._connection._transaction_lock:
            if self._connection._transaction_stack[-1] is not self:
                raise RuntimeError("Transaction is not the current transaction")

            self._connection._transaction_stack.pop()

            await self._transaction.rollback()
            await self._connection.__aexit__()
