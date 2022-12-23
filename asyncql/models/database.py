from __future__ import annotations

import contextlib
from contextvars import ContextVar
from types import TracebackType
from typing import Any, Dict, Iterator, List, Mapping, Optional, Type, Union

from asyncql.backends.models.database import DatabaseBackend
from asyncql.common import imports
from asyncql.models.connection import Connection
from asyncql.models.transaction import Transaction
from asyncql.models.url import DatabaseURL


class Database:
    BACKENDS = {"mysql": "asyncql.backends.mysql:MySQLBackend"}

    def __init__(
        self,
        url: Union[str, DatabaseURL],
        *,
        force_rollback: bool = False,
        **kwargs: Any,
    ) -> None:
        if isinstance(url, str):
            url = DatabaseURL(url)

        self._url = url
        self._kwargs = kwargs
        self._force_rollback = force_rollback

        self.is_connected = False

        backend_str = self._get_backend()
        backend = imports.import_from_string(backend_str)
        if not issubclass(backend, DatabaseBackend):
            raise TypeError(f"Backend must be a subclass of DatabaseBackend")

        self._backend = backend(self._url, **self._kwargs)

        self._connection_context: ContextVar[Connection] = ContextVar(
            "connection_context"
        )
        self._global_connection: Optional[Connection] = None
        self._global_transaction: Optional[Transaction] = None

    async def connect(self) -> None:
        if self.is_connected:
            return

        await self._backend.connect()
        self.is_connected = True

        if self._force_rollback:
            if self._global_connection is not None:
                raise RuntimeError("Connection already established")

            if self._global_transaction is not None:
                raise RuntimeError("Transaction already established")

            self._global_connection = Connection(self._backend)
            self._global_transaction = await self._global_connection.transaction(
                force_rollback=True
            )

            await self._global_transaction.__aenter__()

    async def disconnect(self) -> None:
        if not self.is_connected:
            return

        if self._force_rollback:
            if self._global_connection is None:
                raise RuntimeError("Connection not established")

            if self._global_transaction is None:
                raise RuntimeError("Transaction not established")

            await self._global_transaction.__aexit__()

            self._global_connection = None
            self._global_transaction = None
        else:
            self._connection_context = ContextVar("connection_context")

        await self._backend.disconnect()
        self.is_connected = False

    async def __aenter__(self) -> Database:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        await self.disconnect()

    async def fetch_all(
        self,
        query: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        async with self.connection() as connection:
            rows = await connection.fetch_all(query, params)

        return rows

    async def fetch_one(
        self,
        query: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        async with self.connection() as connection:
            row = await connection.fetch_one(query, params)

        return row

    async def execute(
        self,
        query: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        async with self.connection() as connection:
            result = await connection.execute(query, params)

        return result

    async def execute_many(
        self,
        query: str,
        params: List[Mapping[str, Any]],
    ) -> None:
        async with self.connection() as connection:
            await connection.execute_many(query, params)

    def connection(self) -> Connection:
        if self._global_connection is not None:
            return self._global_connection

        try:
            connection = self._connection_context.get()
        except LookupError:
            connection = Connection(self._backend)
            self._connection_context.set(connection)

        return connection

    def transaction(
        self,
        *,
        force_rollback: bool = False,
        **kwargs: Any,
    ) -> Transaction:
        return Transaction(
            self.connection,
            force_rollback,
            **kwargs,
        )

    @contextlib.contextmanager
    def force_rollback(self) -> Iterator[None]:
        initial = self._force_rollback
        self._force_rollback = True

        try:
            yield
        finally:
            self._force_rollback = initial

    def _get_backend(self) -> str:
        backend = self.BACKENDS.get(self._url.scheme, None)
        if backend is None:
            raise NotImplementedError(f"Unsupported scheme: {self._url.scheme}")

        return backend
