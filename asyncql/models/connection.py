from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, Dict, List, Mapping, Optional, Type

from asyncql.backends.models.database import DatabaseBackend
from asyncql.common import query as querylib
from asyncql.models.transaction import Transaction


class Connection:
    def __init__(self, backend: DatabaseBackend) -> None:
        self._backend = backend

        self._connection_lock = asyncio.Lock()
        self._connection = self._backend.connection()
        self._connection_counter = 0

        self._transaction_lock = asyncio.Lock()
        self._transaction_stack: list[Transaction] = []

        self._query_lock = asyncio.Lock()

    async def __aenter__(self) -> Connection:
        async with self._connection_lock:
            self._connection_counter += 1

            try:
                if self._connection_counter == 1:
                    await self._connection.acquire()
            except BaseException as exc:
                self._connection_counter -= 1
                raise exc

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        async with self._connection_lock:
            if self._connection is None:
                raise RuntimeError("Connection already closed")

            self._connection_counter -= 1
            if self._connection_counter == 0:
                await self._connection.release()

    async def fetch_all(
        self,
        query: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if params is not None:
            query = querylib.parse_query(query, params)

        async with self._query_lock:
            rows = await self._connection.fetch_all(query)

        return rows

    async def fetch_one(
        self,
        query: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if params is not None:
            query = querylib.parse_query(query, params)

        async with self._query_lock:
            row = await self._connection.fetch_one(query)

        return row

    async def execute(
        self,
        query: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        if params is not None:
            query = querylib.parse_query(query, params)

        async with self._query_lock:
            result = await self._connection.execute(query)

        return result

    async def execute_many(
        self,
        query: str,
        params: Optional[List[Mapping[str, Any]]] = None,
    ) -> None:
        queries = [query]
        if params is not None:
            queries = [querylib.parse_query(query, param) for param in params]

        async with self._query_lock:
            await self._connection.execute_many(queries)

    async def transaction(
        self,
        *,
        force_rollback: bool = False,
        **kwargs: Any,
    ) -> Transaction:
        def connection_callable() -> Connection:
            return self

        return Transaction(connection_callable, force_rollback, **kwargs)

    @property
    def raw_connection(self) -> Any:
        return self._connection.raw_connection
