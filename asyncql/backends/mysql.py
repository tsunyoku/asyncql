from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

import aiomysql

from asyncql.backends.models.connection import BackendConnection
from asyncql.backends.models.database import DatabaseBackend
from asyncql.backends.models.transaction import BackendTransaction
from asyncql.exceptions import AsyncqlException
from asyncql.models.url import DatabaseURL


class MySQLBackend(DatabaseBackend):
    def __init__(
        self,
        database_url: Union[DatabaseURL, str],
        use_ssl: bool = False,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
    ) -> None:
        if isinstance(database_url, str):
            database_url = DatabaseURL(database_url)

        self._database_url = database_url
        self._use_ssl = use_ssl
        self._min_size = min_size
        self._max_size = max_size
        self._pool: Optional[aiomysql.Pool] = None

    @property
    def _connection_options(self) -> Dict[str, Any]:
        options = {}

        for option_name, option_value in (
            ("ssl", self._use_ssl),
            ("minsize", self._min_size),
            ("maxsize", self._max_size),
        ):
            if option_value is not None:
                options[option_name] = option_value

        return options

    async def connect(self) -> None:
        if self._pool is not None:
            raise AsyncqlException("Connection already established")

        port = 3306
        if self._database_url.port is not None:
            port = self._database_url.port

        self._pool = await aiomysql.create_pool(
            host=self._database_url.host,
            port=port,
            user=self._database_url.username,
            password=self._database_url.password,
            db=self._database_url.database,
            autocommit=True,
            **self._connection_options,
        )

    async def disconnect(self) -> None:
        if self._pool is None:
            raise AsyncqlException("Connection not established")

        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None

    def connection(self) -> MySQLConnection:
        return MySQLConnection(self)


class MySQLConnection(BackendConnection):
    def __init__(self, database: MySQLBackend) -> None:
        self._database = database
        self._connection: Optional[aiomysql.Connection] = None

    async def acquire(self) -> None:
        if self._connection is not None:
            raise AsyncqlException("Connection already acquired")

        if self._database._pool is None:
            raise AsyncqlException("Connection not established")

        self._connection = await self._database._pool.acquire()

    async def release(self) -> None:
        if self._connection is None:
            raise AsyncqlException("Connection not acquired")

        if self._database._pool is None:
            raise AsyncqlException("Connection not established")

        await self._database._pool.release(self._connection)
        self._connection = None

    async def fetch_all(self, query: str) -> List[Dict[str, Any]]:
        if self._connection is None:
            raise AsyncqlException("Connection not acquired")

        async with self._connection.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(query)
            return await cursor.fetchall()

    async def fetch_one(self, query: str) -> Dict[str, Any]:
        if self._connection is None:
            raise AsyncqlException("Connection not acquired")

        async with self._connection.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(query)
            return await cursor.fetchone()

    async def execute(self, query: str) -> int:
        if self._connection is None:
            raise AsyncqlException("Connection not acquired")

        async with self._connection.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(query)
            return cursor.lastrowid

    async def execute_many(self, queries: List[str]) -> None:
        if self._connection is None:
            raise AsyncqlException("Connection not acquired")

        async with self._connection.cursor(aiomysql.DictCursor) as cursor:
            for query in queries:
                await cursor.execute(query)

    def transaction(self) -> BackendTransaction:
        return MySQLTransaction(self)

    @property
    def raw_connection(self) -> aiomysql.Connection:
        if self._connection is None:
            raise AsyncqlException("Connection not acquired")

        return self._connection


class MySQLTransaction(BackendTransaction):
    def __init__(self, connection: MySQLConnection) -> None:
        self._connection = connection
        self._is_root = False
        self._savepoint_name: Optional[str] = None

    async def start(self, is_root: bool = False) -> None:
        if self._connection._connection is None:
            raise AsyncqlException("Connection not acquired")

        connection = self._connection._connection

        self._is_root = is_root
        if self._is_root:
            await connection.begin()
        else:
            self._savepoint_name = "asyncql_" + str(uuid4()).replace("-", "_")
            async with connection.cursor() as cursor:
                await cursor.execute(f"SAVEPOINT {self._savepoint_name}")

    async def commit(self) -> None:
        if self._connection._connection is None:
            raise AsyncqlException("Connection not acquired")

        connection = self._connection._connection

        if self._is_root:
            await connection.commit()
        else:
            async with connection.cursor() as cursor:
                await cursor.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")

    async def rollback(self) -> None:
        if self._connection._connection is None:
            raise AsyncqlException("Connection not acquired")

        connection = self._connection._connection

        if self._is_root:
            await connection.rollback()
        else:
            async with connection.cursor() as cursor:
                await cursor.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
