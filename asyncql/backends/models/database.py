from __future__ import annotations

from typing import Any, Protocol, Union, runtime_checkable

from asyncql.backends.models.connection import BackendConnection
from asyncql.models.url import DatabaseURL


@runtime_checkable
class DatabaseBackend(Protocol):
    def __init__(
        self,
        database_url: Union[DatabaseURL, str],
        **kwargs: Any,
    ) -> None:
        ...

    async def connect(self) -> None:
        ...

    async def disconnect(self) -> None:
        ...

    def connection(self) -> BackendConnection:
        ...
