from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Protocol

if TYPE_CHECKING:
    from asyncql.backends.models.database import DatabaseBackend
    from asyncql.backends.models.transaction import BackendTransaction


class BackendConnection(Protocol):
    def __init__(self, database: DatabaseBackend) -> None:
        ...

    async def acquire(self) -> None:
        ...

    async def release(self) -> None:
        ...

    async def fetch_all(self, query: str) -> List[Dict[str, Any]]:
        ...

    async def fetch_one(self, query: str) -> Dict[str, Any]:
        ...

    async def execute(self, query: str) -> Any:
        ...

    async def execute_many(self, queries: List[str]) -> None:
        ...

    def transaction(self) -> BackendTransaction:
        ...

    @property
    def raw_connection(self) -> Any:
        ...
