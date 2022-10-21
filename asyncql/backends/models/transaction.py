from typing import Protocol


class BackendTransaction(Protocol):
    async def start(self, is_root: bool = False) -> None:
        ...

    async def commit(self) -> None:
        ...

    async def rollback(self) -> None:
        ...
