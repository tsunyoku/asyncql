from __future__ import annotations

from typing import Any, Optional
from urllib.parse import SplitResult, unquote, urlsplit


class DatabaseURL:
    def __init__(self, url: str) -> None:
        self._url = url
        self._components: Optional[SplitResult] = None

    @property
    def components(self) -> SplitResult:
        if self._components is not None:
            return self._components

        self._components = urlsplit(self._url)
        return self._components

    @property
    def scheme(self) -> str:
        return self.components.scheme

    @property
    def username(self) -> Optional[str]:
        if self.components.username is None:
            return None

        return unquote(self.components.username)

    @property
    def password(self) -> Optional[str]:
        if self.components.password is None:
            return None

        return unquote(self.components.password)

    @property
    def host(self) -> Optional[str]:
        return self.components.hostname

    @property
    def port(self) -> Optional[int]:
        return self.components.port

    @property
    def database(self) -> str:
        path = self.components.path
        if path.startswith("/"):
            path = path[1:]

        return unquote(path)

    def replace(self, **kwargs: Any) -> DatabaseURL:
        if (
            "username" in kwargs
            or "password" in kwargs
            or "host" in kwargs
            or "port" in kwargs
        ):
            host = kwargs.pop("host", self.host)
            port = kwargs.pop("port", self.port)
            username = kwargs.pop("username", self.username)
            password = kwargs.pop("password", self.password)

        if "database" in kwargs:
            kwargs["path"] = "/" + kwargs.pop("database")

        components = self.components._replace(**kwargs)
        return self.__class__(components.geturl())

    @property
    def secure_url(self) -> str:
        if self.password:
            return self.replace(password="********")._url

        return self._url

    def __str__(self) -> str:
        return self._url

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.secure_url!r})"

    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)
