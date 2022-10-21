import importlib
from typing import Any


def import_from_string(import_str: str) -> Any:
    module_str, _, attr_str = import_str.partition(":")
    if not module_str or not attr_str:
        raise ValueError(f"Invalid import string: {import_str}")

    try:
        module = importlib.import_module(module_str)
    except ImportError as exc:
        if exc.name != module_str:
            raise exc from None

        raise RuntimeError(f"Couldn't import {module_str}")

    try:
        attr = getattr(module, attr_str)
    except AttributeError:
        raise RuntimeError(f"Couldn't import {attr_str} from {module_str}")

    return attr
