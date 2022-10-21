from enum import Enum
from typing import Any, Mapping, Sequence

TABLE = str.maketrans(
    {
        "\0": "\\0",
        "\r": "\\r",
        "\x08": "\\b",
        "\x09": "\\t",
        "\x1a": "\\z",
        "\n": "\\n",
        "\r": "\\r",
        '"': '\\"',
        "'": "\\'",
        "\\": "\\\\",
        "%": "\\%",
    }
)


def _sanitize_str(value: str) -> str:
    return value.translate(TABLE)


def _sanitize_value(value: Any) -> str:
    if isinstance(value, str):
        sanitized = _sanitize_str(value)
        value = f"'{sanitized}'"
    elif isinstance(value, bool):
        value = str(value).lower()
    elif isinstance(value, Enum):
        enum_value = value.value
        value = _sanitize_value(enum_value)
    elif isinstance(value, Sequence):
        sanitized_sequence = [_sanitize_value(v) for v in value]
        value = f"({','.join(sanitized_sequence)})"
    else:
        value = _sanitize_str(str(value))

    return value


def parse_query(query: str, params: Mapping[str, Any]) -> str:
    for key, value in params.items():
        query_key = f":{key}"
        key_position = query.index(query_key)

        key_placement = key_position + len(query_key)
        query = (
            query[:key_placement].replace(query_key, _sanitize_value(value))
            + query[key_placement:]
        )

    return query
